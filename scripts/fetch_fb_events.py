#!/usr/bin/env python3
"""
fetch_fb_events.py
──────────────────
Scrapes public FB posts from the tracked page, classifies each post's
sentiment using a keyword rule table, then writes new events to
data/new_events.json for update_pine_script.py to consume.

State tracking:
  data/last_event_timestamp.json  – stores last successfully processed
                                    post timestamp (unix ms) so re-runs
                                    are idempotent.

Environment variables (set as GitHub Actions secrets):
  FB_PAGE_ID   – public page ID or username (e.g. "SomePage")
  FB_COOKIES   – optional; FB session cookies as JSON string for pages
                 that require login (leave empty for fully public pages)

Usage:
  python scripts/fetch_fb_events.py
"""

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from facebook_scraper import get_posts
except ImportError:
    print("ERROR: facebook-scraper not installed. Run: pip install facebook-scraper")
    sys.exit(1)

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = ROOT / "data" / "last_event_timestamp.json"
OUTPUT_FILE = ROOT / "data" / "new_events.json"

# ── Configuration ─────────────────────────────────────────────────────────────
FB_PAGE_ID: str = os.environ.get("FB_PAGE_ID", "")
FB_COOKIES: str = os.environ.get("FB_COOKIES", "")  # JSON string of cookie dict
MAX_POSTS_PER_RUN: int = 10  # safety cap; keeps API cost low

# ── Sentiment rule table ──────────────────────────────────────────────────────
# Each entry: (keywords_list, direction, base_strength)
#   direction : 1  = 偏多 ▲ (bearish poster sentiment → bullish market signal)
#              -1  = 偏空 ▼ (bullish poster sentiment → bearish market signal)
#   strength  : 1 / 2 / 3  (maps to ★☆☆ / ★★☆ / ★★★)
SENTIMENT_RULES: list[tuple[list[str], int, int]] = [
    # ── Strong bullish signals (poster in distress / capitulating) ────────────
    (["停損", "認賠", "虧損", "損失", "全賠", "出清停損", "畢業", "爆倉"], 1, 3),
    (["被套", "套牢", "跌停", "住套房", "房貸", "公園", "淨值歸零"], 1, 3),
    # ── Moderate bullish signals ──────────────────────────────────────────────
    (["賣出", "停利", "獲利了結", "出場", "認損"], 1, 2),
    (["心碎", "白做工", "不懂", "怎麼辦", "救我"], 1, 2),
    # ── Weak bullish signals ──────────────────────────────────────────────────
    (["觀望", "等", "修正", "怕", "謹慎"], 1, 1),
    # ── Strong bearish signals (poster over-confident / chasing) ─────────────
    (["漲停買", "漲停追", "我今天才買漲停", "市價掛", "追漲"], -1, 3),
    (["無敵", "一定漲", "必漲", "破除迷信", "Make"], -1, 3),
    # ── Moderate bearish signals ──────────────────────────────────────────────
    (["買進", "加碼", "補倉", "買了", "入手", "佈局", "加一點", "補一點", "再買"], -1, 2),
    (["看多", "多頭", "應該漲", "會漲", "繼續持有"], -1, 2),
    # ── Weak bearish signals ──────────────────────────────────────────────────
    (["持有", "觀察", "等待", "慢慢漲", "長期"], -1, 1),
]


def classify(text: str) -> tuple[int, int] | None:
    """
    Return (direction, strength) for a post, or None if no rule matched.
    Earlier rules in the list take priority (first-match wins).
    """
    for keywords, direction, strength in SENTIMENT_RULES:
        if any(kw in text for kw in keywords):
            return direction, strength
    return None


def build_tooltip(post_text: str, direction: int, strength: int, dt: datetime) -> str:
    """Produce a multi-line tooltip string matching the existing Pine format."""
    # Derive a short action label from the first matched keyword
    action = "貼文"
    for keywords, d, s in SENTIMENT_RULES:
        if d == direction and s == strength:
            for kw in keywords:
                if kw in post_text:
                    action = kw
                    break
            break

    dir_label = "偏多 ▲" if direction == 1 else "偏空 ▼"
    star_map = {1: "★☆☆", 2: "★★☆", 3: "★★★"}
    stars = star_map.get(strength, "★☆☆")

    # Truncate post text for tooltip (Pine has a practical limit)
    snippet = post_text.replace("\n", " ").strip()
    if len(snippet) > 60:
        snippet = snippet[:57] + "..."

    date_str = dt.astimezone(timezone.utc).strftime("FB %m/%d %H:%M")
    return (
        f"{action}\n"
        f"指標: {dir_label} | 強度: {stars}\n"
        f"{date_str} {snippet}"
    )


def load_state() -> int:
    """Return last processed timestamp in unix ms (0 = fetch all)."""
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            return int(data.get("last_fetched_unix_ms", 0))
        except (json.JSONDecodeError, ValueError):
            pass
    return 0


def save_state(last_unix_ms: int) -> None:
    now_utc = datetime.now(timezone.utc).isoformat()
    STATE_FILE.write_text(
        json.dumps(
            {"last_fetched_unix_ms": last_unix_ms, "last_run_utc": now_utc},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def fetch_posts() -> list[dict]:
    """Fetch recent posts from the FB page using facebook-scraper."""
    if not FB_PAGE_ID:
        print("WARNING: FB_PAGE_ID not set. Skipping scrape.")
        return []

    cookies: dict | None = None
    if FB_COOKIES:
        try:
            cookies = json.loads(FB_COOKIES)
        except json.JSONDecodeError:
            print("WARNING: FB_COOKIES is not valid JSON. Proceeding without cookies.")

    kwargs: dict = {
        "pages": 1,
        "options": {"posts_per_page": MAX_POSTS_PER_RUN},
    }
    if cookies:
        kwargs["cookies"] = cookies

    posts: list[dict] = []
    try:
        for post in get_posts(FB_PAGE_ID, **kwargs):
            posts.append(post)
            if len(posts) >= MAX_POSTS_PER_RUN:
                break
    except (ConnectionError, TimeoutError, ValueError, RuntimeError) as exc:
        print(f"ERROR while fetching posts: {exc}")

    return posts


def main() -> None:
    last_unix_ms = load_state()
    raw_posts = fetch_posts()

    new_events: list[dict] = []
    latest_unix_ms = last_unix_ms

    for post in raw_posts:
        post_time: datetime | None = post.get("time")
        if post_time is None:
            continue

        # Ensure timezone-aware
        if post_time.tzinfo is None:
            post_time = post_time.replace(tzinfo=timezone.utc)

        unix_ms = int(post_time.timestamp() * 1000)

        # Skip already-processed posts
        if unix_ms <= last_unix_ms:
            continue

        text: str = post.get("text") or post.get("post_text") or ""
        if not text:
            continue

        result = classify(text)
        if result is None:
            # Post didn't match any rule – skip it
            continue

        direction, strength = result
        tooltip = build_tooltip(text, direction, strength, post_time)

        new_events.append(
            {
                "unix_ms": unix_ms,
                "direction": direction,
                "strength": strength,
                "tooltip": tooltip,
            }
        )

        if unix_ms > latest_unix_ms:
            latest_unix_ms = unix_ms

    # Sort ascending by time so Pine inserts in chronological order
    new_events.sort(key=lambda e: e["unix_ms"])

    OUTPUT_FILE.write_text(
        json.dumps(new_events, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if new_events:
        save_state(latest_unix_ms)
        print(f"✅ {len(new_events)} new event(s) classified and written to {OUTPUT_FILE}")
    else:
        # Still update last_run_utc even when nothing new
        save_state(last_unix_ms)
        print("ℹ️  No new classifiable events found this run.")


if __name__ == "__main__":
    main()
