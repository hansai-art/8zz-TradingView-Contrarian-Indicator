#!/usr/bin/env python3
"""
fetch_fb_events.py
──────────────────
Scrapes public FB posts from the tracked page, classifies each post's
sentiment using Claude AI (with keyword-rule fallback), then writes new
events to data/new_events.json for update_pine_script.py to consume.

State tracking:
  data/last_event_timestamp.json  – stores last successfully processed
                                    post timestamp (unix ms) so re-runs
                                    are idempotent.

Environment variables (set as GitHub Actions secrets):
  FB_PAGE_ID        – public page ID or username (e.g. "SomePage")
  FB_COOKIES        – optional; FB session cookies as JSON string for pages
                      that require login (leave empty for fully public pages)
  ANTHROPIC_API_KEY – Claude Haiku API key for AI classification.
                      If absent, falls back to the keyword rule table.

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
FB_PAGE_ID: str        = os.environ.get("FB_PAGE_ID", "")
FB_COOKIES: str        = os.environ.get("FB_COOKIES", "")
GOOGLE_API_KEY: str = os.environ.get("GOOGLE_API_KEY", "")
MAX_POSTS_PER_RUN: int = 10

# ── Claude AI classification ──────────────────────────────────────────────────

_SYSTEM = """你是「8zz 反指標」系統的情緒分析器，專門分析台灣散戶的 Facebook 投資貼文。

【核心原則：情緒 > 動作】
就算他「買進」，但情緒是痛苦/被套/停損 → direction: 1（偏多▲）
就算他「賣出」，但情緒是得意/歡呼/獲利了結 → direction: -1（偏空▼）
關鍵是發文者當下的心理狀態，不是他做了什麼動作。

direction:
  1  = 偏多 ▲（恐慌/痛苦/被套 → 市場可能近底部）
 -1  = 偏空 ▼（亢奮/追漲/自大 → 市場可能近頂部）
  0  = 跳過（與投資無關，或情緒完全中性）

strength（情緒強度）:
  1 = 輕微  2 = 明顯  3 = 極端（爆倉/漲停追買/歡天喜地）

ticker:
- 貼文明確提到特定標的 → 輸出 yfinance ticker（如 5274.TWO、TSM、0050.TW、GLD、BTCUSDT）
- 不確定或未提及 → 輸出空字串 ""

輸出純 JSON，不加 markdown：
{"direction": 1, "strength": 2, "action": "停損", "ticker": "", "reasoning": "一句話說明"}"""

_EXAMPLES = [
    {"role": "user",      "content": "我今天停損出場了，損失超過10萬，心情很差"},
    {"role": "assistant", "content": '{"direction": 1, "strength": 3, "action": "停損", "ticker": "", "reasoning": "大額停損出場，情緒極度痛苦，強烈偏多訊號"}'},
    {"role": "user",      "content": "停損渣男鈦昇後買信驊，情緒衝動了"},
    {"role": "assistant", "content": '{"direction": 1, "strength": 3, "action": "衝動(復仇)", "ticker": "5274.TWO", "reasoning": "停損後衝動買進，情緒是痛苦+衝動，即使買進動作仍是偏多訊號"}'},
    {"role": "user",      "content": "今天漲停板追進去了！感覺這支會繼續飆！"},
    {"role": "assistant", "content": '{"direction": -1, "strength": 3, "action": "漲停追買", "ticker": "", "reasoning": "漲停追買加上極度亢奮的FOMO情緒，強烈偏空訊號"}'},
    {"role": "user",      "content": "加碼台積電，長期看好半導體"},
    {"role": "assistant", "content": '{"direction": -1, "strength": 2, "action": "加碼", "ticker": "TSM", "reasoning": "主動加碼且語氣自信看好，屬於偏空訊號"}'},
    {"role": "user",      "content": "今天天氣真好，出去走走"},
    {"role": "assistant", "content": '{"direction": 0, "strength": 1, "action": "", "ticker": "", "reasoning": "與投資無關的生活貼文"}'},
]


def classify_with_ai(text: str) -> dict | None:
    """
    Call Google Gemini Flash to classify a post.
    Returns dict(direction, strength, action, ticker, reasoning), or None on failure.
    """
    if not GOOGLE_API_KEY:
        return None

    try:
        import google.generativeai as genai
    except ImportError:
        print("WARNING: google-generativeai not installed. Falling back to keyword rules.")
        return None

    try:
        genai.configure(api_key=GOOGLE_API_KEY)
        model = genai.GenerativeModel(
            model_name="gemini-2.0-flash-lite",
            system_instruction=_SYSTEM,
        )

        # Build few-shot prompt as a single string
        few_shot = ""
        for i in range(0, len(_EXAMPLES), 2):
            u = _EXAMPLES[i]["content"]
            a = _EXAMPLES[i + 1]["content"]
            few_shot += f"貼文：{u}\n回答：{a}\n\n"
        prompt = few_shot + f"貼文：{text}\n回答："

        response = model.generate_content(prompt)
        raw = response.text.strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.DOTALL).strip()
        result = json.loads(raw)

        return {
            "direction": int(result.get("direction", 0)),
            "strength":  max(1, min(3, int(result.get("strength", 1)))),
            "action":    str(result.get("action", "")).strip(),
            "ticker":    str(result.get("ticker", "")).strip(),
            "reasoning": str(result.get("reasoning", "")).strip(),
        }
    except Exception as exc:
        print(f"WARNING: Gemini API error ({type(exc).__name__}: {exc}). Falling back to keyword rules.")
        return None


# ── Keyword-rule fallback ─────────────────────────────────────────────────────
# Each entry: (keywords_list, direction, base_strength)
SENTIMENT_RULES: list[tuple[list[str], int, int]] = [
    # ── Strong bullish (poster in distress / capitulating) ────────────────────
    (["停損", "認賠", "虧損", "損失", "全賠", "出清停損", "畢業", "爆倉"], 1, 3),
    (["被套", "套牢", "跌停", "住套房", "房貸", "公園", "淨值歸零"],       1, 3),
    # ── Moderate bullish ──────────────────────────────────────────────────────
    (["賣出", "停利", "獲利了結", "出場", "認損"],    1, 2),
    (["心碎", "白做工", "不懂", "怎麼辦", "救我"],    1, 2),
    # ── Weak bullish ──────────────────────────────────────────────────────────
    (["觀望", "等", "修正", "怕", "謹慎"],            1, 1),
    # ── Strong bearish (over-confident / chasing) ─────────────────────────────
    (["漲停買", "漲停追", "市價掛", "追漲"],          -1, 3),
    (["無敵", "一定漲", "必漲"],                      -1, 3),
    # ── Moderate bearish ──────────────────────────────────────────────────────
    (["買進", "加碼", "補倉", "買了", "入手", "佈局", "再買"], -1, 2),
    (["看多", "多頭", "應該漲", "會漲", "繼續持有"],  -1, 2),
    # ── Weak bearish ──────────────────────────────────────────────────────────
    (["持有", "觀察", "等待", "慢慢漲", "長期"],       -1, 1),
]


def classify_with_keywords(text: str) -> tuple[int, int, str] | None:
    """Return (direction, strength, matched_keyword) or None if no rule matched."""
    for keywords, direction, strength in SENTIMENT_RULES:
        for kw in keywords:
            if kw in text:
                return direction, strength, kw
    return None


# ── Tooltip builder ───────────────────────────────────────────────────────────

def build_tooltip(post_text: str, direction: int, strength: int, action: str, dt: datetime) -> str:
    """Produce a multi-line tooltip string matching the existing Pine format."""
    dir_label = "偏多 ▲" if direction == 1 else "偏空 ▼"
    stars = {1: "★☆☆", 2: "★★☆", 3: "★★★"}.get(strength, "★☆☆")

    snippet = post_text.replace("\n", " ").strip()
    if len(snippet) > 60:
        snippet = snippet[:57] + "..."

    date_str = dt.astimezone(timezone.utc).strftime("FB %m/%d %H:%M")
    return (
        f"{action or '貼文'}\n"
        f"指標: {dir_label} | 強度: {stars}\n"
        f"{date_str} {snippet}"
    )


# ── State persistence ─────────────────────────────────────────────────────────

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


# ── FB scraping ───────────────────────────────────────────────────────────────

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

    kwargs: dict = {"pages": 1, "options": {"posts_per_page": MAX_POSTS_PER_RUN}}
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


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    using_ai = bool(GOOGLE_API_KEY)
    print(f"ℹ️  Classifier: {'Gemini Flash (AI)' if using_ai else 'keyword rules (set GOOGLE_API_KEY to enable AI)'}")

    last_unix_ms = load_state()
    raw_posts = fetch_posts()

    new_events: list[dict] = []
    latest_unix_ms = last_unix_ms

    for post in raw_posts:
        post_time: datetime | None = post.get("time")
        if post_time is None:
            continue
        if post_time.tzinfo is None:
            post_time = post_time.replace(tzinfo=timezone.utc)

        unix_ms = int(post_time.timestamp() * 1000)
        if unix_ms <= last_unix_ms:
            continue

        text: str = post.get("text") or post.get("post_text") or ""
        if not text:
            continue

        # ── Try Gemini first ──────────────────────────────────────────────────
        direction, strength, action, ticker = 0, 1, "", ""
        ai = classify_with_ai(text)

        if ai is not None:
            direction = ai["direction"]
            strength  = ai["strength"]
            action    = ai["action"]
            ticker    = ai["ticker"]
            print(f"  [Gemini] dir={direction} str={strength} action='{action}' ticker='{ticker or '(0050 fallback)'}'")
        else:
            # ── Keyword fallback ──────────────────────────────────────────────
            kw = classify_with_keywords(text)
            if kw is None:
                print("  [keyword] no match – skipping")
                continue
            direction, strength, action = kw
            print(f"  [keyword] dir={direction} str={strength} action='{action}'")

        if direction == 0:
            print("  → direction=0, skipping (not investment-related)")
            continue

        tooltip = build_tooltip(text, direction, strength, action, post_time)
        new_events.append({
            "unix_ms":   unix_ms,
            "direction": direction,
            "strength":  strength,
            "ticker":    ticker,
            "tooltip":   tooltip,
        })
        if unix_ms > latest_unix_ms:
            latest_unix_ms = unix_ms

    new_events.sort(key=lambda e: e["unix_ms"])
    OUTPUT_FILE.write_text(
        json.dumps(new_events, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if new_events:
        save_state(latest_unix_ms)
        print(f"✅ {len(new_events)} new event(s) written to {OUTPUT_FILE}")
    else:
        save_state(last_unix_ms)
        print("ℹ️  No new classifiable events found this run.")


if __name__ == "__main__":
    main()
