#!/usr/bin/env python3
"""
build_site_data.py
──────────────────
Parses all events embedded in 8zz-indicator.pine, computes win/loss
outcomes using 0050.TW (Taiwan 50 ETF) as the reference ticker via
yfinance, and writes docs/events.json for the GitHub Pages dashboard.

Outcome logic (mirrors the Pine Script's "Mode B – flip exit"):
  • Each "flip" event (direction change) opens a new prediction.
  • The prediction closes when the next flip event fires (or at today
    if still open).
  • WIN  = reference price moved in the predicted direction.
  • OPEN = prediction is still active (not yet resolved).

Reference ticker: 0050.TW (primary), TWI fallback, ^GSPC for US events.
We use 0050.TW for every event to keep the comparison consistent and
simple – the indicator is framed as a market-wide sentiment gauge, not
an individual-stock tracker.

Usage:
  python scripts/build_site_data.py
"""

import json
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed. Run: pip install yfinance")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
PINE_FILE = ROOT / "8zz-indicator.pine"
OUTPUT_FILE = ROOT / "docs" / "events.json"

REFERENCE_TICKER = "0050.TW"
# Observation window (days) used when no next-flip exists yet (open position)
OPEN_WINDOW_DAYS = 14


# ── Helpers ───────────────────────────────────────────────────────────────────
def stars(strength: int) -> str:
    return {1: "★☆☆", 2: "★★☆", 3: "★★★"}.get(strength, "?")


def parse_events(pine_text: str) -> list[dict]:
    """
    Extract all (time_ms, dir, strength, tooltip) tuples from the .pine file
    in declaration order.
    """
    # Match every quartet of array.push calls
    pattern = re.compile(
        r"array\.push\(evt_time,\s*(\d+)\)\s*"
        r"array\.push\(evt_dir,\s*(-?\d+)\)\s*"
        r"array\.push\(evt_str,\s*(\d+)\)\s*"
        r'array\.push\(evt_tips,\s*"((?:[^"\\]|\\.)*)"\)',
        re.DOTALL,
    )
    events = []
    for m in pattern.finditer(pine_text):
        unix_ms = int(m.group(1))
        direction = int(m.group(2))
        strength = int(m.group(3))
        tooltip = m.group(4).replace("\\n", "\n")
        dt = datetime.fromtimestamp(unix_ms / 1000, tz=timezone.utc)
        events.append(
            {
                "unix_ms": unix_ms,
                "date": dt.strftime("%Y-%m-%d"),
                "time_utc": dt.isoformat(),
                "direction": direction,
                "dir_label": "偏多 ▲" if direction == 1 else "偏空 ▼",
                "strength": strength,
                "strength_label": stars(strength),
                "tooltip": tooltip,
                "is_flip": False,  # set below
            }
        )
    return events


def mark_flips(events: list[dict]) -> list[dict]:
    """
    Walk events in order; mark an event as a flip when its direction
    differs from the previous accepted direction (same logic as Pine).
    """
    current_dir = 0
    for evt in events:
        if evt["direction"] != current_dir:
            evt["is_flip"] = True
            current_dir = evt["direction"]
    return events


def fetch_price_history(ticker: str, start: datetime, end: datetime) -> dict[str, float]:
    """
    Return {date_str: close_price} for the given ticker and date range.
    date_str format: 'YYYY-MM-DD'.
    """
    # Extend end by 5 days to handle weekends / market holidays
    end_padded = end + timedelta(days=5)
    try:
        df = yf.download(
            ticker,
            start=start.strftime("%Y-%m-%d"),
            end=end_padded.strftime("%Y-%m-%d"),
            auto_adjust=True,
            progress=False,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: yfinance error for {ticker}: {exc}")
        return {}

    if df.empty:
        return {}

    prices: dict[str, float] = {}
    for idx, row in df.iterrows():
        date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
        close = float(row["Close"].iloc[0]) if hasattr(row["Close"], "iloc") else float(row["Close"])
        prices[date_str] = close
    return prices


def nearest_close(prices: dict[str, float], target_dt: datetime) -> tuple[str | None, float | None]:
    """
    Find the closest available trading day on or after target_dt.
    Returns (date_str, price) or (None, None) if not found within 7 days.
    """
    for delta in range(8):
        d = (target_dt + timedelta(days=delta)).strftime("%Y-%m-%d")
        if d in prices:
            return d, prices[d]
    return None, None


def compute_outcomes(events: list[dict], prices: dict[str, float]) -> list[dict]:
    """
    For each flip event, determine the entry date/price and exit date/price
    (next flip or +OPEN_WINDOW_DAYS), then compute pnl_pct and outcome.
    """
    flips = [e for e in events if e["is_flip"]]
    today = datetime.now(timezone.utc)

    for i, flip in enumerate(flips):
        entry_dt = datetime.fromisoformat(flip["time_utc"])
        entry_date, entry_price = nearest_close(prices, entry_dt)

        if entry_date is None or entry_price is None:
            flip["entry_price"] = None
            flip["exit_price"] = None
            flip["exit_date"] = None
            flip["pnl_pct"] = None
            flip["outcome"] = "unknown"
            continue

        flip["entry_date"] = entry_date
        flip["entry_price"] = round(entry_price, 2)

        # Exit = next flip's event time, or today if still open
        if i + 1 < len(flips):
            exit_dt = datetime.fromisoformat(flips[i + 1]["time_utc"])
            is_open = False
        else:
            exit_dt = today
            is_open = True

        exit_date, exit_price = nearest_close(prices, exit_dt)

        if exit_date is None or exit_price is None:
            flip["exit_price"] = None
            flip["exit_date"] = None
            flip["pnl_pct"] = None
            flip["outcome"] = "open" if is_open else "unknown"
            continue

        flip["exit_date"] = exit_date
        flip["exit_price"] = round(exit_price, 2)

        price_change_pct = (exit_price - entry_price) / entry_price * 100
        # direction 1 = long signal → win if price rose
        # direction -1 = short signal → win if price fell
        pnl_pct = flip["direction"] * price_change_pct
        flip["pnl_pct"] = round(pnl_pct, 2)

        if is_open:
            flip["outcome"] = "open"
        elif pnl_pct > 0:
            flip["outcome"] = "win"
        elif pnl_pct < 0:
            flip["outcome"] = "loss"
        else:
            flip["outcome"] = "flat"

    return events


def build_stats(events: list[dict]) -> dict:
    flips = [e for e in events if e["is_flip"]]
    resolved = [f for f in flips if f.get("outcome") in ("win", "loss", "flat")]
    wins = [f for f in resolved if f["outcome"] == "win"]
    losses = [f for f in resolved if f["outcome"] == "loss"]
    open_flips = [f for f in flips if f.get("outcome") == "open"]

    win_rate = len(wins) / len(resolved) * 100 if resolved else 0
    pnls = [f["pnl_pct"] for f in resolved if f.get("pnl_pct") is not None]
    avg_pnl = sum(pnls) / len(pnls) if pnls else 0

    # By direction
    bullish_flips = [f for f in resolved if f["direction"] == 1]
    bearish_flips = [f for f in resolved if f["direction"] == -1]
    bull_wins = sum(1 for f in bullish_flips if f["outcome"] == "win")
    bear_wins = sum(1 for f in bearish_flips if f["outcome"] == "win")

    # By strength
    strength_stats: dict[int, dict] = {}
    for s in (1, 2, 3):
        sg = [f for f in resolved if f["strength"] == s]
        sw = sum(1 for f in sg if f["outcome"] == "win")
        strength_stats[s] = {
            "total": len(sg),
            "wins": sw,
            "win_rate": round(sw / len(sg) * 100, 1) if sg else 0,
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "reference_ticker": REFERENCE_TICKER,
        "total_events": len(events),
        "total_flips": len(flips),
        "resolved_flips": len(resolved),
        "open_flips": len(open_flips),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 1),
        "avg_pnl_pct": round(avg_pnl, 2),
        "bullish_flips": len(bullish_flips),
        "bullish_wins": bull_wins,
        "bullish_win_rate": round(bull_wins / len(bullish_flips) * 100, 1) if bullish_flips else 0,
        "bearish_flips": len(bearish_flips),
        "bearish_wins": bear_wins,
        "bearish_win_rate": round(bear_wins / len(bearish_flips) * 100, 1) if bearish_flips else 0,
        "by_strength": strength_stats,
    }


def main() -> None:
    pine_text = PINE_FILE.read_text(encoding="utf-8")
    events = parse_events(pine_text)
    if not events:
        print("ERROR: No events found in the .pine file.")
        sys.exit(1)

    events = mark_flips(events)
    print(f"Parsed {len(events)} events, {sum(1 for e in events if e['is_flip'])} flip signals.")

    # Fetch price history spanning all events
    first_dt = datetime.fromisoformat(events[0]["time_utc"])
    last_dt = datetime.now(timezone.utc) + timedelta(days=1)
    print(f"Fetching {REFERENCE_TICKER} price history {first_dt.date()} → {last_dt.date()} …")
    prices = fetch_price_history(REFERENCE_TICKER, first_dt, last_dt)
    print(f"Got {len(prices)} trading days of data.")

    events = compute_outcomes(events, prices)
    stats = build_stats(events)

    output = {"stats": stats, "events": events}
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        f"✅ docs/events.json written — "
        f"{stats['wins']}W / {stats['losses']}L / {stats['open_flips']} open "
        f"(win rate: {stats['win_rate']}%)"
    )


if __name__ == "__main__":
    main()
