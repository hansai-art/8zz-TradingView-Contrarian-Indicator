#!/usr/bin/env python3
"""
build_site_data.py
──────────────────
Parses all events embedded in 8zz-indicator.pine, computes win/loss
outcomes using per-event tickers (from the evt_ticker array) via yfinance,
and writes docs/events.json for the GitHub Pages dashboard.

Two exit modes are computed in parallel:
  • Mode A – Fixed 14 trading days after entry.
  • Mode B – Flip-based exit: holds until the next direction flip fires
             (or today if still open).

Ticker assignment:
  Each event carries its own ticker from the pine script's evt_ticker array.
  Empty ticker → fallback to FALLBACK_TICKER (0050.TW).

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

FALLBACK_TICKER = "0050.TW"
# Number of trading days for Mode A fixed-bar exit
MODE_A_HOLD_BARS = 14


# ── Helpers ───────────────────────────────────────────────────────────────────
def stars(strength: int) -> str:
    return {1: "★☆☆", 2: "★★☆", 3: "★★★"}.get(strength, "?")


def parse_events(pine_text: str) -> list[dict]:
    """
    Extract all (time_ms, dir, strength, tooltip, ticker) tuples from the
    .pine file in declaration order.
    """
    # Match every quintet of array.push calls (evt_ticker added as 5th)
    pattern = re.compile(
        r"array\.push\(evt_time,\s*(\d+)\)\s*"
        r"array\.push\(evt_dir,\s*(-?\d+)\)\s*"
        r"array\.push\(evt_str,\s*(\d+)\)\s*"
        r'array\.push\(evt_tips,\s*"((?:[^"\\]|\\.)*)"\)\s*'
        r'array\.push\(evt_ticker,\s*"([^"]*)"\)',
        re.DOTALL,
    )
    events = []
    for m in pattern.finditer(pine_text):
        unix_ms = int(m.group(1))
        direction = int(m.group(2))
        strength = int(m.group(3))
        tooltip = m.group(4).replace("\\n", "\n")
        raw_ticker = m.group(5).strip()
        ticker = raw_ticker if raw_ticker else FALLBACK_TICKER
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
                "ticker": ticker,
                "ticker_is_fallback": not raw_ticker,
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


def fetch_all_prices(events: list[dict], first_dt: datetime, last_dt: datetime) -> dict[str, dict[str, float]]:
    """
    Download price history for every unique ticker referenced by events.
    Returns {ticker: {date_str: price}}.
    Falls back to FALLBACK_TICKER data when a ticker yields no data.
    """
    tickers = sorted({e["ticker"] for e in events})
    all_prices: dict[str, dict[str, float]] = {}

    for ticker in tickers:
        print(f"  Fetching {ticker} …")
        prices = fetch_price_history(ticker, first_dt, last_dt)
        if not prices:
            print(f"  WARNING: No data for {ticker}; using {FALLBACK_TICKER} as fallback.")
            if FALLBACK_TICKER not in all_prices:
                all_prices[FALLBACK_TICKER] = fetch_price_history(FALLBACK_TICKER, first_dt, last_dt)
            all_prices[ticker] = all_prices[FALLBACK_TICKER]
        else:
            all_prices[ticker] = prices

    return all_prices


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


def compute_outcomes_mode_b(events: list[dict], all_prices: dict[str, dict[str, float]]) -> list[dict]:
    """
    Mode B – Flip-based exit.
    Each flip event is open until the next flip fires (or today if still open).
    Fills: entry_date, entry_price, exit_date, exit_price, pnl_pct, outcome.
    """
    flips = [e for e in events if e["is_flip"]]
    today = datetime.now(timezone.utc)

    for i, flip in enumerate(flips):
        is_last = i + 1 >= len(flips)
        prices = all_prices.get(flip["ticker"], {})

        entry_dt = datetime.fromisoformat(flip["time_utc"])
        entry_date, entry_price = nearest_close(prices, entry_dt)

        if entry_date is None or entry_price is None:
            flip["entry_date"] = None
            flip["entry_price"] = None
            flip["exit_date"] = None
            flip["exit_price"] = None
            flip["pnl_pct"] = None
            flip["outcome"] = "open" if is_last else "unknown"
            continue

        flip["entry_date"] = entry_date
        flip["entry_price"] = round(entry_price, 2)

        if not is_last:
            exit_dt = datetime.fromisoformat(flips[i + 1]["time_utc"])
            is_open = False
        else:
            exit_dt = today
            is_open = True

        exit_date, exit_price = nearest_close(prices, exit_dt)

        if exit_date is None or exit_price is None:
            flip["exit_date"] = None
            flip["exit_price"] = None
            flip["pnl_pct"] = None
            flip["outcome"] = "open" if is_open else "unknown"
            continue

        flip["exit_date"] = exit_date
        flip["exit_price"] = round(exit_price, 2)

        price_change_pct = (exit_price - entry_price) / entry_price * 100
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


def compute_outcomes_mode_a(
    events: list[dict],
    all_prices: dict[str, dict[str, float]],
    hold_bars: int = MODE_A_HOLD_BARS,
) -> list[dict]:
    """
    Mode A – Fixed trading-day exit.
    Each flip event exits exactly `hold_bars` trading days after entry
    (counting only days in the ticker's own price history).
    Fills: exit_date_a, exit_price_a, pnl_pct_a, outcome_a.
    Uses entry_date already set by Mode B; recomputes if missing.
    """
    flips = [e for e in events if e["is_flip"]]

    for flip in flips:
        prices = all_prices.get(flip["ticker"], {})
        sorted_dates = sorted(prices.keys())

        # Resolve entry date (reuse Mode B result when available)
        entry_date = flip.get("entry_date")
        entry_price_val = flip.get("entry_price")

        if entry_date is None:
            entry_dt = datetime.fromisoformat(flip["time_utc"])
            entry_date, ep = nearest_close(prices, entry_dt)
            entry_price_val = ep

        if entry_date is None or entry_price_val is None or entry_date not in sorted_dates:
            flip["exit_date_a"] = None
            flip["exit_price_a"] = None
            flip["pnl_pct_a"] = None
            flip["outcome_a"] = "open"
            continue

        entry_price_val = float(entry_price_val)
        try:
            entry_idx = sorted_dates.index(entry_date)
        except ValueError:
            flip["exit_date_a"] = None
            flip["exit_price_a"] = None
            flip["pnl_pct_a"] = None
            flip["outcome_a"] = "open"
            continue

        exit_idx = entry_idx + hold_bars
        if exit_idx >= len(sorted_dates):
            # Not enough trading days yet → position still open
            flip["exit_date_a"] = None
            flip["exit_price_a"] = None
            flip["pnl_pct_a"] = None
            flip["outcome_a"] = "open"
            continue

        exit_date_a = sorted_dates[exit_idx]
        exit_price_a = prices[exit_date_a]

        flip["exit_date_a"] = exit_date_a
        flip["exit_price_a"] = round(exit_price_a, 2)

        price_change_pct = (exit_price_a - entry_price_val) / entry_price_val * 100
        pnl_pct_a = flip["direction"] * price_change_pct
        flip["pnl_pct_a"] = round(pnl_pct_a, 2)

        if pnl_pct_a > 0:
            flip["outcome_a"] = "win"
        elif pnl_pct_a < 0:
            flip["outcome_a"] = "loss"
        else:
            flip["outcome_a"] = "flat"

    return events


def _build_mode_stats(
    flips: list[dict],
    outcome_key: str,
    pnl_key: str,
) -> dict:
    """Build win/loss/open stats for a single exit mode."""
    resolved = [f for f in flips if f.get(outcome_key) in ("win", "loss", "flat")]
    wins = [f for f in resolved if f[outcome_key] == "win"]
    losses = [f for f in resolved if f[outcome_key] == "loss"]
    open_flips = [f for f in flips if f.get(outcome_key) == "open"]

    win_rate = len(wins) / len(resolved) * 100 if resolved else 0
    pnls = [f[pnl_key] for f in resolved if f.get(pnl_key) is not None]
    avg_pnl = sum(pnls) / len(pnls) if pnls else 0

    bullish = [f for f in resolved if f["direction"] == 1]
    bearish = [f for f in resolved if f["direction"] == -1]
    bull_wins = sum(1 for f in bullish if f[outcome_key] == "win")
    bear_wins = sum(1 for f in bearish if f[outcome_key] == "win")

    strength_stats: dict[int, dict] = {}
    for s in (1, 2, 3):
        sg = [f for f in resolved if f["strength"] == s]
        sw = sum(1 for f in sg if f[outcome_key] == "win")
        strength_stats[s] = {
            "total": len(sg),
            "wins": sw,
            "win_rate": round(sw / len(sg) * 100, 1) if sg else 0,
        }

    return {
        "resolved_flips": len(resolved),
        "open_flips": len(open_flips),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 1),
        "avg_pnl_pct": round(avg_pnl, 2),
        "bullish_flips": len(bullish),
        "bullish_wins": bull_wins,
        "bullish_win_rate": round(bull_wins / len(bullish) * 100, 1) if bullish else 0,
        "bearish_flips": len(bearish),
        "bearish_wins": bear_wins,
        "bearish_win_rate": round(bear_wins / len(bearish) * 100, 1) if bearish else 0,
        "by_strength": strength_stats,
    }


def build_stats(events: list[dict]) -> dict:
    flips = [e for e in events if e["is_flip"]]

    mode_b = _build_mode_stats(flips, outcome_key="outcome", pnl_key="pnl_pct")
    mode_a = _build_mode_stats(flips, outcome_key="outcome_a", pnl_key="pnl_pct_a")

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fallback_ticker": FALLBACK_TICKER,
        "mode_a_hold_bars": MODE_A_HOLD_BARS,
        "total_events": len(events),
        "total_flips": len(flips),
        # Top-level convenience fields mirror Mode B (flip-based) for backwards compat
        "resolved_flips": mode_b["resolved_flips"],
        "open_flips": mode_b["open_flips"],
        "wins": mode_b["wins"],
        "losses": mode_b["losses"],
        "win_rate": mode_b["win_rate"],
        "avg_pnl_pct": mode_b["avg_pnl_pct"],
        "bullish_flips": mode_b["bullish_flips"],
        "bullish_wins": mode_b["bullish_wins"],
        "bullish_win_rate": mode_b["bullish_win_rate"],
        "bearish_flips": mode_b["bearish_flips"],
        "bearish_wins": mode_b["bearish_wins"],
        "bearish_win_rate": mode_b["bearish_win_rate"],
        "by_strength": mode_b["by_strength"],
        # Per-mode breakdown
        "mode_b": mode_b,
        "mode_a": mode_a,
    }


def main() -> None:
    pine_text = PINE_FILE.read_text(encoding="utf-8")
    events = parse_events(pine_text)
    if not events:
        print("ERROR: No events found in the .pine file.")
        sys.exit(1)

    events = mark_flips(events)
    flip_count = sum(1 for e in events if e["is_flip"])
    print(f"Parsed {len(events)} events, {flip_count} flip signals.")

    unique_tickers = sorted({e["ticker"] for e in events})
    print(f"Unique tickers: {unique_tickers}")

    first_dt = datetime.fromisoformat(events[0]["time_utc"])
    last_dt = datetime.now(timezone.utc) + timedelta(days=1)
    print(f"Fetching price history {first_dt.date()} → {last_dt.date()} …")
    all_prices = fetch_all_prices(events, first_dt, last_dt)

    events = compute_outcomes_mode_b(events, all_prices)
    events = compute_outcomes_mode_a(events, all_prices, hold_bars=MODE_A_HOLD_BARS)
    stats = build_stats(events)

    output = {"stats": stats, "events": events}
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    mb = stats["mode_b"]
    ma = stats["mode_a"]
    print(
        f"✅ docs/events.json written\n"
        f"   Mode B (flip)  : {mb['wins']}W / {mb['losses']}L / {mb['open_flips']} open  "
        f"→ win rate {mb['win_rate']}%  avg {mb['avg_pnl_pct']:+.2f}%\n"
        f"   Mode A ({MODE_A_HOLD_BARS} bars): {ma['wins']}W / {ma['losses']}L / {ma['open_flips']} open  "
        f"→ win rate {ma['win_rate']}%  avg {ma['avg_pnl_pct']:+.2f}%"
    )


if __name__ == "__main__":
    main()
