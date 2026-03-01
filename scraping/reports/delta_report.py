# scraping/reports/delta_report.py
"""
Delta report generator.

Reads delta files written by SnapshotStore and produces:
  - A formatted terminal table for a single day
  - A historical growth series for the past N days
  - Optional JSON export

Usage:
  # Report for today
  python -m scraping.reports.delta_report

  # Report for a specific date
  python -m scraping.reports.delta_report --date 2025-07-01

  # Historical series for the last 30 days
  python -m scraping.reports.delta_report --history 30

  # Export to JSON
  python -m scraping.reports.delta_report --history 30 --export report.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from scraping.storage.snapshot_store import SnapshotStore, _delta_filename

# Formatting

def _bar(value: int, max_value: int, width: int = 20, char: str = "█") -> str:
    """ASCII progress bar proportional to value / max_value."""
    if max_value == 0:
        return " " * width
    filled = round(value / max_value * width)
    return char * filled + "░" * (width - filled)


def format_single_delta(delta: dict) -> str:
    """Formats a single delta entry as a readable block."""
    lines = [
        f"  Source        : {delta['source']}",
        f"  Type          : {delta['chat_type']}",
        f"  Current date  : {delta['date']}",
        f"  Previous date : {delta.get('prev_date') or 'N/A (first snapshot)'}",
        f"  Total today   : {delta['total_today']:,}",
        f"  Total previous: {delta['total_prev']:,}",
        f"  New entries   : +{delta['n_new']:,}",
        f"  Removed       : -{delta['n_removed']:,}",
        f"  Net change    : {delta['net_change']:+,}",
    ]
    if delta["n_new"] > 0:
        preview = delta["new"][:5]
        suffix  = f"  … and {delta['n_new'] - 5} more" if delta["n_new"] > 5 else ""
        lines.append(f"  New (preview) : {', '.join('@' + u for u in preview)}{suffix}")
    if delta["n_removed"] > 0:
        preview = delta["removed"][:5]
        suffix  = f"  … and {delta['n_removed'] - 5} more" if delta["n_removed"] > 5 else ""
        lines.append(f"  Removed (prev): {', '.join('@' + u for u in preview)}{suffix}")
    return "\n".join(lines)


def format_daily_report(deltas: list[dict], run_date: date) -> str:
    """Full report table for a single day."""
    if not deltas:
        return f"No data found for {run_date.isoformat()}"

    header = (
        f"\n{'━' * 72}\n"
        f"    Delta Report — {run_date.isoformat()}\n"
        f"{'━' * 72}"
    )
    rows = [format_single_delta(d) for d in deltas]

    total_new     = sum(d["n_new"]     for d in deltas)
    total_removed = sum(d["n_removed"] for d in deltas)
    net           = total_new - total_removed
    footer = (
        f"\n{'─' * 72}\n"
        f"  TOTAL   +{total_new:,} new   -{total_removed:,} removed   "
        f"net {net:+,}\n"
        f"{'━' * 72}"
    )
    return header + "\n\n" + ("\n\n" + "·" * 40 + "\n\n").join(rows) + footer


def format_history_table(history: list[dict]) -> str:
    """Growth series table for multiple days."""
    if not history:
        return "No historical data available."

    by_date: dict[str, list[dict]] = {}
    for h in history:
        by_date.setdefault(h["date"], []).append(h)

    lines = [
        f"\n{'━' * 82}",
        "    Historical Growth Series",
        f"{'━' * 82}",
        f"  {'Date':<12} {'Source':<22} {'Type':<10} "
        f"{'New':>8} {'Removed':>9} {'Net':>7}  Bar",
        f"{'─' * 82}",
    ]

    max_new = max((h.get("n_new", 0) for h in history), default=1)

    for d in sorted(by_date.keys()):
        for h in by_date[d]:
            bar = _bar(h.get("n_new", 0), max_new, width=15)
            lines.append(
                f"  {h['date']:<12} {h['source']:<22} {h['chat_type']:<10} "
                f"{h.get('n_new', 0):>+8,} {h.get('n_removed', 0):>9,} "
                f"{h.get('net_change', 0):>+7,}  {bar}"
            )

    lines.append(f"{'━' * 82}")
    return "\n".join(lines)


# Loader

def load_deltas_for_date(store: SnapshotStore, run_date: date) -> list[dict]:
    fpath = store.delta_dir / _delta_filename(run_date)
    if not fpath.exists():
        return []
    with open(fpath, encoding="utf-8") as f:
        return json.load(f)


def load_history(store: SnapshotStore, days: int) -> list[dict]:
    """Aggregates delta data for the last N days."""
    today  = date.today()
    result = []
    for offset in range(days, -1, -1):
        d      = today - timedelta(days=offset)
        deltas = load_deltas_for_date(store, d)
        agg: dict[tuple, dict] = {}
        for delta in deltas:
            key = (delta["date"], delta["source"], delta["chat_type"])
            if key not in agg:
                agg[key] = {
                    "date":       delta["date"],
                    "source":     delta["source"],
                    "chat_type":  delta["chat_type"],
                    "n_new":      0,
                    "n_removed":  0,
                    "net_change": 0,
                }
            agg[key]["n_new"]      += delta.get("n_new", 0)
            agg[key]["n_removed"]  += delta.get("n_removed", 0)
            agg[key]["net_change"] += delta.get("net_change", 0)
        result.extend(agg.values())
    return result


# CLI

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delta report generator")
    parser.add_argument("--date",     default=None, help="Report date (YYYY-MM-DD). Default: today")
    parser.add_argument("--history",  type=int, default=None,
                        help="Show historical series for the last N days")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--export",   default=None, help="Export results to a JSON file")
    args = parser.parse_args()

    store    = SnapshotStore(data_dir=args.data_dir)
    run_date = date.fromisoformat(args.date) if args.date else date.today()

    if args.history:
        history = load_history(store, args.history)
        print(format_history_table(history))
        if args.export:
            with open(args.export, "w", encoding="utf-8") as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            print(f"\n✓ History exported to {args.export}")
    else:
        deltas = load_deltas_for_date(store, run_date)
        print(format_daily_report(deltas, run_date))
        if args.export:
            with open(args.export, "w", encoding="utf-8") as f:
                json.dump(deltas, f, ensure_ascii=False, indent=2)
            print(f"\n✓ Delta exported to {args.export}")
