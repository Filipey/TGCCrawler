# scripts/threshold.py
"""
CDF analysis and threshold application for crypto_fraction.

Two modes:

  inspect   — reads crypto_fraction from all 'analysed' chats and prints
              the CDF distribution. Use this to visually identify the knee
              and decide on a threshold.

  apply     — given a threshold, promotes chats above it to 'collected'
              and marks the rest as 'discarded'. This is a one-way operation:
              run inspect first, then apply once you are confident.

Usage:
    # Print the CDF table (10 percentile steps)
    python scripts/threshold.py inspect

    # Print with finer granularity and export to CSV
    python scripts/threshold.py inspect --steps 100 --export cdf.csv

    # Apply threshold=0.30: chats with crypto_fraction >= 0.30 → collected
    python scripts/threshold.py apply --threshold 0.30

    # Dry-run: show what would be promoted/discarded without writing
    python scripts/threshold.py apply --threshold 0.30 --dry-run
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.db_manager import DBManager

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
    handlers = [logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("threshold")


# Helpers

def _fetch_analysed(db: DBManager) -> list[dict]:
    """
    Returns all 'analysed' chat documents that have a collection_stats field.
    Each dict contains at minimum: _id, source, collection_stats.
    """
    docs = list(db._chats.find(
        {
            "status":                        "analysed",
            "collection_stats.crypto_fraction": {"$exists": True},
        },
        {
            "_id":              1,
            "source":           1,
            "collection_stats": 1,
        },
    ))
    return docs


def _extract_fractions(docs: list[dict]) -> list[float]:
    return [
        d["collection_stats"]["crypto_fraction"]
        for d in docs
        if d.get("collection_stats", {}).get("crypto_fraction") is not None
    ]


# Inspect

def cmd_inspect(args: argparse.Namespace) -> None:
    db   = DBManager(args.config)
    docs = _fetch_analysed(db)
    db.close()

    if not docs:
        logger.info("No 'analysed' chats found in MongoDB.")
        return

    fractions = sorted(_extract_fractions(docs))
    n         = len(fractions)

    logger.info(f"Analysed chats with crypto_fraction: {n}")

    # Summary statistics
    mean   = sum(fractions) / n
    median = fractions[n // 2]
    p25    = fractions[int(n * 0.25)]
    p75    = fractions[int(n * 0.75)]
    p90    = fractions[int(n * 0.90)]
    p95    = fractions[int(n * 0.95)]

    print()
    print("─" * 50)
    print("  crypto_fraction — Summary Statistics")
    print("─" * 50)
    print(f"  n        : {n:>10,}")
    print(f"  min      : {fractions[0]:>10.4f}")
    print(f"  p25      : {p25:>10.4f}")
    print(f"  median   : {median:>10.4f}")
    print(f"  mean     : {mean:>10.4f}")
    print(f"  p75      : {p75:>10.4f}")
    print(f"  p90      : {p90:>10.4f}")
    print(f"  p95      : {p95:>10.4f}")
    print(f"  max      : {fractions[-1]:>10.4f}")
    print("─" * 50)

    # CDF table
    steps       = args.steps
    step_size   = 1.0 / steps
    rows        = []

    print()
    print(f"  {'Threshold':>10}  {'% chats kept':>13}  {'n kept':>8}  {'n discarded':>12}")
    print("─" * 50)

    for i in range(steps + 1):
        threshold   = round(i * step_size, 6)
        n_kept      = sum(1 for f in fractions if f >= threshold)
        pct_kept    = n_kept / n * 100
        n_discarded = n - n_kept
        rows.append({
            "threshold":   threshold,
            "n_kept":      n_kept,
            "n_discarded": n_discarded,
            "pct_kept":    round(pct_kept, 2),
        })
        print(f"  {threshold:>10.4f}  {pct_kept:>12.1f}%  {n_kept:>8,}  {n_discarded:>12,}")

    print("─" * 50)

    # Source breakdown
    sources: dict[str, list[float]] = {}
    for d in docs:
        src = d.get("source", "unknown")
        cf  = d.get("collection_stats", {}).get("crypto_fraction")
        if cf is not None:
            sources.setdefault(src, []).append(cf)

    if len(sources) > 1:
        print()
        print("  crypto_fraction by source:")
        print(f"  {'Source':<22}  {'n':>6}  {'mean':>8}  {'median':>8}")
        print("─" * 50)
        for src, vals in sorted(sources.items()):
            vals.sort()
            src_mean   = sum(vals) / len(vals)
            src_median = vals[len(vals) // 2]
            print(f"  {src:<22}  {len(vals):>6,}  {src_mean:>8.4f}  {src_median:>8.4f}")
        print("─" * 50)

    # Export
    if args.export:
        with open(args.export, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"CDF exported to {args.export}")


# Apply

def cmd_apply(args: argparse.Namespace) -> None:
    if args.threshold is None:
        logger.error("--threshold is required for the apply command.")
        sys.exit(1)

    threshold = args.threshold
    db        = DBManager(args.config)
    docs      = _fetch_analysed(db)

    if not docs:
        logger.info("No 'analysed' chats found.")
        db.close()
        return

    to_collect  = [d for d in docs
                   if d["collection_stats"]["crypto_fraction"] >= threshold]
    to_discard  = [d for d in docs
                   if d["collection_stats"]["crypto_fraction"] <  threshold]

    logger.info(f"Threshold: {threshold}")
    logger.info(f"  → collected  : {len(to_collect):,}")
    logger.info(f"  → discarded  : {len(to_discard):,}")

    if args.dry_run:
        logger.info("dry-run — no changes written to MongoDB.")
        # Show a preview of the boundary chats
        boundary = sorted(docs, key=lambda d: d["collection_stats"]["crypto_fraction"])
        print()
        print(f"  {'_id':<30}  {'source':<18}  {'crypto_fraction':>15}  {'decision':>10}")
        print("─" * 80)
        for d in boundary:
            cf       = d["collection_stats"]["crypto_fraction"]
            decision = "collected" if cf >= threshold else "discarded"
            print(f"  {d['_id']:<30}  {d.get('source','?'):<18}  {cf:>15.4f}  {decision:>10}")
        print("─" * 80)
        db.close()
        return

    # Confirm before writing
    print(
        f"\n  This will permanently update {len(docs):,} documents in MongoDB.\n"
        f"  collected={len(to_collect):,}  discarded={len(to_discard):,}\n"
    )
    confirm = input("  Type 'yes' to proceed: ").strip().lower()
    if confirm != "yes":
        logger.info("Aborted.")
        db.close()
        return

    # Promote to collected
    collected_ids = [d["_id"] for d in to_collect]
    if collected_ids:
        result = db._chats.update_many(
            {"_id": {"$in": collected_ids}},
            {"$set": {"status": "collected"}},
        )
        logger.info(f"Promoted to 'collected': {result.modified_count:,}")

    # Demote to discarded
    discarded_ids = [d["_id"] for d in to_discard]
    if discarded_ids:
        result = db._chats.update_many(
            {"_id": {"$in": discarded_ids}},
            {"$set": {"status": "discarded"}},
        )
        logger.info(f"Marked as 'discarded': {result.modified_count:,}")

    logger.info(f"[queue] {db.queue_stats()}")
    db.close()


# CLI

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="CDF analysis and threshold application for crypto_fraction.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="config/config.ini",
        help="Path to the INI configuration file.",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # inspect
    p_inspect = sub.add_parser(
        "inspect",
        help="Print the CDF of crypto_fraction across all analysed chats.",
    )
    p_inspect.add_argument(
        "--steps",
        type=int,
        default=20,
        help="Number of CDF steps to display (default: 20 = every 5%%).",
    )
    p_inspect.add_argument(
        "--export",
        default=None,
        metavar="FILE.csv",
        help="Export the CDF table to a CSV file.",
    )

    # apply
    p_apply = sub.add_parser(
        "apply",
        help="Apply a threshold: promote/discard all analysed chats.",
    )
    p_apply.add_argument(
        "--threshold",
        type=float,
        required=True,
        help="Minimum crypto_fraction to keep a chat (e.g. 0.30).",
    )
    p_apply.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without writing to MongoDB.",
    )

    args = parser.parse_args()

    if args.command == "inspect":
        cmd_inspect(args)
    elif args.command == "apply":
        cmd_apply(args)