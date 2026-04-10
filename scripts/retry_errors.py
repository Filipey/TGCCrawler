# scripts/retry_errors.py
"""
Retry chats that are currently in 'error' status.

Errors in the pipeline are often transient (FloodWait, network timeout,
username temporarily unavailable). This script resets those chats back
to 'pending' so they will be picked up by the next analyze_chats.py run.

Optionally filters by error message substring so you can target specific
failure classes (e.g. only FloodWait errors, only "not found" errors).

Usage:
    # Inspect errors before doing anything
    python scripts/retry_errors.py --dry-run

    # Reset all errors back to pending
    python scripts/retry_errors.py

    # Reset only chats whose error message contains "FloodWait"
    python scripts/retry_errors.py --filter "FloodWait"

    # Reset only chats from a specific source
    python scripts/retry_errors.py --source snowball

    # Reset at most 50 chats
    python scripts/retry_errors.py --limit 50
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.db_manager import DBManager

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
    handlers = [logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("retry_errors")

_SEP = "─" * 70


def _build_query(args: argparse.Namespace) -> dict:
    query: dict = {"status": "error"}
    if args.source:
        query["source"] = args.source
    if args.filter:
        query["error_msg"] = {"$regex": args.filter, "$options": "i"}
    return query


def run(args: argparse.Namespace) -> None:
    db    = DBManager(args.config)
    query = _build_query(args)

    # Fetch candidates
    candidates = list(db._chats.find(
        query,
        {"_id": 1, "source": 1, "error_msg": 1, "processed_at": 1},
        sort=[("processed_at", -1)],
        limit=args.limit or 0,   # 0 = no limit in pymongo
    ))

    total_errors = db._chats.count_documents({"status": "error"})
    matched      = len(candidates)

    print()
    print(_SEP)
    print(f"  Total chats in 'error'  : {total_errors:,}")
    print(f"  Matched by filters      : {matched:,}")
    if args.source:
        print(f"  Source filter           : {args.source}")
    if args.filter:
        print(f"  Error message filter    : {args.filter!r}")
    if args.limit:
        print(f"  Limit                   : {args.limit}")
    print(_SEP)

    if not candidates:
        logger.info("No matching chats found.")
        db.close()
        return

    # Preview
    print(f"\n  {'Username':<28}  {'Source':<18}  {'Last error'}")
    print(_SEP)
    for doc in candidates[:30]:
        username = doc["_id"][:27]
        source   = (doc.get("source") or "?")[:17]
        err      = (doc.get("error_msg") or "")[:55]
        print(f"  {username:<28}  {source:<18}  {err}")
    if matched > 30:
        print(f"  ... and {matched - 30} more.")
    print(_SEP)

    if args.dry_run:
        logger.info("dry-run — no changes written.")
        db.close()
        return

    # Confirm
    print(f"\n  {matched:,} chats will be reset to 'pending'.")
    confirm = input("  Type 'yes' to proceed: ").strip().lower()
    if confirm != "yes":
        logger.info("Aborted.")
        db.close()
        return

    # Reset
    ids_to_reset = [doc["_id"] for doc in candidates]
    result = db._chats.update_many(
        {"_id": {"$in": ids_to_reset}},
        {"$set": {
            "status":       "pending",
            "error_msg":    None,
            "processed_at": datetime.now(timezone.utc),
        }},
    )

    logger.info(f"Reset {result.modified_count:,} chats to 'pending'.")
    logger.info(f"[queue] {db.queue_stats()}")
    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reset error chats back to pending for retry.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="config/config.ini",
        help="Path to the INI configuration file.",
    )
    parser.add_argument(
        "--filter",
        default=None,
        metavar="SUBSTRING",
        help="Only reset chats whose error_msg contains this string (case-insensitive).",
    )
    parser.add_argument(
        "--source",
        default=None,
        choices=["tgstats", "telegramchannels", "snowball", "manual"],
        help="Only reset chats from this source.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of chats to reset.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be reset without writing to MongoDB.",
    )
    run(parser.parse_args())