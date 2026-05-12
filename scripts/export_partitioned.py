# scripts/export_partitioned.py
"""
Backfill the partitioned JSONL export from MongoDB.

Reads collected/analysed chats from MongoDB and writes their messages to the
same folder layout that the live pipeline produces:

    {output_dir}/{source}/{YYYY-MM-DD}.jsonl

The date used for partitioning is the chat's ``processed_at`` field (i.e. the
day the pipeline finished collecting that chat), matching what the live
JsonExporter writes.  Chats with no ``processed_at`` are bucketed under today.

Messages are written one chat at a time so memory usage stays flat regardless
of dataset size.

Usage:
    # Export analysed + collected chats to data/messages/  (default)
    python scripts/export_partitioned.py

    # Custom output directory
    python scripts/export_partitioned.py --output-dir /tmp/export

    # Include only a specific source
    python scripts/export_partitioned.py --source telegramchannels

    # Include only a specific status
    python scripts/export_partitioned.py --statuses collected

    # Dry-run: show counts without writing anything
    python scripts/export_partitioned.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import EXPORT_BASE_DIR
from modules.db_manager import DBManager
from modules.json_exporter import JsonExporter

logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s [%(levelname)s] %(message)s",
    handlers = [logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("export_partitioned")

_SEP = "─" * 56


def run(args: argparse.Namespace) -> None:
    db       = DBManager(args.config)
    exporter = JsonExporter(args.output_dir)
    today    = datetime.now(timezone.utc).date()

    # Build chat filter
    chat_filter: dict = {"status": {"$in": args.statuses}}
    if args.source:
        chat_filter["source"] = args.source

    n_chats = db._chats.count_documents(chat_filter)
    logger.info(f"Chats to export: {n_chats:,}  (statuses={args.statuses})")

    if args.dry_run:
        logger.info("Dry-run — no files will be written.")

    total_messages = 0
    total_files: dict[str, set[str]] = {}  # source -> set of date strings written

    chat_cursor = db._chats.find(
        chat_filter,
        {"_id": 1, "telegram_id": 1, "source": 1, "processed_at": 1},
        sort=[("source", 1), ("processed_at", 1)],
    )

    for i, chat in enumerate(chat_cursor, 1):
        chat_id    = chat.get("telegram_id")
        source     = chat.get("source") or "unknown"
        proc_at    = chat.get("processed_at")
        crawl_date = proc_at.date() if isinstance(proc_at, datetime) else today

        if chat_id is None:
            logger.debug(f"[{chat['_id']}] No telegram_id — skipping.")
            continue

        messages = list(
            db._messages.find(
                {"chat_id": chat_id},
                {"_id": 0},          # _id is already stored as the string field
            )
        )

        if not messages:
            logger.debug(f"[{chat['_id']}] No messages in collection — skipping.")
            continue

        if not args.dry_run:
            exporter.write_messages(
                messages   = messages,
                source     = source,
                crawl_date = crawl_date,
            )

        total_messages += len(messages)
        total_files.setdefault(source, set()).add(crawl_date.isoformat())

        if i % 100 == 0:
            logger.info(f"  Progress: {i:,}/{n_chats:,} chats  |  {total_messages:,} messages so far")

    # Summary
    print()
    print(_SEP)
    print("  Export complete" + ("  (dry-run)" if args.dry_run else ""))
    print(_SEP)
    print(f"  Chats processed  : {n_chats:,}")
    print(f"  Messages written : {total_messages:,}")
    print(f"  Output directory : {Path(args.output_dir).resolve()}")
    print()
    for source, dates in sorted(total_files.items()):
        print(f"  {source}/")
        for d in sorted(dates):
            print(f"    {d}.jsonl")
    print(_SEP)

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export MongoDB messages to partitioned JSONL files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="config/config.ini",
        help="Path to the INI configuration file.",
    )
    parser.add_argument(
        "--output-dir",
        default=EXPORT_BASE_DIR,
        help="Root output directory (mirrors live pipeline layout).",
    )
    parser.add_argument(
        "--statuses",
        nargs="+",
        default=["analysed", "collected"],
        choices=["analysed", "collected"],
        help="Chat statuses to include.",
    )
    parser.add_argument(
        "--source",
        default=None,
        choices=["telegramchannels", "tgstats", "snowball"],
        help="Restrict export to a single source.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count and log what would be exported without writing any files.",
    )
    run(parser.parse_args())
