# scripts/export.py
"""
Export collected chats and messages to research-ready file formats.

Exports the 'collected' chats and their messages from MongoDB into flat
files suitable for analysis. Three formats are supported:

  jsonl   — one JSON object per line (default, streamable, large datasets)
  csv     — flat tabular format (messages only; chats metadata separately)
  parquet — columnar binary format (requires pyarrow; best for pandas/spark)

What is exported:
  - chats.{ext}    — one row per collected chat (metadata + collection_stats)
  - messages.{ext} — one row per message belonging to a collected chat

Usage:
    # Export to JSONL in ./exports/
    python scripts/export.py

    # Export to CSV
    python scripts/export.py --format csv --output-dir ./exports

    # Export to Parquet
    python scripts/export.py --format parquet --output-dir ./exports

    # Export only chats (no messages — faster for inspection)
    python scripts/export.py --no-messages

    # Export a specific subset of statuses (e.g. include analysed too)
    python scripts/export.py --statuses collected analysed
"""

from __future__ import annotations

import argparse
import json
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
logger = logging.getLogger("export")


# Serialisation helpers

def _sanitise(obj):
    """Recursively convert MongoDB-specific types to JSON-serialisable ones."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _sanitise(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitise(v) for v in obj]
    return obj


def _flatten_message(doc: dict) -> dict:
    """
    Flattens nested lists in a message document to make it CSV-friendly.
    reactions, entities, hashtags, outbound_links, outbound_tg_usernames
    are serialised as JSON strings in CSV mode.
    """
    flat = dict(doc)
    for field in ("reactions", "entities", "hashtags",
                  "outbound_links", "outbound_tg_usernames"):
        if field in flat and isinstance(flat[field], list):
            flat[field] = json.dumps(flat[field], ensure_ascii=False)
    # Flatten datetime fields to ISO strings
    for field in ("date", "edit_date", "forwarded_date"):
        if isinstance(flat.get(field), datetime):
            flat[field] = flat[field].isoformat()
    return flat


def _flatten_chat(doc: dict) -> dict:
    """Flattens collection_stats and lang_result into top-level keys."""
    flat = dict(doc)
    # Promote collection_stats fields
    for k, v in (flat.pop("collection_stats", None) or {}).items():
        flat[f"stats_{k}"] = v.isoformat() if isinstance(v, datetime) else v
    # Promote lang_result fields
    for k, v in (flat.pop("lang_result", None) or {}).items():
        if isinstance(v, dict):
            flat[f"lang_{k}"] = json.dumps(v, ensure_ascii=False)
        else:
            flat[f"lang_{k}"] = v
    # Datetime fields
    for field in ("creation_date", "added_at", "processed_at"):
        if isinstance(flat.get(field), datetime):
            flat[field] = flat[field].isoformat()
    return flat


# Writers

def _write_jsonl(docs, path: Path, sanitise: bool = True) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(path, "w", encoding="utf-8") as f:
        for doc in docs:
            obj = _sanitise(doc) if sanitise else doc
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            count += 1
    return count


def _write_csv(rows, path: Path) -> int:
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    count  = 0
    writer = None
    with open(path, "w", newline="", encoding="utf-8") as f:
        for row in rows:
            if writer is None:
                writer = csv.DictWriter(f, fieldnames=list(row.keys()),
                                        extrasaction="ignore")
                writer.writeheader()
            writer.writerow(row)
            count += 1
    return count


def _write_parquet(rows, path: Path) -> int:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        logger.error(
            "pyarrow is required for Parquet export. "
            "Install it with: pip install pyarrow"
        )
        sys.exit(1)

    path.parent.mkdir(parents=True, exist_ok=True)
    records = list(rows)
    if not records:
        return 0
    table = pa.Table.from_pylist(records)
    pq.write_table(table, str(path))
    return len(records)


# Main

def run(args: argparse.Namespace) -> None:
    db         = DBManager(args.config)
    output_dir = Path(args.output_dir)
    fmt        = args.format

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Exporting to {output_dir}/  format={fmt}")

    # Chats
    chat_query = {"status": {"$in": args.statuses}}
    n_chats    = db._chats.count_documents(chat_query)
    logger.info(f"Chats to export ({', '.join(args.statuses)}): {n_chats:,}")

    chat_cursor = db._chats.find(chat_query)

    if fmt == "jsonl":
        chat_path = output_dir / "chats.jsonl"
        written   = _write_jsonl(chat_cursor, chat_path)
    elif fmt == "csv":
        chat_path = output_dir / "chats.csv"
        written   = _write_csv((_flatten_chat(d) for d in chat_cursor), chat_path)
    else:  # parquet
        chat_path = output_dir / "chats.parquet"
        written   = _write_parquet((_flatten_chat(d) for d in chat_cursor), chat_path)

    logger.info(f"Chats written : {written:,}  → {chat_path}")

    if args.no_messages:
        db.close()
        return

    # Messages
    # Collect the telegram_ids of exported chats to filter messages
    collected_telegram_ids = [
        doc["telegram_id"]
        for doc in db._chats.find(chat_query, {"telegram_id": 1})
        if doc.get("telegram_id") is not None
    ]

    if not collected_telegram_ids:
        logger.warning("No telegram_ids found — skipping message export.")
        db.close()
        return

    n_messages = db._messages.count_documents(
        {"chat_id": {"$in": collected_telegram_ids}}
    )
    logger.info(f"Messages to export: {n_messages:,}")

    msg_cursor = db._messages.find({"chat_id": {"$in": collected_telegram_ids}})

    if fmt == "jsonl":
        msg_path = output_dir / "messages.jsonl"
        written  = _write_jsonl(msg_cursor, msg_path)
    elif fmt == "csv":
        msg_path = output_dir / "messages.csv"
        written  = _write_csv((_flatten_message(_sanitise(d)) for d in msg_cursor),
                               msg_path)
    else:  # parquet
        msg_path = output_dir / "messages.parquet"
        written  = _write_parquet((_flatten_message(_sanitise(d)) for d in msg_cursor),
                                   msg_path)

    logger.info(f"Messages written: {written:,}  → {msg_path}")

    # Summary
    print()
    print("─" * 50)
    print("  Export complete")
    print("─" * 50)
    print(f"  Format    : {fmt}")
    print(f"  Directory : {output_dir.resolve()}")
    print(f"  Chats     : {output_dir / ('chats.' + ('jsonl' if fmt == 'jsonl' else fmt))}")
    if not args.no_messages:
        print(f"  Messages  : {output_dir / ('messages.' + ('jsonl' if fmt == 'jsonl' else fmt))}")
    print("─" * 50)

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export collected chats and messages to research-ready files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="config/config.ini",
        help="Path to the INI configuration file.",
    )
    parser.add_argument(
        "--format",
        choices=["jsonl", "csv", "parquet"],
        default="jsonl",
        help="Output file format.",
    )
    parser.add_argument(
        "--output-dir",
        default="exports",
        help="Directory where output files will be written.",
    )
    parser.add_argument(
        "--statuses",
        nargs="+",
        default=["collected"],
        choices=["collected", "analysed"],
        help="Chat statuses to include in the export.",
    )
    parser.add_argument(
        "--no-messages",
        action="store_true",
        help="Export only chat metadata, skip messages.",
    )
    run(parser.parse_args())