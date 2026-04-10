# scripts/status_report.py
"""
Pipeline status dashboard.

Prints a snapshot of the current MongoDB state: chat counts by status
and source, message collection stats, and a list of chats in 'error'
with their error messages.

Usage:
    python scripts/status_report.py
    python scripts/status_report.py --errors-limit 50
    python scripts/status_report.py --no-errors
"""

from __future__ import annotations

import argparse
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
logger = logging.getLogger("status_report")

_SEP  = "─" * 62
_SEP2 = "━" * 62


def _pct(part: int, total: int) -> str:
    if total == 0:
        return "  —  "
    return f"{part / total * 100:5.1f}%"


def run(args: argparse.Namespace) -> None:
    db = DBManager(args.config)

    # 1. Chats by status
    status_counts = db.count_by_status()
    total_chats   = sum(status_counts.values())

    print()
    print(_SEP2)
    print("  PIPELINE STATUS REPORT")
    print(_SEP2)
    print(f"\n  Total chats in MongoDB : {total_chats:>10,}")
    print()
    print(f"  {'Status':<26}  {'Count':>8}  {'Share':>7}")
    print(_SEP)

    status_order = [
        "pending", "running", "analysed", "collected",
        "discarded", "discarded_language", "discarded_ttl", "error",
    ]
    for status in status_order:
        n = status_counts.get(status, 0)
        print(f"  {status:<26}  {n:>8,}  {_pct(n, total_chats):>7}")

    # Any statuses not in our expected list
    for status, n in sorted(status_counts.items()):
        if status not in status_order:
            print(f"  {status:<26}  {n:>8,}  {_pct(n, total_chats):>7}")

    print(_SEP)

    # 2. Chats by source
    source_pipeline = [{"$group": {"_id": "$source", "count": {"$sum": 1}}}]
    source_counts   = {
        r["_id"]: r["count"]
        for r in db._chats.aggregate(source_pipeline)
    }

    print(f"\n  {'Source':<26}  {'Count':>8}  {'Share':>7}")
    print(_SEP)
    for source, n in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"  {source:<26}  {n:>8,}  {_pct(n, total_chats):>7}")
    print(_SEP)

    # 3. Analysed chats — crypto_fraction summary
    analysed_pipeline = [
        {"$match": {
            "status": "analysed",
            "collection_stats.crypto_fraction": {"$exists": True},
        }},
        {"$group": {
            "_id":              None,
            "count":            {"$sum": 1},
            "avg_crypto_frac":  {"$avg": "$collection_stats.crypto_fraction"},
            "avg_n_english":    {"$avg": "$collection_stats.n_english"},
            "avg_n_total":      {"$avg": "$collection_stats.n_messages_total"},
            "total_messages":   {"$sum": "$collection_stats.n_messages_total"},
            "n_window":         {"$sum": {
                "$cond": [{"$eq": ["$collection_stats.collect_mode", "window"]}, 1, 0]
            }},
            "n_fallback":       {"$sum": {
                "$cond": [{"$eq": ["$collection_stats.collect_mode", "fallback"]}, 1, 0]
            }},
        }},
    ]
    analysed_res = list(db._chats.aggregate(analysed_pipeline))

    if analysed_res:
        a = analysed_res[0]
        print(f"\n  Analysed chats — collection stats")
        print(_SEP)
        print(f"  {'Total analysed':<30} {a['count']:>10,}")
        print(f"  {'Collect mode: window':<30} {a['n_window']:>10,}")
        print(f"  {'Collect mode: fallback':<30} {a['n_fallback']:>10,}")
        print(f"  {'Avg messages/chat':<30} {a['avg_n_total']:>10,.0f}")
        print(f"  {'Avg English msgs/chat':<30} {a['avg_n_english']:>10,.0f}")
        print(f"  {'Avg crypto_fraction':<30} {a['avg_crypto_frac']:>10.4f}")
        print(f"  {'Total messages collected':<30} {a['total_messages']:>10,}")
        print(_SEP)

    # 4. Messages collection
    total_messages = db._messages.count_documents({})
    print(f"\n  Messages collection")
    print(_SEP)
    print(f"  {'Total messages stored':<30} {total_messages:>10,}")
    print(_SEP)

    # 5. Error details
    n_errors = status_counts.get("error", 0)
    if n_errors > 0 and not args.no_errors:
        limit = args.errors_limit
        error_docs = list(db._chats.find(
            {"status": "error"},
            {"_id": 1, "source": 1, "error_msg": 1, "processed_at": 1},
            sort=[("processed_at", -1)],
            limit=limit,
        ))

        print(f"\n  Chats in 'error'  (showing {len(error_docs)} of {n_errors})")
        print(_SEP)
        print(f"  {'Username':<28}  {'Source':<16}  Error")
        print(_SEP)
        for doc in error_docs:
            username = doc["_id"][:27]
            source   = (doc.get("source") or "?")[:15]
            err      = (doc.get("error_msg") or "")[:60]
            print(f"  {username:<28}  {source:<16}  {err}")
        print(_SEP)

        if n_errors > limit:
            print(f"  ... and {n_errors - limit} more. Use --errors-limit to see more.")

    print()
    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline status dashboard.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="config/config.ini",
        help="Path to the INI configuration file.",
    )
    parser.add_argument(
        "--errors-limit",
        type=int,
        default=20,
        help="Maximum number of error chats to display.",
    )
    parser.add_argument(
        "--no-errors",
        action="store_true",
        help="Suppress the error details section.",
    )
    run(parser.parse_args())