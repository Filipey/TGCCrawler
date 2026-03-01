# scripts/seed_loader.py
"""
CLIT to populate MongoDB with seeds from scrapers.


Read the JSONL snapshots most recent (or scrape now) and insert the usernames into MongoDB with status=pending.

Examples:
  # Scrape now and insert into MongoDB
  python scripts/seed_loader.py --source tgstats --max-pages 3

  # Load from the most recent snapshot already saved on disk (no scraping)
  python scripts/seed_loader.py --from-snapshot --source tgstats --type group

  # Add usernames manually (without scraping or snapshot)
  python scripts/seed_loader.py --manual cryptonews ethtrader

  # Dry-run
  python scripts/seed_loader.py --source all --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.db_manager import DBManager
from scraping import SnapshotStore, TGChannelsScraper, TGStatScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("seed_loader")


def _load_from_snapshot(store, source, chat_type):
    """Load usernames from the most recent snapshot on disk."""
    today = date.today()
    prev  = store.find_previous_date(source, chat_type, before=today) or today
    for d in (today, prev):
        records = store.load_snapshot(source, chat_type, d)
        if records:
            logger.info(f"[snapshot] {source}/{chat_type} @ {d.isoformat()} → {len(records)} registros")
            return [r["username"] for r in records]
    return []


def run(args):
    store = SnapshotStore(data_dir=args.data_dir)
    db    = DBManager() if not args.dry_run else None
    total_inserted = 0

    sources = ["tgstats", "tgchannels"] if args.source == "all" else [args.source]
    types   = args.types or ["group", "channel"]

    for source in [s for s in sources if s != "manual"]:
        for chat_type in types:
            if args.from_snapshot:
                usernames = _load_from_snapshot(store, source, chat_type)
            else:
                if source == "tgstats":
                    scraper = TGStatScraper(max_pages=args.max_pages, chat_types=[chat_type], delay=args.delay)
                else:
                    scraper = TGChannelsScraper(max_pages=args.max_pages, chat_types=[chat_type],
                                                languages=args.languages or None, delay=args.delay)
                logger.info(f"  Scrapping {source!r} / {chat_type!r}...")
                records   = scraper.scrape_all()
                usernames = [r.username for r in records]
                if not args.dry_run and records:
                    store.save_snapshot(records, source=source, chat_type=chat_type)
                    store.save_csv(records,      source=source, chat_type=chat_type)
                    delta = store.compute_delta(source=source, chat_type=chat_type)
                    store.save_delta(delta)

            logger.info(f"   {source}/{chat_type} → {len(usernames)} usernames")
            if not args.dry_run and db and usernames:
                inserted = db.bulk_upsert_pending(usernames, source=source)
                total_inserted += inserted
                logger.info(f"   New in database: {inserted}")
            elif args.dry_run:
                for u in usernames[:5]:
                    print(f"  (dry-run) @{u}")
                if len(usernames) > 5:
                    print(f"  ... e mais {len(usernames)-5}")

    if args.manual:
        logger.info(f"  Adding {len(args.manual)} usernames mannually")
        if not args.dry_run and db:
            inserted = db.bulk_upsert_pending(args.manual, source="manual")
            total_inserted += inserted

    print("\n" + "" * 50)
    if not args.dry_run and db:
        print(f"  New inserted : {total_inserted}")
        print(f"  Queue status  : {db.queue_stats()}")
        db.close()
    print(store.daily_summary())
    print("" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--source",    choices=["tgstats", "tgchannels", "all"], default="all")
    parser.add_argument("--types",     nargs="+", choices=["group", "channel"], default=["group", "channel"])
    parser.add_argument("--max-pages", type=int,   default=3)
    parser.add_argument("--languages", nargs="*",  default=None)
    parser.add_argument("--delay",     type=float, default=2.0)
    parser.add_argument("--manual",    nargs="*",  default=[])
    parser.add_argument("--from-snapshot", action="store_true")
    parser.add_argument("--data-dir",  default="data")
    parser.add_argument("--dry-run",   action="store_true")
    run(parser.parse_args())
