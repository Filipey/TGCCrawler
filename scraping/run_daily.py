# scraping/run_daily.py
"""
Daily scraping runner.

Executes the full scraping cycle:
  1. Scrapes TGStat (groups + channels, all categories)
  2. Scrapes TelegramChannels.me (groups + channels, all categories x languages)
  3. Saves JSONL + CSV snapshots
  4. Computes daily deltas against the previous snapshot
  5. Prints a summary to stdout and logs

Usage:
  # Default: all sources, groups + channels
  python -m scraping.run_daily

  # TGStat only, with sensitive categories, 3 pages per category
  python -m scraping.run_daily --source tgstats --max-pages 3 --include-sensitive

  # TelegramChannels.me only, English and Portuguese
  python -m scraping.run_daily --source tgchannels --languages en pt

  # Groups only
  python -m scraping.run_daily --types group

  # Dry-run: scrape but do not write any files
  python -m scraping.run_daily --dry-run

Schedule via cron (every day at 03:00):
  0 3 * * * cd /app && python -m scraping.run_daily >> logs/daily_scrape.log 2>&1
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraping.sources.base import ScrapeRecord
from scraping.sources.tgchannels import TGChannelsScraper
from scraping.sources.tgstat import TGStatScraper
from scraping.storage.snapshot_store import SnapshotStore

Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/daily_scrape.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("run_daily")


# Runners

def run_tgstat(
    args:  argparse.Namespace,
    store: SnapshotStore,
    today: date,
) -> list[ScrapeRecord]:
    logger.info("═══ TGStat ═══")

    scraper = TGStatScraper(
        max_pages          = args.max_pages,
        include_sensitive  = args.include_sensitive,
        chat_types         = args.types,
        delay_between_cats = args.delay,
        delay              = args.delay,
        use_stealth        = not args.no_stealth,
    )
    records = scraper.scrape_all()

    if args.dry_run:
        logger.info(f"[dry-run] TGStat {len(records)} records (not saved)")
        return records

    for chat_type in args.types:
        subset = [r for r in records if r.chat_type == chat_type]
        if not subset:
            continue
        store.save_snapshot(subset, source="tgstats", chat_type=chat_type, run_date=today)
        store.save_csv(subset,      source="tgstats", chat_type=chat_type, run_date=today)
        delta = store.compute_delta(source="tgstats", chat_type=chat_type, today=today)
        store.save_delta(delta, run_date=today)

    return records


def run_tgchannels(
    args:  argparse.Namespace,
    store: SnapshotStore,
    today: date,
) -> list[ScrapeRecord]:
    logger.info("═══ TelegramChannels.me ═══")

    scraper = TGChannelsScraper(
        max_pages            = args.max_pages,
        languages            = args.languages or None,
        chat_types           = args.types,
        delay_between_combos = args.delay,
        delay                = args.delay,
        use_stealth          = not args.no_stealth,
    )
    records = scraper.scrape_all()

    if args.dry_run:
        logger.info(f"[dry-run] TGChannels {len(records)} records (not saved)")
        return records

    for chat_type in args.types:
        subset = [r for r in records if r.chat_type == chat_type]
        if not subset:
            continue
        store.save_snapshot(subset, source="telegramchannels", chat_type=chat_type, run_date=today)
        store.save_csv(subset,      source="telegramchannels", chat_type=chat_type, run_date=today)
        delta = store.compute_delta(source="telegramchannels", chat_type=chat_type, today=today)
        store.save_delta(delta, run_date=today)

    return records


def main(args: argparse.Namespace) -> None:
    today = date.today()
    store = SnapshotStore(data_dir=args.data_dir)

    all_records: list[ScrapeRecord] = []
    sources = (
        ["tgstats", "tgchannels"]
        if args.source == "all"
        else [args.source]
    )

    if "tgstats" in sources:
        all_records.extend(run_tgstat(args, store, today))

    if "tgchannels" in sources:
        all_records.extend(run_tgchannels(args, store, today))

    logger.info(f"✓ Scraping complete. Total raw records: {len(all_records)}")

    if not args.dry_run:
        summary = store.daily_summary(run_date=today)
        print("\n" + summary)
        logger.info("\n" + summary)


# CLI

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Daily seed scraper for Telegram directories",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--source",
        choices=["tgstats", "tgchannels", "all"],
        default="all",
        help="Source(s) to scrape",
    )
    parser.add_argument(
        "--types",
        nargs="+",
        choices=["group", "channel"],
        default=["group", "channel"],
        help="Chat types to collect",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum pages per category / combination",
    )
    parser.add_argument(
        "--languages",
        nargs="*",
        default=None,
        help=(
            "Languages for TelegramChannels.me (e.g. en pt es). "
            "Omit to use all languages in the taxonomy."
        ),
    )
    parser.add_argument(
        "--include-sensitive",
        action="store_true",
        help="Include sensitive TGStat categories (darknet, adult, etc.)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.5,
        help="Pause between requests (seconds)",
    )
    parser.add_argument(
        "--no-stealth",
        action="store_true",
        help="Use plain requests instead of Scrapling/Playwright",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Root directory for snapshots and deltas",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape but do not write any output files",
    )

    main(parser.parse_args())
