# scraping/__init__.py
"""
Seed scraping package for external Telegram directories.

Sub-packages:
  sources/    → per-platform scrapers (TGStat, TelegramChannels.me)
  storage/    → snapshot persistence and daily delta computation
  reports/    → terminal report formatting and JSON export

Daily entry point:
  python -m scraping.run_daily [--source] [--types] [--max-pages] ...
"""

from .sources.base         import ScrapeRecord
from .sources.tgstat       import TGStatScraper
from .sources.tgchannels   import TGChannelsScraper
from .sources.taxonomies   import (
    TGSTAT_CATEGORIES,
    TGCHANNELS_CATEGORIES,
    TGCHANNELS_LANGUAGES,
)
from .storage.snapshot_store import SnapshotStore

__all__ = [
    "ScrapeRecord",
    "TGStatScraper",
    "TGChannelsScraper",
    "TGSTAT_CATEGORIES",
    "TGCHANNELS_CATEGORIES",
    "TGCHANNELS_LANGUAGES",
    "SnapshotStore",
]
