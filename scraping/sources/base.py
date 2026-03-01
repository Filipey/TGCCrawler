# scraping/sources/base.py
"""
Base scraper class and shared data structures.

Responsibilities:
  - HTML fetching with retries, exponential backoff, and Scrapling  (requests fallback)
  - Interface definition for scrape_page() / scrape_all()
  - ScrapeRecord dataclass (one unit of scraped data per listing entry)
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Shared regex patterns
_TG_USERNAME_RE = re.compile(
    r"(?:t(?:elegram)?\.me/|@)([\w]{5,32})",
    re.IGNORECASE,
)
_TG_JOINCHAT_RE = re.compile(
    r"t(?:elegram)?\.me/(?:joinchat|\+)([\w\-]{16,})",
    re.IGNORECASE,
)
_BLACKLIST = frozenset({
    "joinchat", "share", "proxy", "addstickers", "addtheme",
    "iv", "s", "c", "r",
})


def normalise_username(raw: str) -> Optional[str]:
    """
    Normalises a raw username or href to a clean lowercase string (no @).
    Returns None if the value is invalid or blacklisted.
    """
    cleaned = raw.strip().lstrip("@").lower()
    if re.match(r"^\w{5,32}$", cleaned) and cleaned not in _BLACKLIST:
        return cleaned
    return None


# Result dataclass

@dataclass
class ScrapeRecord:
    """
    A single entry scraped from an external directory listing.
    Represents one Telegram channel or group found on a ranking page.
    """
    username:       str                     # normalised Telegram username
    source:         str                     # "tgstats" | "telegramchannels"
    chat_type:      str                     # "group" | "channel" | "unknown"
    category_slug:  str                     # category slug or numeric ID
    category_label: str                     # human-readable category label
    language:       Optional[str] = None    # language filter used (TGChannels.me)
    rank:           Optional[int] = None    # position in the listing
    scraped_at:     datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> dict:
        return {
            "username":       self.username,
            "source":         self.source,
            "chat_type":      self.chat_type,
            "category_slug":  self.category_slug,
            "category_label": self.category_label,
            "language":       self.language,
            "rank":           self.rank,
            "scraped_at":     self.scraped_at.isoformat(),
        }


# Base class

class BaseScraper:
    """
    Shared fetch infrastructure for all seed scrapers.
    Subclasses must implement scrape_page() and scrape_all().
    """

    SOURCE_NAME: str = "base"

    def __init__(
        self,
        timeout:     int   = 30,
        retries:     int   = 3,
        delay:       float = 2.0,
        use_stealth: bool  = True,
    ):
        self.timeout     = timeout
        self.retries     = retries
        self.delay       = delay
        self.use_stealth = use_stealth
        self._fetcher    = self._build_fetcher() if use_stealth else None

    # Fetch layer

    def _build_fetcher(self):
        try:
            from scrapling.fetchers import StealthyFetcher
            return StealthyFetcher(auto_match=False)
        except ImportError:
            logger.warning(
                "Scrapling/Playwright not available — falling back to requests."
            )
            return None

    def _fetch_html(self, url: str) -> Optional[str]:
        """Fetches HTML with retries and exponential backoff."""
        if self._fetcher is not None:
            return self._fetch_scrapling(url)
        return self._fetch_requests(url)

    def _fetch_scrapling(self, url: str) -> Optional[str]:
        for attempt in range(1, self.retries + 1):
            try:
                page = self._fetcher.fetch(
                    url,
                    timeout=self.timeout * 1000,
                    wait_for_network_idle=True,
                )
                return page.html_content
            except Exception as exc:
                wait = self.delay * (2 ** (attempt - 1))
                logger.warning(
                    f"[{self.SOURCE_NAME}] Scrapling attempt {attempt}/{self.retries} "
                    f"failed ({exc}). Waiting {wait:.1f}s..."
                )
                time.sleep(wait)
        logger.error(f"[{self.SOURCE_NAME}] Scrapling exhausted retries for {url}")
        return None

    def _fetch_requests(self, url: str) -> Optional[str]:
        import requests
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        for attempt in range(1, self.retries + 1):
            try:
                resp = requests.get(url, headers=headers, timeout=self.timeout)
                resp.raise_for_status()
                return resp.text
            except Exception as exc:
                wait = self.delay * (2 ** (attempt - 1))
                logger.warning(
                    f"[{self.SOURCE_NAME}] requests attempt {attempt}/{self.retries} "
                    f"failed ({exc}). Waiting {wait:.1f}s..."
                )
                time.sleep(wait)
        logger.error(f"[{self.SOURCE_NAME}] requests exhausted retries for {url}")
        return None

    # Interface

    def scrape_page(self, url: str, **ctx) -> list[ScrapeRecord]:
        """Scrapes a single listing page. Returns a list of ScrapeRecords."""
        raise NotImplementedError

    def scrape_all(self, **kwargs) -> list[ScrapeRecord]:
        """Iterates all categories/pages and aggregates results."""
        raise NotImplementedError
