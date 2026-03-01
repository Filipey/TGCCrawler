# scraping/sources/tgstat.py
"""
TGStat scraper.

Collects Trending Public Groups and Trending Public Channels for
ALL available categories on the platform.

URLs:
  Groups    https://tgstat.com/ratings/chats/<slug>/public?sort=mau&page=N
  Channels  https://tgstat.com/ratings/channels/<slug>/public?sort=100&page=N

Pagination via &page=N (starts at 1; stops when the page returns no
new entries or the per-category page limit is reached).
"""

from __future__ import annotations

import logging
import re
import time
from typing import Optional

from .base import BaseScraper, ScrapeRecord, normalise_username
from .taxonomies import TGSTAT_CATEGORIES, TGStatCategory

logger = logging.getLogger(__name__)

# URL templates

BASE_URL         = "https://tgstat.com"
GROUP_URL_TMPL   = BASE_URL + "/ratings/chats/{slug}/public?sort=mau&page={page}"
CHANNEL_URL_TMPL = BASE_URL + "/ratings/channels/{slug}/public?sort=100&page={page}"

# CSS pattern observed in TGStat listing cards: <a href="/channel/@username">
_HREF_PATTERN = re.compile(r"/channel/@?([\w]{5,32})/?$", re.IGNORECASE)
_FALLBACK_RE  = re.compile(r"/channel/@?([\w]{5,32})", re.IGNORECASE)


class TGStatScraper(BaseScraper):
    """
    Scrapes trending public groups and channels from TGStat for all categories.

    Args:
        max_pages:          Maximum pages per category (default 5 ≈ 100 entries).
        include_sensitive:  If True, also scrapes sensitive categories (darknet, adult...).
        chat_types:         Types to collect: "group", "channel", or both.
        delay_between_cats: Pause between categories (seconds).
    """

    SOURCE_NAME = "tgstats"

    def __init__(
        self,
        max_pages:           int       = 5,
        include_sensitive:   bool      = False,
        chat_types:          list[str] = None,
        delay_between_cats:  float     = 3.0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.max_pages          = max_pages
        self.include_sensitive  = include_sensitive
        self.chat_types         = chat_types or ["group", "channel"]
        self.delay_between_cats = delay_between_cats

    # Parsing

    def _parse_records(
        self,
        html:      str,
        category:  TGStatCategory,
        chat_type: str,
        page:      int,
    ) -> list[ScrapeRecord]:
        """Extracts ScrapeRecords from a TGStat listing page HTML."""
        records: list[ScrapeRecord] = []

        try:
            from scrapling import Adaptor
            doc = Adaptor(html, auto_match=False)

            for rank_offset, a in enumerate(doc.css("a[href*='/channel/']")):
                href = a.attrib.get("href", "")
                m    = _HREF_PATTERN.search(href)
                if not m:
                    continue
                username = normalise_username(m.group(1))
                if username:
                    records.append(ScrapeRecord(
                        username       = username,
                        source         = self.SOURCE_NAME,
                        chat_type      = chat_type,
                        category_slug  = category.slug,
                        category_label = category.label,
                        rank           = (page - 1) * 20 + rank_offset + 1,
                    ))

        except ImportError:
            # Pure regex fallback when Scrapling is unavailable
            for rank_offset, match in enumerate(_FALLBACK_RE.finditer(html)):
                username = normalise_username(match.group(1))
                if username:
                    records.append(ScrapeRecord(
                        username       = username,
                        source         = self.SOURCE_NAME,
                        chat_type      = chat_type,
                        category_slug  = category.slug,
                        category_label = category.label,
                        rank           = (page - 1) * 20 + rank_offset + 1,
                    ))

        # Deduplicate within the page while preserving order
        seen:   set[str]           = set()
        unique: list[ScrapeRecord] = []
        for r in records:
            if r.username not in seen:
                seen.add(r.username)
                unique.append(r)
        return unique

    # scrape_page

    def scrape_page(self, url: str, **ctx) -> list[ScrapeRecord]:
        """
        Scrapes a single TGStat listing URL.

        Expected ctx keys:
            category  (TGStatCategory)
            chat_type (str)
            page      (int)
        """
        category  = ctx.get("category")
        chat_type = ctx.get("chat_type", "group")
        page      = ctx.get("page", 1)

        html = self._fetch_html(url)
        if not html:
            return []

        records = self._parse_records(html, category, chat_type, page)
        logger.info(
            f"[tgstat] {chat_type:7s} | {category.slug:20s} | "
            f"page={page} → {len(records)} records"
        )
        return records

    # scrape_category

    def scrape_category(
        self,
        category:  TGStatCategory,
        chat_type: str,
    ) -> list[ScrapeRecord]:
        """Iterates all pages for a (category × type) pair until exhausted or max_pages."""
        url_tmpl = GROUP_URL_TMPL if chat_type == "group" else CHANNEL_URL_TMPL

        # "public" is the "All categories" aggregate and uses a fixed URL pattern
        if category.slug == "public":
            if chat_type == "group":
                url_tmpl = BASE_URL + "/ratings/chats/public?sort=mau&page={page}"
            else:
                url_tmpl = BASE_URL + "/ratings/channels/public?sort=100&page={page}"

        all_records:    list[ScrapeRecord] = []
        seen_usernames: set[str]           = set()

        for page in range(1, self.max_pages + 1):
            url          = url_tmpl.format(slug=category.slug, page=page)
            page_records = self.scrape_page(
                url,
                category=category,
                chat_type=chat_type,
                page=page,
            )

            new = [r for r in page_records if r.username not in seen_usernames]
            if not new:
                logger.debug(
                    f"[tgstat] {category.slug}/{chat_type} "
                    f"page {page} — no new entries, stopping pagination."
                )
                break

            seen_usernames.update(r.username for r in new)
            all_records.extend(new)
            time.sleep(self.delay)

        return all_records

    # scrape_all

    def scrape_all(self, **kwargs) -> list[ScrapeRecord]:
        """
        Iterates ALL TGStat categories for the configured chat types.

        A username may appear in multiple categories (which is valid
        contextual information), but within a single category+type pair
        there are no duplicates.
        """
        categories = [
            c for c in TGSTAT_CATEGORIES
            if not c.is_sensitive or self.include_sensitive
        ]

        total = len(categories) * len(self.chat_types)
        done  = 0
        all_records: list[ScrapeRecord] = []

        for category in categories:
            for chat_type in self.chat_types:
                done += 1
                logger.info(
                    f"[tgstat] ── [{done}/{total}] "
                    f"category={category.slug!r}  type={chat_type!r}"
                )
                records = self.scrape_category(category, chat_type)
                all_records.extend(records)
                time.sleep(self.delay_between_cats)

        logger.info(
            f"[tgstat] Done. "
            f"Total raw records: {len(all_records)} across "
            f"{len(categories)} categories × {len(self.chat_types)} types."
        )
        return all_records
