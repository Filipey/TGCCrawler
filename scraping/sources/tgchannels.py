# scraping/sources/tgchannels.py
"""
TelegramChannels.me scraper.

Collects public groups and channels by ranking across all combinations
of category x language x type available on the platform.

Base URL:
  https://telegramchannels.me/ranking?language=<lang>&category=<id>&type=<type>&page=N

Types:
  type=group    public groups
  type=channel  public channels

Categories:
  category=all  or  category=2..33  (see taxonomies.py)

Languages (language= parameter):
  en, ru, ar, es, pt, de, fr, it, tr, fa, hi, id, uk, uz, kk
"""

from __future__ import annotations

import logging
import re
import time
from typing import Optional

from .base import BaseScraper, ScrapeRecord, normalise_username
from .taxonomies import (TGCHANNELS_CATEGORIES, TGCHANNELS_LANGUAGES,
                         TGCHANNELS_TYPES, TGChannelsCategory)

logger = logging.getLogger(__name__)

# URL template

BASE_URL    = "https://telegramchannels.me"
RANKING_URL = BASE_URL + "/ranking?language={lang}&category={cat}&type={type}&page={page}"

# href patterns observed in the platform HTML
_GROUPS_RE   = re.compile(r"/groups/([\w]{5,32})/?",   re.IGNORECASE)
_CHANNELS_RE = re.compile(r"/channels/([\w]{5,32})/?", re.IGNORECASE)
_TG_LINK_RE  = re.compile(r"t(?:elegram)?\.me/([\w]{5,32})", re.IGNORECASE)

_NAV_BLACKLIST = frozenset({
    "groups", "channels", "bots", "ranking", "search",
    "category", "language", "page",
})


class TGChannelsScraper(BaseScraper):
    """
    Scrapes the ranking of public groups and channels from TelegramChannels.me.

    Args:
        max_pages:             Maximum pages per combination (default 5).
        languages:             Language codes to query. None = all TGCHANNELS_LANGUAGES.
        chat_types:            Types to collect ("group", "channel"). None = both.
        category_ids:          Category IDs to query. None = all categories.
        delay_between_combos:  Pause between (category x lang x type) combos (seconds).
    """

    SOURCE_NAME = "telegramchannels"

    def __init__(
        self,
        max_pages:             int              = 5,
        languages:             list[str] | None = None,
        chat_types:            list[str] | None = None,
        category_ids:          list[str] | None = None,
        delay_between_combos:  float            = 2.5,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.max_pages             = max_pages
        self.languages             = languages or TGCHANNELS_LANGUAGES
        self.chat_types            = chat_types or TGCHANNELS_TYPES
        self.category_ids          = category_ids   # None = all
        self.delay_between_combos  = delay_between_combos

    # ── Parsing ───────────────────────────────────────────────────────────────

    def _parse_records(
        self,
        html:      str,
        category:  TGChannelsCategory,
        chat_type: str,
        language:  str,
        page:      int,
    ) -> list[ScrapeRecord]:
        """Extracts ScrapeRecords from a TelegramChannels.me ranking page."""
        records: list[ScrapeRecord] = []
        pattern  = _GROUPS_RE if chat_type == "group" else _CHANNELS_RE

        try:
            from scrapling import Adaptor
            doc      = Adaptor(html, auto_match=False)
            path_seg = "groups" if chat_type == "group" else "channels"
            anchors  = doc.css(f"a[href*='/{path_seg}/']")

            for rank_offset, a in enumerate(anchors):
                href = a.attrib.get("href", "")
                m    = pattern.search(href)
                if not m:
                    continue
                username = normalise_username(m.group(1))
                if username and username not in _NAV_BLACKLIST:
                    records.append(ScrapeRecord(
                        username       = username,
                        source         = self.SOURCE_NAME,
                        chat_type      = chat_type,
                        category_slug  = category.category_id,
                        category_label = category.label,
                        language       = language,
                        rank           = (page - 1) * 20 + rank_offset + 1,
                    ))

            # Fallback: t.me direct links in case the URL path structure changes
            if not records:
                for rank_offset, a in enumerate(doc.css("a[href*='t.me/']")):
                    href     = a.attrib.get("href", "")
                    m        = _TG_LINK_RE.search(href)
                    if not m:
                        continue
                    username = normalise_username(m.group(1))
                    if username:
                        records.append(ScrapeRecord(
                            username       = username,
                            source         = self.SOURCE_NAME,
                            chat_type      = chat_type,
                            category_slug  = category.category_id,
                            category_label = category.label,
                            language       = language,
                            rank           = (page - 1) * 20 + rank_offset + 1,
                        ))

        except ImportError:
            for rank_offset, m in enumerate(pattern.finditer(html)):
                username = normalise_username(m.group(1))
                if username and username not in _NAV_BLACKLIST:
                    records.append(ScrapeRecord(
                        username       = username,
                        source         = self.SOURCE_NAME,
                        chat_type      = chat_type,
                        category_slug  = category.category_id,
                        category_label = category.label,
                        language       = language,
                        rank           = (page - 1) * 20 + rank_offset + 1,
                    ))

        # Deduplicate within the page
        seen:   set[str]           = set()
        unique: list[ScrapeRecord] = []
        for r in records:
            if r.username not in seen:
                seen.add(r.username)
                unique.append(r)
        return unique

    # scrape_page

    def scrape_page(self, url: str, **ctx) -> list[ScrapeRecord]:
        category  = ctx["category"]
        chat_type = ctx["chat_type"]
        language  = ctx["language"]
        page      = ctx.get("page", 1)

        html = self._fetch_html(url)
        if not html:
            return []

        records = self._parse_records(html, category, chat_type, language, page)
        logger.info(
            f"[tgchannels] {chat_type:7s} | cat={category.category_id:3s} "
            f"| lang={language:2s} | page={page} → {len(records)} records"
        )
        return records

    # scrape_combo

    def scrape_combo(
        self,
        category:  TGChannelsCategory,
        chat_type: str,
        language:  str,
    ) -> list[ScrapeRecord]:
        """Iterates pages for a single (category x type x language) combination."""
        all_records:    list[ScrapeRecord] = []
        seen_usernames: set[str]           = set()

        for page in range(1, self.max_pages + 1):
            url = RANKING_URL.format(
                lang=language,
                cat=category.category_id,
                type=chat_type,
                page=page,
            )
            page_records = self.scrape_page(
                url,
                category=category,
                chat_type=chat_type,
                language=language,
                page=page,
            )

            new = [r for r in page_records if r.username not in seen_usernames]
            if not new:
                break  # end of pagination or no new entries

            seen_usernames.update(r.username for r in new)
            all_records.extend(new)
            time.sleep(self.delay)

        return all_records

    # scrape_all

    def scrape_all(self, **kwargs) -> list[ScrapeRecord]:
        """
        Iterates all (category x type x language) combinations.

        A username may appear across multiple combos — that contextual
        information (category, language) is preserved in each record.
        """
        from .taxonomies import TGCHANNELS_BY_ID

        categories = (
            [TGCHANNELS_BY_ID[cid] for cid in self.category_ids
             if cid in TGCHANNELS_BY_ID]
            if self.category_ids
            else TGCHANNELS_CATEGORIES
        )

        combos = [
            (cat, ct, lang)
            for cat  in categories
            for ct   in self.chat_types
            for lang in self.languages
        ]
        total        = len(combos)
        all_records: list[ScrapeRecord] = []

        for idx, (category, chat_type, language) in enumerate(combos, 1):
            logger.info(
                f"[tgchannels] ── [{idx}/{total}] "
                f"cat={category.category_id!r} ({category.label}) "
                f"| type={chat_type!r} | lang={language!r}"
            )
            records = self.scrape_combo(category, chat_type, language)
            all_records.extend(records)
            time.sleep(self.delay_between_combos)

        logger.info(
            f"[tgchannels] Done. "
            f"Total raw records: {len(all_records)} across {total} combinations."
        )
        return all_records
