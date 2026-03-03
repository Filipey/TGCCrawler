# scraping/sources/tgchannels.py
"""
TelegramChannels.me scraper with live taxonomy discovery.

The platform's numeric category IDs are NOT stable — they have been observed
changing between days without notice. To work around this, the scraper always
fetches the live taxonomy from the ranking page's <select> element before
starting collection. The static list in taxonomies.py serves only as a fallback.

Base URL:
  https://telegramchannels.me/ranking?language=<lang>&category=<id>&type=<type>&page=N

Types:
  type=group    public groups
  type=channel  public channels

Categories: discovered dynamically from the page's <select name="category">
Languages:  stable codes (en, ru, ar, es, ...) — defined in taxonomies.py
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

# URL templates

BASE_URL     = "https://telegramchannels.me"
RANKING_URL  = BASE_URL + "/ranking"
RANKING_PAGE = RANKING_URL + "?language={lang}&category={cat}&type={type}&page={page}"

# Regex for the <select name="category"> option elements
# Matches: <option value="9">Cryptocurrencies</option>
_OPTION_RE = re.compile(
    r'<option\s+value=["\']([^"\']+)["\'][^>]*>\s*([^<]+?)\s*</option>',
    re.IGNORECASE,
)

# href patterns observed in the platform HTML
_GROUPS_RE   = re.compile(r"/groups/([\w]{5,32})/?",   re.IGNORECASE)
_CHANNELS_RE = re.compile(r"/channels/([\w]{5,32})/?", re.IGNORECASE)
_TG_LINK_RE  = re.compile(r"t(?:elegram)?\.me/([\w]{5,32})", re.IGNORECASE)

_NAV_BLACKLIST = frozenset({
    "groups", "channels", "bots", "ranking", "search",
    "category", "language", "page",
})


# Taxonomy fetcher

class TGChannelsTaxonomyFetcher:
    """
    Fetches the live category taxonomy from TelegramChannels.me.

    Parses the <select name="category"> element on the ranking page to
    extract the current (value -> label) mapping. This is the only reliable
    way to get correct IDs because the platform reassigns them without notice.

    Args:
        base_scraper: A BaseScraper instance used for the HTTP fetch.
        language:     Language code for the probe request (default "en").
    """

    _SELECT_RE = re.compile(
        r'<select[^>]+name=["\']category["\'][^>]*>(.*?)</select>',
        re.IGNORECASE | re.DOTALL,
    )

    def __init__(self, base_scraper: BaseScraper, language: str = "en"):
        self._scraper  = base_scraper
        self._language = language

    def fetch(self) -> list[TGChannelsCategory]:
        """
        Fetches and parses the live taxonomy.

        Returns a list of TGChannelsCategory objects reflecting the current
        platform state. Always includes the "all" sentinel.

        Falls back to TGCHANNELS_CATEGORIES on any error and emits a WARNING
        so operators know to investigate.
        """
        probe_url = (
            f"{RANKING_URL}?language={self._language}"
            f"&category=all&type=group&page=1"
        )
        html = self._scraper._fetch_html(probe_url)

        if not html:
            logger.warning(
                "[tgchannels taxonomy] Probe fetch failed — "
                "falling back to static taxonomy. Category IDs may be stale."
            )
            return list(TGCHANNELS_CATEGORIES)

        categories = self._parse_select(html)

        if not categories:
            logger.warning(
                "[tgchannels taxonomy] Could not parse <select name='category'> — "
                "falling back to static taxonomy. Category IDs may be stale."
            )
            return list(TGCHANNELS_CATEGORIES)

        # Ensure "all" is always present
        if not any(c.category_id == "all" for c in categories):
            categories.insert(0, TGChannelsCategory("all", "All Categories"))

        logger.info(
            f"[tgchannels taxonomy] Live taxonomy fetched: "
            f"{len(categories)} categories "
            f"(IDs: {', '.join(c.category_id for c in categories[:5])}...)"
        )
        return categories

    def _parse_select(self, html: str) -> list[TGChannelsCategory]:
        """
        Extracts <option value="...">Label</option> pairs from the
        category <select> block.

        Tries two strategies in order:
          1. Scrapling CSS selector (most accurate)
          2. Pure regex on the raw HTML (fallback)
        """
        # Strategy 1: Scrapling structural parsing
        try:
            from scrapling import Adaptor
            doc = Adaptor(html)

            select = (
                doc.css("select[name='category']") or
                doc.css("select[id='category']")   or
                doc.css("select[name*='category']")
            )

            if select:
                options = select[0].css("option")
                result  = []
                for opt in options:
                    value = (opt.attrib.get("value") or "").strip()
                    label = (opt.text or "").strip()
                    if value and label:
                        label = re.sub(r"\s+", " ", label)
                        result.append(TGChannelsCategory(value, label))
                if result:
                    return result

        except (ImportError, Exception) as exc:
            logger.debug(f"[tgchannels taxonomy] Scrapling parse failed: {exc}")

        # Strategy 2: Regex on the full HTML
        select_match = self._SELECT_RE.search(html)
        if not select_match:
            return self._parse_options_global(html)

        return self._parse_options_global(select_match.group(1))

    @staticmethod
    def _parse_options_global(html: str) -> list[TGChannelsCategory]:
        """Extracts all <option value=...> tags from an HTML fragment."""
        result = []
        for m in _OPTION_RE.finditer(html):
            value = m.group(1).strip()
            label = re.sub(r"\s+", " ", m.group(2)).strip()
            label = (label
                     .replace("&amp;", "&")
                     .replace("&lt;",  "<")
                     .replace("&gt;",  ">")
                     .replace("&#39;", "'")
                     .replace("&quot;", "\""))
            if value and label:
                result.append(TGChannelsCategory(value, label))
        return result


# Scraper

class TGChannelsScraper(BaseScraper):
    """
    Scrapes the ranking of public groups and channels from TelegramChannels.me.

    On every run, fetches the live category taxonomy first so that category IDs
    are always current — the platform reassigns numeric IDs without warning.

    Args:
        max_pages:             Maximum pages per combination (default 5).
        languages:             Language codes to query. None = all TGCHANNELS_LANGUAGES.
        chat_types:            Types to collect ("group", "channel"). None = both.
        category_ids:          Whitelist of category IDs (live values). None = all.
        delay_between_combos:  Pause between (category x lang x type) combos (seconds).
        taxonomy_language:     Language used for the taxonomy probe request.
    """

    SOURCE_NAME = "telegramchannels"

    def __init__(
        self,
        max_pages:             int              = 5,
        languages:             list[str] | None = None,
        chat_types:            list[str] | None = None,
        category_ids:          list[str] | None = None,
        delay_between_combos:  float            = 2.5,
        taxonomy_language:     str              = "en",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.max_pages            = max_pages
        self.languages            = languages or TGCHANNELS_LANGUAGES
        self.chat_types           = chat_types or TGCHANNELS_TYPES
        self.category_ids         = category_ids
        self.delay_between_combos = delay_between_combos
        self.taxonomy_language    = taxonomy_language

        self._taxonomy_fetcher = TGChannelsTaxonomyFetcher(
            base_scraper=self,
            language=taxonomy_language,
        )
        # Populated on first call to scrape_all() or fetch_taxonomy()
        self._live_categories: list[TGChannelsCategory] | None = None

    # Taxonomy

    def fetch_taxonomy(self) -> list[TGChannelsCategory]:
        """
        Fetches (or returns the cached) live taxonomy for this run.
        Re-fetches on a new scraper instance; cached within one run.
        """
        if self._live_categories is None:
            self._live_categories = self._taxonomy_fetcher.fetch()
        return self._live_categories

    def _resolve_categories(self) -> list[TGChannelsCategory]:
        """
        Returns the list of categories to scrape for this run.
        Filters by category_ids whitelist when provided.
        """
        live = self.fetch_taxonomy()

        if self.category_ids is None:
            return live

        live_by_id = {c.category_id: c for c in live}
        resolved   = [live_by_id[cid] for cid in self.category_ids if cid in live_by_id]

        missing = [cid for cid in self.category_ids if cid not in live_by_id]
        if missing:
            logger.warning(
                f"[tgchannels] Requested category IDs not found in live taxonomy: "
                f"{missing}. They may have been reassigned — run with "
                f"--dump-taxonomy to inspect current IDs."
            )

        return resolved

    # Parsing

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
            doc      = Adaptor(html)
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
            url = RANKING_PAGE.format(
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
        Fetches the live taxonomy, then iterates all combinations of
        (category x type x language).

        The taxonomy fetch adds one HTTP request per scraper instance but
        guarantees that category IDs are current regardless of platform changes.
        """
        categories = self._resolve_categories()

        combos = [
            (cat, ct, lang)
            for cat  in categories
            for ct   in self.chat_types
            for lang in self.languages
        ]
        total        = len(combos)
        all_records: list[ScrapeRecord] = []

        logger.info(
            f"[tgchannels] Starting — "
            f"{len(categories)} categories x {len(self.chat_types)} types x "
            f"{len(self.languages)} languages = {total} combinations"
        )

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


# CLI helpers

if __name__ == "__main__":
    import argparse
    import json
    import sys

    parser = argparse.ArgumentParser(description="TelegramChannels.me scraper CLI")
    parser.add_argument(
        "--dump-taxonomy",
        action="store_true",
        help="Fetch and print the live category taxonomy as JSON, then exit.",
    )
    parser.add_argument(
        "--language", default="en",
        help="Language for the taxonomy probe request (default: en)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    if args.dump_taxonomy:
        scraper = TGChannelsScraper(taxonomy_language=args.language)
        cats    = scraper.fetch_taxonomy()
        print(json.dumps(
            [{"id": c.category_id, "label": c.label} for c in cats],
            indent=2, ensure_ascii=False,
        ))
        sys.exit(0)