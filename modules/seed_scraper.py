# modules/seed_scraper.py
"""
Scraper de seeds iniciais usando Scrapling.

Extracts usernames/links from crypto channels and groups from:
  - TGStat  (https://tgstat.com/en/channels?q=crypto&sort=members)
  - Telegramchannels.me (https://telegramchannels.me/channels?category=cryptocurrency)

Each source has its own method. Results are lists of usernames
normalised (no @, no t.me/) ready for insertion into MongoDB.

Uso standalone:
    python -m modules.seed_scraper --source tgstats --pages 5
"""

from __future__ import annotations

import logging
import re
import time
from typing import Iterator

logger = logging.getLogger(__name__)

#  Regex helpers 
_TG_USERNAME_RE = re.compile(
    r"(?:t(?:elegram)?\.me/|@)([\w]{5,32})",
    re.IGNORECASE,
)


def _extract_username(href: str) -> str | None:
    """Extrai username de um href ou texto contendo link do Telegram."""
    m = _TG_USERNAME_RE.search(href or "")
    if m:
        uname = m.group(1).lower()
        # Ignora paths genéricos do Telegram
        if uname not in {"joinchat", "share", "proxy", "addstickers", "addtheme"}:
            return uname
    return None


#  Base 

class BaseScraper:
    """Common interface for all seed scrapers."""

    SOURCE_NAME: str = "base"

    def __init__(self, timeout: int = 30, retries: int = 3, delay: float = 1.5):
        self.timeout = timeout
        self.retries = retries
        self.delay = delay
        self._fetcher = self._build_fetcher()

    def _build_fetcher(self):
        """Initialises the Scrapling fetcher with real browser headers."""
        try:
            from scrapling import Fetcher, StealthyFetcher

            # StealthyFetcher uses Playwright + stealth — best for anti-bot sites
            return StealthyFetcher(auto_match=False)
        except ImportError:
            logger.warning("Scrapling not installed, falling back to requests.")
            return None

    def _fetch_html(self, url: str) -> str | None:
        """Fetches HTML from the URL with retries."""
        if self._fetcher is None:
            return self._fetch_requests_fallback(url)

        for attempt in range(1, self.retries + 1):
            try:
                from scrapling import StealthyFetcher
                page = self._fetcher.fetch(url, timeout=self.timeout * 1000)
                return page.html_content
            except Exception as exc:
                logger.warning(f"[{self.SOURCE_NAME}] tentativa {attempt} falhou: {exc}")
                time.sleep(self.delay * attempt)
        return None

    def _fetch_requests_fallback(self, url: str) -> str | None:
        """Fallback using requests when Scrapling is unavailable."""
        import requests
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
        for attempt in range(1, self.retries + 1):
            try:
                resp = requests.get(url, headers=headers, timeout=self.timeout)
                resp.raise_for_status()
                return resp.text
            except Exception as exc:
                logger.warning(f"[{self.SOURCE_NAME}] requests fallback tentativa {attempt}: {exc}")
                time.sleep(self.delay * attempt)
        return None

    def scrape_page(self, url: str) -> list[str]:
        raise NotImplementedError

    def scrape_all(self, pages: int = 5) -> list[str]:
        raise NotImplementedError


#  TGStat Scraper 

class TGStatScraper(BaseScraper):
    """
    Extracts crypto channels/groups from TGStat.

    Base URL: https://tgstat.com/en/channels?q=crypto&sort=members&page=N
    Also iterates categories: bitcoin, blockchain, defi, nft, trading
    """

    SOURCE_NAME = "tgstats"
    BASE_URL = "https://tgstat.com/en/channels"
    CRYPTO_QUERIES = ["crypto", "bitcoin", "blockchain", "defi", "nft", "altcoin"]

    def scrape_page(self, url: str) -> list[str]:
        html = self._fetch_html(url)
        if not html:
            return []

        usernames: list[str] = []

        # Attempt structured parsing with Scrapling
        try:
            from scrapling import Adaptor
            page = Adaptor(html, auto_match=False)
            for a in page.css("a[href*='/channel/']"):
                href = a.attrib.get("href", "")
                parts = href.rstrip("/").split("/")
                if parts:
                    candidate = parts[-1].lstrip("@").lower()
                    if re.match(r"^\w{5,32}$", candidate):
                        usernames.append(candidate)
            for a in page.css("a[href*='t.me/']"):
                u = _extract_username(a.attrib.get("href", ""))
                if u:
                    usernames.append(u)
        except ImportError:
            usernames = re.findall(r"/channel/@?([\w]{5,32})", html)
            usernames += [u for u in _TG_USERNAME_RE.findall(html) if u]

        unique = list(dict.fromkeys(usernames))
        logger.info(f"[tgstats] {url} → {len(unique)} usernames")
        return unique

    def scrape_all(self, pages: int = 5) -> list[str]:
        all_usernames: list[str] = []
        seen: set[str] = set()

        for query in self.CRYPTO_QUERIES:
            for page_num in range(1, pages + 1):
                url = f"{self.BASE_URL}?q={query}&sort=members&page={page_num}"
                batch = self.scrape_page(url)
                new = [u for u in batch if u not in seen]
                seen.update(new)
                all_usernames.extend(new)
                logger.info(
                    f"[tgstats] query={query!r} page={page_num} "
                    f"novos={len(new)} total={len(all_usernames)}"
                )
                time.sleep(self.delay)
                if not batch:
                    break

        return all_usernames


#  TelegramChannels.me Scrapper 

class TelegramChannelsMeScraper(BaseScraper):
    """
    Extracts channels/groups from the cryptocurrency category on telegramchannels.me.

    URL base: https://telegramchannels.me/channels?category=cryptocurrency&sort=rating&page=N
    """

    SOURCE_NAME = "telegramchannels"
    BASE_URL = "https://telegramchannels.me"
    CRYPTO_CATEGORIES = [
        "cryptocurrency",
        "bitcoin",
        "blockchain",
        "trading",
        "defi",
    ]

    def scrape_page(self, url: str) -> list[str]:
        html = self._fetch_html(url)
        if not html:
            return []

        usernames: list[str] = []

        try:
            from scrapling import Adaptor
            page = Adaptor(html, auto_match=False)

            # telegramchannels.me uses cards with /channels/<username> links
            for a in page.css("a[href*='/channels/']"):
                href = a.attrib.get("href", "")
                parts = href.rstrip("/").split("/")
                candidate = parts[-1].lstrip("@").lower()
                if re.match(r"^\w{5,32}$", candidate) and candidate not in {
                    "channels", "groups", "bots", "category"
                }:
                    usernames.append(candidate)

            for a in page.css("a[href*='t.me/']"):
                u = _extract_username(a.attrib.get("href", ""))
                if u:
                    usernames.append(u)

        except ImportError:
            usernames = re.findall(r"/channels/([\w]{5,32})", html)
            usernames += [u for u in _TG_USERNAME_RE.findall(html) if u]

        unique = list(dict.fromkeys(usernames))
        logger.info(f"[telegramchannels] {url} → {len(unique)} usernames")
        return unique

    def scrape_all(self, pages: int = 5) -> list[str]:
        all_usernames: list[str] = []
        seen: set[str] = set()

        for category in self.CRYPTO_CATEGORIES:
            for page_num in range(1, pages + 1):
                url = (
                    f"{self.BASE_URL}/channels"
                    f"?category={category}&sort=rating&page={page_num}"
                )
                batch = self.scrape_page(url)
                new = [u for u in batch if u not in seen]
                seen.update(new)
                all_usernames.extend(new)
                logger.info(
                    f"[telegramchannels] category={category!r} page={page_num} "
                    f"novos={len(new)} total={len(all_usernames)}"
                )
                time.sleep(self.delay)
                if not batch:
                    break

        return all_usernames


#  Factory 

SCRAPERS: dict[str, type[BaseScraper]] = {
    "tgstats": TGStatScraper,
    "telegramchannels": TelegramChannelsMeScraper,
}


def get_scraper(source: str, **kwargs) -> BaseScraper:
    if source not in SCRAPERS:
        raise ValueError(f"Source desconhecido: {source!r}. Opções: {list(SCRAPERS)}")
    return SCRAPERS[source](**kwargs)


#  CLI standalone 
if __name__ == "__main__":
    import argparse
    import json

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Seed scraper CLI")
    parser.add_argument("--source", choices=list(SCRAPERS), required=True)
    parser.add_argument("--pages", type=int, default=3)
    parser.add_argument("--output", default=None, help="Save results as JSON to this file")
    args = parser.parse_args()

    scraper = get_scraper(args.source)
    results = scraper.scrape_all(pages=args.pages)

    print(f"\n✓ Total encontrado: {len(results)} usernames")
    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"✓ Salvo em {args.output}")
    else:
        for u in results[:20]:
            print(f"  @{u}")
        if len(results) > 20:
            print(f"  ... e mais {len(results)-20}")
