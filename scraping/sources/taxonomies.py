# scraping/sources/taxonomies.py
"""
Full category taxonomies for both scraping platforms.

TGStat
----------
Extracted from the platform HTML: /ratings/chats/<slug>/public?sort=mau
Covers all ~44 available topic categories.

TelegramChannels.me
--------------------
  ?category=    "all" or numeric IDs — WARNING: unstable, fetched live at runtime
  ?type=         "group" or "channel"
  ?language=     language code (en, ru, ar, es, pt, de, fr, ...)

  !! The numeric category IDs are NOT stable across days. The platform has been
  observed silently reassigning the same integer to a different category.
  TGChannelsScraper always fetches the live <select> taxonomy before scraping.
  The list below (TGCHANNELS_CATEGORIES) is used only as a fallback when that
  fetch fails entirely. A WARNING is emitted in that case.
  Last verified: 2025-07-01
"""

from __future__ import annotations

from dataclasses import dataclass, field

# TGStat

@dataclass(frozen=True)
class TGStatCategory:
    slug: str           # used in the URL path
    label: str          # human-readable name
    is_sensitive: bool = False  # e.g. darknet, erotica, adult — skipped by default


TGSTAT_CATEGORIES: list[TGStatCategory] = [
    TGStatCategory("blogs",         "Blogs"),
    TGStatCategory("news",          "News and media"),
    TGStatCategory("entertainment", "Humor and entertainment"),
    TGStatCategory("tech",          "Technologies"),
    TGStatCategory("economics",     "Economics"),
    TGStatCategory("business",      "Business and startups"),
    TGStatCategory("crypto",        "Cryptocurrencies"),
    TGStatCategory("travels",       "Travel"),
    TGStatCategory("marketing",     "Marketing, PR, advertising"),
    TGStatCategory("psychology",    "Psychology"),
    TGStatCategory("design",        "Design"),
    TGStatCategory("politics",      "Politics"),
    TGStatCategory("art",           "Art"),
    TGStatCategory("law",           "Law"),
    TGStatCategory("education",     "Education"),
    TGStatCategory("books",         "Books"),
    TGStatCategory("language",      "Linguistics"),
    TGStatCategory("career",        "Career"),
    TGStatCategory("edutainment",   "Edutainment"),
    TGStatCategory("courses",       "Courses and guides"),
    TGStatCategory("sport",         "Sport"),
    TGStatCategory("beauty",        "Fashion and beauty"),
    TGStatCategory("medicine",      "Medicine"),
    TGStatCategory("health",        "Health and Fitness"),
    TGStatCategory("pics",          "Pictures and photos"),
    TGStatCategory("apps",          "Software & Applications"),
    TGStatCategory("video",         "Video and films"),
    TGStatCategory("music",         "Music"),
    TGStatCategory("games",         "Games"),
    TGStatCategory("food",          "Food and cooking"),
    TGStatCategory("quotes",        "Quotes"),
    TGStatCategory("handmade",      "Handiwork"),
    TGStatCategory("babies",        "Family & Children"),
    TGStatCategory("nature",        "Nature"),
    TGStatCategory("construction",  "Interior and construction"),
    TGStatCategory("telegram",      "Telegram"),
    TGStatCategory("instagram",     "Instagram"),
    TGStatCategory("sales",         "Sales"),
    TGStatCategory("transport",     "Transport"),
    TGStatCategory("religion",      "Religion"),
    TGStatCategory("esoterics",     "Esoterics"),
    TGStatCategory("other",         "Other"),
    # Sensitive categories: collected only with --include-sensitive flag
    TGStatCategory("darknet",  "Darknet",       is_sensitive=True),
    TGStatCategory("gambling", "Bookmaking",    is_sensitive=True),
    TGStatCategory("shock",    "Shock content", is_sensitive=True),
    TGStatCategory("erotica",  "Erotic",        is_sensitive=True),
    TGStatCategory("adult",    "Adult",         is_sensitive=True),
]

TGSTAT_BY_SLUG: dict[str, TGStatCategory] = {c.slug: c for c in TGSTAT_CATEGORIES}


# TelegramChannels.me

@dataclass(frozen=True)
class TGChannelsCategory:
    category_id: str   # "all" or integer-string — UNSTABLE, reassigned without notice
    label: str


# Static fallback — used only when the live taxonomy fetch fails.
# To refresh: python -m scraping.sources.tgchannels --dump-taxonomy
TGCHANNELS_CATEGORIES: list[TGChannelsCategory] = [
    TGChannelsCategory("all", "All Categories"),
    TGChannelsCategory("1",   "Nature & Animals"),
    TGChannelsCategory("3",   "Art & Design"),
    TGChannelsCategory("4",   "Auto & Moto"),
    TGChannelsCategory("5",   "Blogs"),
    TGChannelsCategory("6",   "Books & Magazine"),
    TGChannelsCategory("7",   "Business & Startups"),
    TGChannelsCategory("8",   "Celebrities"),
    TGChannelsCategory("9",   "Cryptocurrencies"),
    TGChannelsCategory("10",  "Economics & Finance"),
    TGChannelsCategory("11",  "Education"),
    TGChannelsCategory("12",  "Entertainment"),
    TGChannelsCategory("13",  "Fashion & Beauty"),
    TGChannelsCategory("14",  "Food"),
    TGChannelsCategory("15",  "Games & Apps"),
    TGChannelsCategory("16",  "Health"),
    TGChannelsCategory("17",  "Languages"),
    TGChannelsCategory("18",  "Love"),
    TGChannelsCategory("20",  "Marketing"),
    TGChannelsCategory("21",  "Music"),
    TGChannelsCategory("23",  "News & Media"),
    TGChannelsCategory("24",  "Photo"),
    TGChannelsCategory("25",  "Science"),
    TGChannelsCategory("26",  "Self Development"),
    TGChannelsCategory("27",  "Sports & Fitness"),
    TGChannelsCategory("28",  "Technology"),
    TGChannelsCategory("29",  "Travel"),
    TGChannelsCategory("30",  "Movies & Videos"),
    TGChannelsCategory("31",  "Other"),
    TGChannelsCategory("32",  "Shop"),
    TGChannelsCategory("33",  "Betting"),
    TGChannelsCategory("34",  "Utilities & Tools"),
    TGChannelsCategory("35",  "Communication"),
    TGChannelsCategory("36",  "Telegram"),
    TGChannelsCategory("37",  "Political"),
    TGChannelsCategory("38",  "Stickers"),
    TGChannelsCategory("39",  "NSFW & Adults"),
    TGChannelsCategory("40",  "Telegram Miniapps & Games"),
    TGChannelsCategory("41",  "Crypto Airdrop"),
    TGChannelsCategory("42",  "Crypto & FX Trading"),
]

TGCHANNELS_BY_ID: dict[str, TGChannelsCategory] = {
    c.category_id: c for c in TGCHANNELS_CATEGORIES
}

# Languages supported by the platform filter
TGCHANNELS_LANGUAGES: list[str] = [
    "en", "ru", "ar", "es", "pt", "de", "fr", "it", "tr",
    "fa", "hi", "id", "uk", "uz", "kk",
]

# Supported chat types
TGCHANNELS_TYPES: list[str] = ["group", "channel"]