# scraping/sources/taxonomies.py
"""
Full category taxonomies for both scraping platforms.

TGStat
----------
Extracted from the platform HTML: /ratings/chats/<slug>/public?sort=mau
Covers all ~44 available topic categories.

TelegramChannels.me
--------------------
  ?category=    "all" or numeric IDs 2..33
  ?type=         "group" or "channel"
  ?language=     language code (en, ru, ar, es, pt, de, fr, ...)
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
    TGStatCategory("public",        "All categories"),
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
    category_id: str   # "all" or integer-string ("2".."33")
    label: str


TGCHANNELS_CATEGORIES: list[TGChannelsCategory] = [
    TGChannelsCategory("all", "All categories"),
    TGChannelsCategory("2",   "Technologies"),
    TGChannelsCategory("3",   "Cryptocurrencies"),
    TGChannelsCategory("4",   "Travel"),
    TGChannelsCategory("5",   "Politics"),
    TGChannelsCategory("6",   "Economics"),
    TGChannelsCategory("7",   "News"),
    TGChannelsCategory("8",   "Sport"),
    TGChannelsCategory("9",   "Entertainment"),
    TGChannelsCategory("10",  "Humor"),
    TGChannelsCategory("11",  "Education"),
    TGChannelsCategory("12",  "Music"),
    TGChannelsCategory("13",  "Art"),
    TGChannelsCategory("14",  "Blogs"),
    TGChannelsCategory("15",  "Books"),
    TGChannelsCategory("16",  "Business"),
    TGChannelsCategory("17",  "Food"),
    TGChannelsCategory("18",  "Health & Fitness"),
    TGChannelsCategory("19",  "Marketing"),
    TGChannelsCategory("20",  "Design"),
    TGChannelsCategory("21",  "Fashion & Beauty"),
    TGChannelsCategory("22",  "Medicine"),
    TGChannelsCategory("23",  "Psychology"),
    TGChannelsCategory("24",  "Religion"),
    TGChannelsCategory("25",  "Science"),
    TGChannelsCategory("26",  "Law"),
    TGChannelsCategory("27",  "Linguistics"),
    TGChannelsCategory("28",  "Family & Kids"),
    TGChannelsCategory("29",  "Animals & Nature"),
    TGChannelsCategory("30",  "Games"),
    TGChannelsCategory("31",  "Software & Apps"),
    TGChannelsCategory("32",  "Video & Films"),
    TGChannelsCategory("33",  "Other"),
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
