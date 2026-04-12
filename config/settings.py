# config/settings.py
"""
Global pipeline configuration constants.

Edit values here to change pipeline behaviour without touching module code.
"""

from datetime import datetime, timezone

MONGO_DB_NAME       = "tg_crypto"
COLLECTION_CHATS    = "chats"
COLLECTION_MESSAGES = "messages"

# Chat queue status values
STATUS_PENDING            = "pending"
STATUS_RUNNING            = "running"             # locked by an active worker
STATUS_ANALYSED           = "analysed"            # collected + scored; threshold TBD
STATUS_COLLECTED          = "collected"           # reserved for post-threshold confirmation
STATUS_DISCARDED          = "discarded"           # failed crypto fraction threshold
STATUS_DISCARDED_LANGUAGE = "discarded_language"  # failed English majority check
STATUS_DISCARDED_TTL      = "discarded_ttl"       # chat uses disappearing messages
STATUS_ERROR              = "error"

# Fixed crawl window: March 2026 (last 30 days before the 2026-04-01 cutoff).
# Seed chats always use this window.
# Snowball chats use this window when it yields >= SNOWBALL_MIN_MESSAGES,
# otherwise fall back to the last SNOWBALL_FALLBACK_LIMIT messages (no date bound).
COLLECT_DATE_FROM = datetime(2026, 3, 1,  0,  0,  0, tzinfo=timezone.utc)
COLLECT_DATE_TO   = datetime(2026, 4, 1, 23, 59, 59, tzinfo=timezone.utc)

# If a snowball chat has fewer than this many messages in the 30-day window,
# fall back to collecting the last SNOWBALL_FALLBACK_LIMIT messages regardless
# of date — this handles inactive or recently created chats.
SNOWBALL_MIN_MESSAGES      = 10_000
SNOWBALL_FALLBACK_LIMIT    = 10_000

ITER_SLEEP_SEC  = 0.0001
CHAT_SLEEP_SEC  = 2

LANGUAGE_ENGLISH_THRESHOLD = 0.60
LANGUAGE_MIN_CHARS         = 20
LANGUAGE_USE_LANGID        = True

ROBERTA_MODEL_PATH   = "models/roberta-crypto"
ROBERTA_BATCH_SIZE   = 32
ROBERTA_THRESHOLD    = 0.5
ROBERTA_CRYPTO_LABEL = 1

SEED_SOURCES = {
    "tgstats":          "https://tgstat.com/ratings",
    "telegramchannels": "https://telegramchannels.me/ranking",
}
SCRAPER_TIMEOUT = 30
SCRAPER_RETRIES = 3

LOG_DIR   = "logs"
LOG_LEVEL = "INFO"