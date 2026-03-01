# config/settings.py
"""
Global pipeline configuration constants.

Edit values here to change pipeline behaviour without touching module code.
"""

# MongoDB
MONGO_DB_NAME       = "tg_crypto"
COLLECTION_CHATS    = "chats"       # queue + chat metadata
COLLECTION_MESSAGES = "messages"    # collected messages

# Chat queue status values
STATUS_PENDING            = "pending"
STATUS_RUNNING            = "running"           # locked by an active worker
STATUS_COLLECTED          = "collected"
STATUS_DISCARDED          = "discarded"         # failed RoBERTa filter
STATUS_DISCARDED_LANGUAGE = "discarded_language"  # failed English language filter
STATUS_ERROR              = "error"

# Telethon collection
# Number of messages to fetch for the initial RoBERTa triage pass
MESSAGES_FOR_CLASSIFICATION = 100
# Number of messages to fetch for full collection after approval (None = all)
MESSAGES_FULL_COLLECTION    = 5000
# Delay between iterated messages (seconds) — reduces flood risk
ITER_SLEEP_SEC  = 0.0001
# Delay between processed chats (seconds)
CHAT_SLEEP_SEC  = 2

# Language detection
# Minimum fraction of messages detected as English to accept a chat
LANGUAGE_ENGLISH_THRESHOLD = 0.60
# Minimum character count per message for reliable language detection
LANGUAGE_MIN_CHARS         = 20
# Whether to enable langid as secondary/fallback detector
LANGUAGE_USE_LANGID        = True

# RoBERTa classifier
ROBERTA_MODEL_PATH   = "models/roberta-crypto"  # local path or HF Hub model ID
ROBERTA_BATCH_SIZE   = 32
# Minimum confidence to classify a message as crypto
ROBERTA_THRESHOLD    = 0.7
# Label used by your fine-tuned model for the positive (crypto) class
ROBERTA_CRYPTO_LABEL = "crypto"

# Seed scraper
SEED_SOURCES = {
    "tgstats":        "https://tgstat.com/ratings",
    "telegramchannels": "https://telegramchannels.me/ranking",
}
SCRAPER_TIMEOUT = 30   # seconds
SCRAPER_RETRIES = 3

#  Logging 
LOG_DIR   = "logs"
LOG_LEVEL = "INFO"
