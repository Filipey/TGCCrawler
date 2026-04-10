# TGCC Pipeline

A snowball data collection pipeline for Telegram cryptocurrency channels and groups.

## Project Structure

```
tg_crypto_pipeline/
├── config/
│   ├── config.ini              # Telegram API + MongoDB credentials (gitignored)
│   ├── config.ini.example      # Template — copy and fill in your credentials
│   └── settings.py             # Pipeline behaviour constants (thresholds, limits, etc.)
│                               # Infrastructure config (paths, credentials) → config.ini
│
├── modules/
│   ├── db_manager.py           # MongoDB access layer — queue CRUD + message persistence
│   ├── telethon_collector.py   # Telethon collector — channels + public groups
│   ├── language_detector.py    # English majority check (langdetect + langid)
│   └── roberta_classifier.py   # RoBERTa inference — crypto vs. non-crypto
│
├── scraping/                   # Modular seed scraping package
│   ├── sources/
│   │   ├── base.py             # BaseScraper + ScrapeRecord dataclass
│   │   ├── taxonomies.py       # Category lists for TGStat and TGChannels.me
│   │   ├── tgstat.py           # TGStat trending groups/channels scraper
│   │   └── tgchannels.py       # TelegramChannels.me ranking scraper (live taxonomy)
│   ├── storage/
│   │   └── snapshot_store.py   # JSONL/CSV snapshots + daily delta computation
│   ├── reports/
│   │   └── delta_report.py     # Terminal report + JSON export for historical deltas
│   └── run_daily.py            # Daily scraping entry point (cron-friendly)
│
├── scripts/
│   ├── seed_loader.py          # Populate MongoDB from scraped snapshots or live scrape
│   ├── analyse.py              # Process pending chats: collect → filter → score → analysed
│   ├── threshold.py            # Inspect crypto_fraction CDF and apply keep/discard threshold
│   ├── status_report.py        # Dashboard: chat counts by status, error details, message stats
│   ├── retry_errors.py         # Reset error chats back to pending for retry
│   └── export.py               # Export collected chats and messages (JSONL / CSV / Parquet)
│
├── logs/                       # Log files (created at runtime)
├── sessions/                   # Telethon session files (created at runtime)
│
├── main.py                     # Pipeline orchestrator — MongoDB queue loop (long-running)
├── requirements.txt
├── docker-compose.yaml
└── Dockerfile
```

---

## Pipeline Flow

```
[scraping/run_daily.py]
        │  fetches live taxonomy from TelegramChannels.me
        │  scrapes TGStat + TelegramChannels.me (all categories × languages)
        │  saves JSONL + CSV snapshots, computes daily delta
        ▼
[scripts/seed_loader.py]
        │  inserts usernames into MongoDB with status=pending
        ▼
[scripts/analyse.py  OR  main.py]
        │
        ├─ 1. pop_next_pending()  (atomic, safe for concurrent workers)
        │
        ├─ 2. TelegramCollector.collect_messages()
        │       Seed chats (tgstats / telegramchannels):
        │         → fixed 30-day window [2025-03-01, 2025-04-01]
        │       Snowball chats:
        │         → 30-day window first; if < 10k messages, fall back to
        │           last 10k messages regardless of date
        │
        ├─ 3. TTL check
        │       any message with ttl_period set → disappearing messages
        │       → status = discarded_ttl   ──────────────────────────────┐
        │                                                                │
        ├─ 4. LanguageDetector.detect()                                  │
        │       checks English majority fraction across the full batch   │
        │       → NOT English: status = discarded_language   ────────────┤
        │                                                                │
        ├─ 5. RoBERTaCryptoClassifier.classify_batch()                   │
        │       classifies every message in the batch                    │
        │       computes crypto_fraction = n_crypto / n_english          │
        │                                                                │
        ├─ 6. DBManager.bulk_insert_messages()                           │
        │       persists all collected messages to MongoDB               │
        │                                                                │
        ├─ 7. status = analysed                                          │
        │       stores collection_stats (n_messages_total, n_english,    │
        │       n_crypto, crypto_fraction, collect_mode, date window)    │
        │                                                                │
        └─ 8. Snowball: discovered usernames → status = pending  ◄───────┘
                        (forwarded channel sources + @mentions + t.me/ links)
        │
        ▼
[scripts/threshold.py inspect]
        │  reads crypto_fraction from all 'analysed' chats
        │  prints CDF table + summary statistics
        │  exports CDF to CSV for external plotting
        ▼
[scripts/threshold.py apply --threshold X]
        │  chats with crypto_fraction >= X → status = collected
        │  chats with crypto_fraction <  X → status = discarded
        ▼
[scripts/export.py]
           exports collected chats + messages to JSONL / CSV / Parquet
```

---

## Scripts Reference

| Script | Purpose |
|---|---|
| `scripts/seed_loader.py` | Load seed usernames into MongoDB from disk snapshots or by scraping live |
| `scripts/analyse.py` | Process `pending` chats through the full pipeline; enqueue snowball discoveries |
| `scripts/threshold.py inspect` | Print the CDF of `crypto_fraction` across all `analysed` chats |
| `scripts/threshold.py apply` | Promote/discard `analysed` chats based on a crypto_fraction threshold |
| `scripts/status_report.py` | Snapshot of MongoDB state: counts by status/source, error details, message stats |
| `scripts/retry_errors.py` | Reset `error` chats back to `pending` (with optional error message filter) |
| `scripts/export.py` | Export `collected` chats and messages to JSONL, CSV, or Parquet |

### Common usage patterns

```bash
# Check pipeline state at any time
python scripts/status_report.py

# Process up to 200 pending chats
python scripts/analyse.py --limit 200

# Retry only FloodWait errors before the next run
python scripts/retry_errors.py --filter "FloodWait"

# Inspect crypto_fraction distribution (20 steps = every 5%)
python scripts/threshold.py inspect --steps 20 --export cdf.csv

# Simulate a threshold before applying
python scripts/threshold.py apply --threshold 0.30 --dry-run

# Apply threshold
python scripts/threshold.py apply --threshold 0.30

# Export final dataset to Parquet
python scripts/export.py --format parquet --output-dir ./dataset
```

---

## Snowball Policy

Seed chats (sourced from TGStat or TelegramChannels.me) are known-active channels
and are always collected using the fixed 30-day window.

Snowball chats are discovered dynamically during collection (from message forwards,
@mentions, and t.me/ links) and have no prior activity guarantee. They follow a
two-stage collection strategy:

1. **Window attempt** — collect messages in the 30-day window first.
2. **Fallback** — if fewer than `SNOWBALL_MIN_MESSAGES` (default: 10,000) messages
   are found, re-collect the last `SNOWBALL_FALLBACK_LIMIT` (default: 10,000)
   messages with no date bound.

The `collect_mode` field in `collection_stats` records which strategy was used
(`"window"` or `"fallback"`), so downstream analysis can distinguish between the two.

---

## Language Detection

Every chat goes through a language filter after collection. Only predominantly
English chats proceed to classification.

The implementation follows the **TeleScope** paper (Gangopadhyay et al., ICWSM 2025,
Section 4.1):

1. **Per-message detection** — each message is classified with **langdetect**
   (Naive Bayes + character n-grams, 55 languages). Messages shorter than
   `LANGUAGE_MIN_CHARS` characters are skipped as unreliable.
2. **Fallback detector** — **langid** is used when langdetect fails on short texts.
3. **Noise filtering** — URLs and @mentions are stripped before detection.
4. **Corpus-level detection** — the full concatenated batch (capped at 5,000 chars)
   is also classified independently and stored for reference.
5. **English fraction vote** — a chat is accepted when the fraction of messages
   detected as `"en"` meets or exceeds `LANGUAGE_ENGLISH_THRESHOLD` (default: 60%).

The result is stored in the `chats` collection under `lang_result`:

```json
{
  "is_english":        true,
  "dominant_language": "en",
  "english_fraction":  0.73,
  "corpus_language":   "en",
  "language_counts":   {"en": 73, "es": 12, "ru": 3}
}
```

---

## Message Metadata Schema

Aligned with the **TeleScope dataset** (Gangopadhyay et al., ICWSM 2025, §4.2–4.3).

### Identifiers

| Field | Type | Description |
|---|---|---|
| `_id` | str | `"{chat_id}_{message_id}"` — MongoDB primary key |
| `chat_id` | int | Numeric Telegram ID of the parent chat |
| `chat_username` | str\|None | Username of the parent chat |
| `message_id` | int | Message ID unique within the channel |

### Content

| Field | Type | Description |
|---|---|---|
| `text` | str | Full message text |
| `date` | datetime | UTC timestamp of the original post |
| `edit_date` | datetime\|None | Last edit timestamp |
| `is_pinned` | bool | Whether the message is pinned |

### Authorship

| Field | Type | Description |
|---|---|---|
| `author_id` | int\|None | User ID of the sender (groups) or None (channels) |
| `is_bot_author` | bool | Whether the sender is a bot |
| `is_verified_author` | bool | Whether the sender is a verified account |

### Threading

| Field | Type | Description |
|---|---|---|
| `reply_to_message_id` | int\|None | ID of the message being replied to |

### Forward Provenance

| Field | Type | Description |
|---|---|---|
| `is_forwarded` | bool | Forward flag |
| `forwarded_from_id` | int\|None | Numeric ID of the origin chat/user |
| `forwarded_from_type` | str\|None | `"channel"` \| `"user"` \| `"chat"` |
| `forwarded_from_name` | str\|None | Display name of the forward source |
| `forwarded_date` | datetime\|None | Original post datetime in the source channel |
| `forwarded_message_id` | int\|None | Message ID in the origin channel |

### Engagement Metrics (TeleScope §4.2 + §4.3)

| Field | Type | Description |
|---|---|---|
| `views` | int\|None | View count (channels only) — proxy for reach |
| `forwards_count` | int\|None | Times this message was forwarded from this channel |
| `reactions` | list | `[{"emoticon": str, "count": int, "is_chosen": bool}]` |

> **Note on aggregated interactions:** The Telegram API only provides per-channel
> engagement metrics. To compute network-wide aggregates (total views + reactions
> across all channels a message reached), the TeleScope paper reconstructs
> forwarding chains via backward-tracing. Our pipeline stores the raw per-channel
> values; cross-channel aggregation can be performed as a post-processing step on
> the `messages` + `chats` collections.

### Media

| Field | Type | Description |
|---|---|---|
| `has_media` | bool | Whether the message has a media attachment |
| `media_type` | str\|None | `photo` / `video` / `document` / `gif` / `sticker` / `voice_note` / `poll` / `webpage_preview` / `geo_location` / `contact` / `other` |

### Telegram Entities (TeleScope §4.2)

| Field | Type | Description |
|---|---|---|
| `entities` | list | `[{"type": str, "value": str, "offset": int, "length": int, "url": str\|None}]` |
| `hashtags` | list[str] | Extracted `#hashtag` strings |
| `outbound_links` | list[str] | URLs found in message text |
| `outbound_tg_usernames` | list[str] | `@mentions` and `t.me/` links (snowball input) |

Entity types: `bold`, `italic`, `underline`, `strikethrough`, `code`, `pre`,
`url`, `text_url`, `mention`, `mention_name`, `hashtag`, `cashtag`,
`bot_command`, `email`, `phone_number`, `bank_card`, `spoiler`, `blockquote`.

---

## Chat Metadata Schema

Stored in the `chats` MongoDB collection.

| Field | Type | Description |
|---|---|---|
| `_id` | str | Normalised username (no @) |
| `telegram_id` | int\|None | Numeric Telegram ID |
| `type` | str | `"channel"` \| `"group"` \| `"unknown"` |
| `title` | str\|None | Display title |
| `description` | str\|None | Channel 'about' text |
| `n_subscribers` | int\|None | Participant count |
| `is_scam` | bool | Reported for fraudulent activity |
| `is_fake` | bool | Impersonates a known person or service |
| `is_verified` | bool | Verified across 2+ social media platforms |
| `is_restricted` | bool | Restricted by Telegram (e.g. adult content) |
| `restriction_reason` | str\|None | Reason string when `is_restricted=True` |
| `creation_date` | datetime\|None | Channel creation timestamp |
| `status` | str | See **Chat Status Values** below |
| `source` | str | `tgstats` \| `telegramchannels` \| `snowball` \| `manual` |
| `lang_result` | dict\|None | Language detection summary |
| `collection_stats` | dict\|None | Per-chat collection metrics (see below) |
| `added_at` | datetime | When the chat was first enqueued |
| `processed_at` | datetime\|None | When the chat last changed status |
| `error_msg` | str\|None | Error message if `status = error` |

### collection_stats

Populated by `analyse.py` after a successful collection run.

| Field | Type | Description |
|---|---|---|
| `n_messages_total` | int | All messages collected in the window |
| `n_english` | int | Messages with non-empty text sent to the classifier |
| `n_crypto` | int | English messages classified as crypto-related |
| `crypto_fraction` | float | `n_crypto / n_english` — used for threshold analysis |
| `collect_mode` | str | `"window"` (30-day bound) or `"fallback"` (last N msgs) |
| `date_from` | datetime\|None | Window start (set only for `collect_mode = window`) |
| `date_to` | datetime\|None | Window end (set only for `collect_mode = window`) |

### Chat Status Values

| Status | Description |
|---|---|
| `pending` | Enqueued, not yet processed |
| `running` | Currently being processed by a worker |
| `analysed` | Collected and scored; threshold not yet applied |
| `collected` | Confirmed crypto chat (post-threshold) |
| `discarded` | Below crypto_fraction threshold |
| `discarded_language` | Failed English majority check |
| `discarded_ttl` | Chat uses disappearing messages (GDPR constraint) |
| `error` | Processing failed; see `error_msg` |

---

## Quick Start

```bash
pip install -r requirements.txt

# 1. Copy and fill in credentials
cp config/config.ini.example config/config.ini
# Edit: api_id, api_hash, phone, MongoDB URI, RoBERTa model_path, use_gpu

# 2. Scrape initial seeds (all categories, groups + channels)
python -m scraping.run_daily --source all --max-pages 3

# 3. Load seeds into MongoDB
python scripts/seed_loader.py --from-snapshot --source all

# 4. Analyse all pending chats
python scripts/analyse.py

# 5. Check pipeline state
python scripts/status_report.py

# 6. Retry any transient errors
python scripts/retry_errors.py --filter "FloodWait"
python scripts/analyse.py  # re-run to process the retried chats

# 7. Inspect the crypto_fraction distribution and decide on a threshold
python scripts/threshold.py inspect --steps 20 --export cdf.csv

# 8. Apply the threshold
python scripts/threshold.py apply --threshold 0.30

# 9. Export the final dataset
python scripts/export.py --format parquet --output-dir ./dataset
```

### Using mock classifier (no GPU required)

```bash
python scripts/analyse.py --mock-classifier
```

### Scheduled daily scraping (cron)

```bash
# Re-scrape seeds and enqueue new discoveries every day at 03:00
0 3 * * * cd /app && python -m scraping.run_daily >> logs/daily_scrape.log 2>&1

# Analyse any new pending chats (seeds + snowball) every day at 04:00
0 4 * * * cd /app && python scripts/analyse.py >> logs/analyse.log 2>&1
```

### Docker

```bash
docker compose up -d
```

---

## Configuration Reference

### `config/config.ini` — Infrastructure (machine-specific)

| Section | Key | Description |
|---|---|---|
| `[Telegram]` | `api_id` | Telegram API ID |
| `[Telegram]` | `api_hash` | Telegram API hash |
| `[Telegram]` | `phone` | Phone number for authentication |
| `[Telegram]` | `username` | Telegram username (used for session file name) |
| `[MongoDB]` | `uri` | MongoDB connection URI |
| `[RoBERTa]` | `model_path` | Local path or HuggingFace Hub model ID |
| `[RoBERTa]` | `use_gpu` | `True` to use CUDA, `False` to force CPU |

### `config/settings.py` — Pipeline behaviour

| Constant | Default | Description |
|---|---|---|
| `COLLECT_DATE_FROM` | `2025-03-01` | Start of the fixed collection window |
| `COLLECT_DATE_TO` | `2025-04-01` | End of the fixed collection window |
| `SNOWBALL_MIN_MESSAGES` | `10000` | Minimum messages in window before fallback triggers |
| `SNOWBALL_FALLBACK_LIMIT` | `10000` | Messages to collect when fallback triggers |
| `LANGUAGE_ENGLISH_THRESHOLD` | `0.60` | Min English fraction to accept a chat |
| `LANGUAGE_MIN_CHARS` | `20` | Min characters per message for detection |
| `LANGUAGE_USE_LANGID` | `True` | Enable langid as secondary detector |
| `ROBERTA_BATCH_SIZE` | `32` | Messages per RoBERTa inference batch |
| `ROBERTA_THRESHOLD` | `0.50` | Min confidence to classify a message as crypto |
| `ROBERTA_CRYPTO_LABEL` | `"1"` | Positive class label from your fine-tuned model |
| `ITER_SLEEP_SEC` | `0.0001` | Delay between iterated messages (flood protection) |
| `CHAT_SLEEP_SEC` | `2` | Delay between chats (flood protection) |

---

## References

- **Gangopadhyay, S., Dessì, D., Dimitrov, D., Dietze, S.** *TeleScope: A Longitudinal Dataset for Investigating Online Discourse and Information Interaction on Telegram.* ICWSM 2025. — Schema design, language detection methodology (§4.1), message metadata (§4.2), engagement metrics and channel-to-channel graph (§4.3).
- **La Morgia, M., Mei, A., Mongardini, A.** *TGDataset: Collecting and Exploring the Largest Telegram Channels Dataset.* KDD 2025. — Snowball methodology (§4.1).
- **Telethon Message constructor:** https://tl.telethon.dev/constructors/message.html
- **Telethon MessageReactions constructor:** https://tl.telethon.dev/constructors/messageReactions.html
- **langdetect:** https://pypi.org/project/langdetect/