# TGCC Pipeline

A snowball data collection pipeline for Telegram cryptocurrency channels and groups.

## Project Structure

```
tg_crypto_pipeline/
├── config/
│   ├── config.ini              # Telegram API + MongoDB credentials (gitignored)
│   ├── config.ini.example      # Template — copy and fill in your credentials
│   └── settings.py             # All pipeline constants (thresholds, limits, etc.)
│
├── modules/
│   ├── db_manager.py           # MongoDB access layer — queue CRUD + message persistence
│   ├── telethon_collector.py   # Telethon collector — channels + public groups
│   ├── language_detector.py    # English majority check (langdetect + langid)
│   ├── roberta_classifier.py   # RoBERTa inference — crypto vs. non-crypto
│   └── seed_scraper.py         # Legacy single-file scraper (superseded by scraping/)
│
├── scraping/                   # Modular seed scraping package
│   ├── sources/
│   │   ├── base.py             # BaseScraper + ScrapeRecord dataclass
│   │   ├── taxonomies.py       # Full category lists for TGStat and TGChannels.me
│   │   ├── tgstat.py           # TGStat trending groups/channels scraper
│   │   └── tgchannels.py       # TelegramChannels.me ranking scraper
│   ├── storage/
│   │   └── snapshot_store.py   # JSONL/CSV snapshots + daily delta computation
│   ├── reports/
│   │   └── delta_report.py     # Terminal report + JSON export for historical deltas
│   └── run_daily.py            # Daily scraping entry point (cron-friendly)
│
├── scripts/
│   └── seed_loader.py          # CLI — populates MongoDB from scraped snapshots
│
├── logs/                       # Rotating log files (created at runtime)
├── sessions/                   # Telethon session files (created at runtime)
│
├── main.py                     # Pipeline orchestrator — MongoDB queue loop
├── requirements.txt
├── docker-compose.yaml
└── Dockerfile
```

---

## Pipeline Flow

```
[scraping/run_daily.py]
        │  scrapes TGStat + TelegramChannels.me (all categories × languages)
        │  saves JSONL + CSV snapshots, computes daily delta
        ▼
[scripts/seed_loader.py]
        │  inserts usernames into MongoDB with status=pending
        ▼
[main.py — queue loop]
        │
        ├─ 1. pop_next_pending()  (atomic, safe for concurrent workers)
        │
        ├─ 2. TelegramCollector.collect_messages()
        │       collects N triage messages with full metadata:
        │       text, date, edit_date, views, forwards_count,
        │       reactions, entities, reply_to, forward provenance, ...
        │
        ├─ 3. LanguageDetector.detect()
        │       checks English majority fraction across the message batch
        │       → NOT English: status = discarded_language  ────────────┐
        │                                                                │
        ├─ 4. RoBERTaCryptoClassifier.classify_batch()                  │
        │       checks crypto relevance via fine-tuned RoBERTa           │
        │       → NOT crypto: status = discarded  ───────────────────── ┤
        │                                                                │
        ├─ 5. Full collection (up to MESSAGES_FULL_COLLECTION msgs)     │
        │                                                                │
        ├─ 6. DBManager.bulk_insert_messages()                          │
        │       persists messages to MongoDB                             │
        │                                                                │
        ├─ 7. status = collected                                         │
        │                                                                │
        └─ 8. Snowball: discovered usernames → status = pending  ◄──────┘
                        (forwarded channel sources + @mentions + t.me/ links)
```

---

## Language Detection (Step 3)

After the triage collection, every chat goes through a language filter before
reaching the RoBERTa classifier. The goal is to ensure the collected dataset
is predominantly English-language content.

### Methodology

The implementation follows the same approach described in the **TeleScope**
paper (Gangopadhyay et al., ICWSM 2025, Section 4.1):

1. **Per-message detection** — each message text is passed individually to
   **langdetect** (Naive Bayes + character n-grams, 55 languages, 99.77%
   accuracy on news corpora). Messages shorter than `LANGUAGE_MIN_CHARS`
   characters are skipped as unreliable.

2. **Fallback detector** — when langdetect throws an exception or returns
   an ambiguous result on short texts, **langid** is used as a secondary
   detector.

3. **Noise filtering** — URLs (`https?://...`) and Telegram @mentions are
   stripped from each message before detection to avoid false signals.

4. **Corpus-level detection** — the full concatenation of all triage
   messages is also passed to the detector as a single string (capped at
   5,000 characters), providing a corpus-level language signal stored
   alongside the per-message result.

5. **English fraction vote** — a chat is accepted as English if the
   fraction of individual messages classified as `"en"` is at or above
   `LANGUAGE_ENGLISH_THRESHOLD` (default: 60%).

### Result storage

The full language result is stored in the `chats` MongoDB collection under
the `lang_result` field:

```json
{
  "is_english":        true,
  "dominant_language": "en",
  "english_fraction":  0.73,
  "corpus_language":   "en",
  "language_counts":   {"en": 73, "es": 12, "ru": 3}
}
```

Chats that fail the language check receive `status = "discarded_language"`
and are permanently skipped by the queue loop.

### Configuration

| Constant | Default | Description |
|---|---|---|
| `LANGUAGE_ENGLISH_THRESHOLD` | `0.60` | Min English fraction to accept a chat |
| `LANGUAGE_MIN_CHARS` | `20` | Min characters per message for reliable detection |
| `LANGUAGE_USE_LANGID` | `True` | Enable langid as fallback |

---

## Message Metadata Schema

Aligned with the **TeleScope dataset** (Gangopadhyay et al., ICWSM 2025,
Sections 4.2 and 4.3).

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
| `edit_date` | datetime\|None | Last edit timestamp (tracks post-publication edits) |
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
| `reply_to_message_id` | int\|None | ID of the message being replied to (thread reconstruction) |

### Forward Provenance

| Field | Type | Description |
|---|---|---|
| `is_forwarded` | bool | Forward flag |
| `forwarded_from_id` | int\|None | Numeric ID of the origin chat/user |
| `forwarded_from_type` | str\|None | `"channel"` \| `"user"` \| `"chat"` |
| `forwarded_from_name` | str\|None | Display name of the forward source (if available) |
| `forwarded_date` | datetime\|None | Original post datetime in the source channel |
| `forwarded_message_id` | int\|None | Message ID in the origin channel |

### Engagement Metrics (TeleScope §4.2 + §4.3)

| Field | Type | Description |
|---|---|---|
| `views` | int\|None | View count — channels only; proxy for reach |
| `forwards_count` | int\|None | Number of times this message was forwarded from this channel |
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
| `outbound_links` | list[str] | URLs found in the message text |
| `outbound_tg_usernames` | list[str] | `@mentions` and `t.me/` links (snowball input) |

Entity types include: `bold`, `italic`, `underline`, `strikethrough`, `code`,
`pre`, `url`, `text_url`, `mention`, `mention_name`, `hashtag`, `cashtag`,
`bot_command`, `email`, `phone_number`, `bank_card`, `spoiler`, `blockquote`.

---

## Channel Metadata Schema

Stored in the `chats` MongoDB collection.

| Field | Type | Description |
|---|---|---|
| `_id` | str | Normalised username (no @) |
| `telegram_id` | int\|None | Numeric Telegram ID |
| `type` | str | `"channel"` \| `"group"` \| `"unknown"` |
| `title` | str\|None | Channel/group display title |
| `description` | str\|None | Channel 'about' text |
| `n_subscribers` | int\|None | Participant count |
| `is_scam` | bool | Reported for fraudulent activity (TeleScope Table 4 and TGDataset) |
| `is_fake` | bool | Impersonates a known person or service |
| `is_verified` | bool | Verified across 2+ social media platforms |
| `is_restricted` | bool | Restricted by Telegram (e.g. adult, copyrighted) |
| `restriction_reason` | str\|None | Reason string when `is_restricted=True` |
| `creation_date` | datetime\|None | Channel creation timestamp |
| `status` | str | `pending` \| `running` \| `collected` \| `discarded` \| `discarded_language` \| `error` |
| `source` | str | `tgstats` \| `telegramchannels` \| `snowball` \| `manual` |
| `roberta_score` | float\|None | Mean crypto probability from classifier |
| `lang_result` | dict\|None | Language detection summary (see Language Detection) |

---

## Quick Start

```bash
pip install -r requirements.txt

# 1. Copy and fill in credentials
cp config/config.ini.example config/config.ini
# Edit: api_id, api_hash, phone, MongoDB URI, RoBERTa model path

# 2. Scrape initial seeds (all categories, groups + channels)
python -m scraping.run_daily --source all --max-pages 3

# 3. Load seeds into MongoDB
python scripts/seed_loader.py --from-snapshot --source all

# 4. Start the pipeline (use --mock-classifier for testing without a GPU)
python main.py --mock-classifier

# 5. Check today's delta report
python -m scraping.reports.delta_report

# 6. View the 30-day growth history
python -m scraping.reports.delta_report --history 30
```

### Scheduled daily scraping (cron)

```bash
# Run at 03:00 every day
0 3 * * * cd /app && python -m scraping.run_daily >> logs/daily_scrape.log 2>&1
```

### Docker

```bash
docker compose up -d
```

---

## Configuration Reference (`config/settings.py`)

| Constant | Default | Description |
|---|---|---|
| `MESSAGES_FOR_CLASSIFICATION` | `100` | Messages fetched for the triage pass |
| `MESSAGES_FULL_COLLECTION` | `5000` | Messages fetched after approval |
| `LANGUAGE_ENGLISH_THRESHOLD` | `0.60` | Min English fraction to accept a chat |
| `LANGUAGE_MIN_CHARS` | `20` | Min characters per message for detection |
| `LANGUAGE_USE_LANGID` | `True` | Enable langid as secondary detector |
| `ROBERTA_THRESHOLD` | `0.70` | Min crypto probability to accept a chat |
| `ROBERTA_CRYPTO_LABEL` | `"crypto"` | Positive class label from your model |
| `CHAT_SLEEP_SEC` | `2` | Pause between chats (flood protection) |

---

## References

- **Gangopadhyay, S., Dessì, D., Dimitrov, D., Dietze, S.** *TeleScope: A Longitudinal
  Dataset for Investigating Online Discourse and Information Interaction on Telegram.*
  ICWSM 2025. — Schema design, language detection methodology (§4.1), message metadata
  (§4.2), engagement metrics and channel-to-channel graph (§4.3).
- **La Morgia, M., Mei, Alessandro., Mongardini A.** *TGDataset: Collecting and Exploring the Largest Telegram Channels Dataset* KDD 2025. — Schema design, snowball methodology (§4.1).
- **Telethon Message constructor:** https://tl.telethon.dev/constructors/message.html
- **Telethon MessageReactions constructor:** https://tl.telethon.dev/constructors/messageReactions.html
- **langdetect:** https://pypi.org/project/langdetect/
