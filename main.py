# main.py
"""
TG Crypto Pipeline — Main Orchestrator

Queue-driven loop that processes one Telegram chat at a time.

Pipeline per chat:

    1.  Pop the next 'pending' chat from MongoDB (atomic operation).
    2.  Determine collection mode based on source:
            seed     (tgstats | telegramchannels):
                Collect all messages in the fixed 30-day window
                [COLLECT_DATE_FROM, COLLECT_DATE_TO].
            snowball:
                Try the 30-day window first.  If that yields < SNOWBALL_MIN_MESSAGES,
                re-collect the last SNOWBALL_FALLBACK_LIMIT messages regardless of date.
    3.  TTL check — if any message has ttl_period: mark 'discarded_ttl' and skip.
    4.  Language detection on the full message batch.
            → Not English: mark 'discarded_language' and skip.
    5.  RoBERTa classification on all messages (English chat → all msgs sent).
    6.  Compute crypto_fraction = n_crypto / n_english.
    7.  Persist all messages to MongoDB.
    8.  Mark chat as 'analysed' (intermediate status; threshold decided later
        from the CDF of crypto_fraction across all analysed chats).
    9.  Snowball: enqueue discovered usernames as new 'pending' entries.
    10. Repeat until queue empty or SIGINT/SIGTERM received.

Fault tolerance:
    - FloodWaitError:  respects the Telegram-mandated wait and resumes.
    - Per-chat errors: marks status='error' and continues.
    - SIGINT/SIGTERM:  graceful shutdown after finishing current chat.
"""

from __future__ import annotations

import asyncio
import configparser
import logging
import os
import signal
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional

from config.settings import (COLLECT_DATE_FROM, COLLECT_DATE_TO,
                             LANGUAGE_ENGLISH_THRESHOLD, LANGUAGE_MIN_CHARS,
                             LANGUAGE_USE_LANGID, ROBERTA_BATCH_SIZE,
                             ROBERTA_CRYPTO_LABEL, ROBERTA_MODEL_PATH,
                             ROBERTA_THRESHOLD, SNOWBALL_FALLBACK_LIMIT,
                             SNOWBALL_MIN_MESSAGES, STATUS_PENDING)
from modules.db_manager import DBManager
from modules.language_detector import build_language_detector
from modules.roberta_classifier import build_classifier
from modules.telethon_collector import TelegramCollector

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")

# Chat sources that come from curated seed lists (known-active chats).
# Everything else is treated as a snowball candidate with unknown activity.
_SEED_SOURCES = frozenset({"tgstats", "telegramchannels"})


def load_config(path: str = "config/config.ini") -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


class PipelineOrchestrator:
    """
    Coordinates the full collection pipeline for a MongoDB-backed queue.

    Components:
        - DBManager:          queue management + persistence
        - TelegramCollector:  Telethon-based message collection
        - LanguageDetector:   English majority check
        - CryptoClassifier:   RoBERTa per-message classification
    """

    def __init__(
        self,
        config_file:     str  = "config/config.ini",
        mock_classifier: bool = False,
    ):
        cfg = load_config(config_file)

        self.db        = DBManager(config_file)
        self.collector = TelegramCollector(config_file)

        self.lang_detector = build_language_detector(
            threshold  = LANGUAGE_ENGLISH_THRESHOLD,
            min_chars  = LANGUAGE_MIN_CHARS,
            use_langid = LANGUAGE_USE_LANGID,
        )

        model_path = cfg.get("RoBERTa", "model_path", fallback=ROBERTA_MODEL_PATH)
        use_gpu    = cfg.getboolean("RoBERTa", "use_gpu", fallback=False)

        self.classifier = build_classifier(
            model_path   = model_path,
            mock         = mock_classifier,
            threshold    = ROBERTA_THRESHOLD,
            batch_size   = ROBERTA_BATCH_SIZE,
            crypto_label = ROBERTA_CRYPTO_LABEL,
            use_gpu      = use_gpu,
        )

        self._shutdown = False

    def _handle_signal(self, signum, frame) -> None:
        logger.warning("Shutdown signal received. Finishing current task...")
        self._shutdown = True

    @staticmethod
    def _serialise_message(msg) -> dict:
        d = asdict(msg)
        for date_field in ("date", "edit_date", "forwarded_date"):
            if d.get(date_field) and not isinstance(d[date_field], datetime):
                d[date_field] = None
        return d

    async def _collect_for_chat(self, chat_key: str, source: str):
        """
        Runs the collection strategy appropriate for the chat source.

        Seed chats → fixed 30-day window, no fallback.
        Snowball chats → 30-day window first; if < SNOWBALL_MIN_MESSAGES,
                         re-collect last SNOWBALL_FALLBACK_LIMIT messages.

        Returns (CollectionResult, collect_mode) where collect_mode is
        "window" or "fallback".
        """
        is_seed = source in _SEED_SOURCES

        result = await self.collector.collect_messages(
            username  = chat_key,
            date_from = COLLECT_DATE_FROM,
            date_to   = COLLECT_DATE_TO,
        )

        # Propagate TTL immediately — no point checking message count
        if result.has_ttl:
            return result, "window"

        if is_seed or len(result.messages) >= SNOWBALL_MIN_MESSAGES:
            return result, "window"

        # Snowball fallback: not enough messages in the window
        logger.info(
            f"[{chat_key}] Window yielded only {len(result.messages)} messages "
            f"(< {SNOWBALL_MIN_MESSAGES}) — falling back to last "
            f"{SNOWBALL_FALLBACK_LIMIT} messages."
        )
        result = await self.collector.collect_messages(
            username = chat_key,
            limit    = SNOWBALL_FALLBACK_LIMIT,
        )
        return result, "fallback"

    async def _process_chat(self, chat_doc: dict) -> None:
        chat_key = chat_doc["_id"]
        source   = chat_doc.get("source", "unknown")
        logger.info(f"━━ Processing: '{chat_key}'  source={source!r}")

        try:
            # Step 1 & 2: Collect
            result, collect_mode = await self._collect_for_chat(chat_key, source)

            # Step 3: TTL check
            if result.has_ttl:
                logger.info(f"[{chat_key}] Discarding — disappearing messages (TTL).")
                self.db.mark_chat_discarded(chat_key, reason="ttl")
                return

            if not result.messages:
                logger.warning(f"[{chat_key}] No messages collected — marking error.")
                self.db.mark_chat_error(chat_key, "No messages collected in window.")
                return

            # Step 4: Language detection
            lang_result = self.lang_detector.detect(result.messages)
            lang_summary = {
                "is_english":        lang_result.is_english,
                "dominant_language": lang_result.dominant_language,
                "english_fraction":  round(lang_result.english_fraction, 4),
                "corpus_language":   lang_result.corpus_language,
                "language_counts":   lang_result.language_counts,
            }

            if not lang_result.is_english:
                logger.info(
                    f"[{chat_key}] Language FAILED — "
                    f"dominant='{lang_result.dominant_language}'  "
                    f"en_fraction={lang_result.english_fraction:.1%}"
                )
                self.db._chats.update_one(
                    {"_id": chat_key},
                    {"$set": {
                        "lang_result":  lang_summary,
                        "status":       "discarded_language",
                        "processed_at": datetime.now(timezone.utc),
                    }},
                )
                return

            logger.info(
                f"[{chat_key}] Language PASSED — "
                f"en_fraction={lang_result.english_fraction:.1%}"
            )

            # Step 5: RoBERTa on all messages
            texts      = [m.text for m in result.messages if m.text.strip()]
            clf_result = self.classifier.classify_batch(texts)

            # Step 6: Compute crypto_fraction
            n_messages_total = len(result.messages)
            n_english        = len(texts)          # messages with non-empty text
            n_crypto         = clf_result.n_crypto
            crypto_fraction  = n_crypto / n_english if n_english > 0 else 0.0

            logger.info(
                f"[{chat_key}] RoBERTa — "
                f"n_total={n_messages_total}  n_english={n_english}  "
                f"n_crypto={n_crypto}  crypto_fraction={crypto_fraction:.3f}"
            )

            # Step 7: Persist messages
            msg_dicts = [self._serialise_message(m) for m in result.messages]
            inserted  = self.db.bulk_insert_messages(msg_dicts)
            logger.info(f"[{chat_key}] {inserted}/{len(msg_dicts)} messages stored.")

            # Step 8: Mark as analysed
            meta = result.metadata
            chat_metadata = {
                "telegram_id":        meta.telegram_id,
                "type":               meta.chat_type,
                "title":              meta.title,
                "username":           meta.username,
                "description":        meta.description,
                "n_subscribers":      meta.n_subscribers,
                "is_scam":            meta.is_scam,
                "is_fake":            meta.is_fake,
                "is_verified":        meta.is_verified,
                "is_restricted":      meta.is_restricted,
                "restriction_reason": meta.restriction_reason,
                "creation_date":      meta.creation_date,
            }
            collection_stats = {
                "n_messages_total":  n_messages_total,
                "n_english":         n_english,
                "n_crypto":          n_crypto,
                "crypto_fraction":   round(crypto_fraction, 6),
                "collect_mode":      collect_mode,
                "date_from":         COLLECT_DATE_FROM if collect_mode == "window" else None,
                "date_to":           COLLECT_DATE_TO   if collect_mode == "window" else None,
            }

            self.db.mark_chat_analysed(
                chat_id_str      = chat_key,
                metadata         = chat_metadata,
                lang_result      = lang_summary,
                collection_stats = collection_stats,
            )

            # Step 9: Snowball
            if result.snowball_usernames:
                added = self.db.bulk_upsert_pending(
                    usernames = result.snowball_usernames,
                    source    = "snowball",
                )
                logger.info(
                    f"[{chat_key}] Snowball: "
                    f"{len(result.snowball_usernames)} candidates, {added} new."
                )

        except ValueError as exc:
            self.db.mark_chat_error(chat_key, str(exc))

        except Exception as exc:
            logger.exception(f"[{chat_key}] Unexpected error: {exc}")
            self.db.mark_chat_error(chat_key, str(exc))

    async def run(self) -> None:
        signal.signal(signal.SIGINT,  self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        async with self.collector:
            logger.info("✓ Pipeline started.")
            empty_polls = 0

            while not self._shutdown:
                logger.info(f"[queue] {self.db.queue_stats()}")
                chat_doc = self.db.pop_next_pending()

                if chat_doc is None:
                    empty_polls += 1
                    wait = min(30 * empty_polls, 300)
                    logger.info(f"[queue] Empty. Waiting {wait}s… (poll #{empty_polls})")
                    await asyncio.sleep(wait)
                    continue

                empty_polls = 0
                try:
                    await self._process_chat(chat_doc)
                except Exception as exc:
                    logger.exception(f"Critical error in _process_chat: {exc}")

            logger.info("✓ Graceful shutdown complete.")
            self.db.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="TG Crypto Pipeline Orchestrator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mock-classifier",
        action="store_true",
        help="Use keyword-based mock classifier (no model loading required)",
    )
    parser.add_argument(
        "--config",
        default="config/config.ini",
        help="Path to the INI configuration file",
    )
    args = parser.parse_args()

    orchestrator = PipelineOrchestrator(
        config_file     = args.config,
        mock_classifier = args.mock_classifier,
    )
    asyncio.run(orchestrator.run())