# main.py
"""
TG Crypto Pipeline — Main Orchestrator

Queue-driven loop that processes one Telegram chat at a time:

    1.  Pop the next 'pending' chat from MongoDB (atomic operation).
    2.  Collect N messages for triage via Telethon.
    3.  Language detection — verify the chat is predominantly English.
            → Non-English: mark as 'discarded_language' and skip.
    4.  RoBERTa classification — verify the chat is crypto-related.
            → Non-crypto:  mark as 'discarded' and skip.
    5.  Full collection of up to MESSAGES_FULL_COLLECTION messages.
    6.  Persist messages to MongoDB.
    7.  Mark chat as 'collected'.
    8.  Snowball: enqueue discovered usernames as new 'pending' entries.
    9.  Repeat until the queue is empty or SIGINT/SIGTERM is received.

Fault tolerance:
    - FloodWaitError:  respects the Telegram-mandated wait time and resumes.
    - Per-chat errors: marks status='error' and moves to the next chat.
    - SIGINT/SIGTERM:  graceful shutdown — finishes current chat before exiting.
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

from config.settings import (LANGUAGE_ENGLISH_THRESHOLD, LANGUAGE_MIN_CHARS,
                             LANGUAGE_USE_LANGID, MESSAGES_FOR_CLASSIFICATION,
                             MESSAGES_FULL_COLLECTION, ROBERTA_BATCH_SIZE,
                             ROBERTA_CRYPTO_LABEL, ROBERTA_MODEL_PATH,
                             ROBERTA_THRESHOLD, STATUS_PENDING)
from modules.db_manager import DBManager
from modules.language_detector import build_language_detector
from modules.roberta_classifier import build_classifier
from modules.telethon_collector import TelegramCollector

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")


def load_config(path: str = "config/config.ini") -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


# Orchestrator

class PipelineOrchestrator:
    """
    Coordinates the full collection pipeline for a MongoDB-backed queue.

    Components:
        - DBManager:          queue management + persistence
        - TelegramCollector:  Telethon-based message collection
        - LanguageDetector:   English majority check (step 3)
        - CryptoClassifier:   RoBERTa crypto relevance filter (step 4)
    """

    def __init__(
        self,
        config_file:      str  = "config/config.ini",
        mock_classifier:  bool = False,
        mock_lang:        bool = False,
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
        logger.warning(" Shutdown signal received. Finishing current task...")
        self._shutdown = True

    @staticmethod
    def _serialise_message(msg) -> dict:
        """
        Converts a CollectedMessage dataclass to a MongoDB-ready dict.
        Nested dataclasses (TelegramEntity, ReactionCount) are converted
        to plain dicts. Datetime fields are preserved as-is (pymongo handles them).
        """
        d = asdict(msg)
        # asdict() recursively converts nested dataclasses — no extra work needed.
        # Ensure datetime fields that might be non-datetime are set to None.
        for date_field in ("date", "edit_date", "forwarded_date"):
            if d.get(date_field) and not isinstance(d[date_field], datetime):
                d[date_field] = None
        return d

    async def _process_chat(self, chat_doc: dict) -> None:
        """
        Runs the full pipeline for a single chat document.

        Pipeline steps:
            1. Triage collection  (MESSAGES_FOR_CLASSIFICATION messages)
            2. Language detection (English majority check)
            3. RoBERTa classification (crypto relevance)
            4. Full collection    (MESSAGES_FULL_COLLECTION messages)
            5. Persistence + snowball enqueue
        """
        chat_key = chat_doc["_id"]
        logger.info(f"━━ Processing: '{chat_key}' (source={chat_doc.get('source')!r})")

        try:
            triage = await self.collector.collect_messages(
                username = chat_key,
                limit    = MESSAGES_FOR_CLASSIFICATION,
            )
            texts = [m.text for m in triage.messages if m.text.strip()]

            if not texts:
                logger.warning(f"[{chat_key}] No text content found — discarding.")
                self.db.mark_chat_discarded(chat_key, roberta_score=0.0)
                return

            lang_result = self.lang_detector.detect(triage.messages)

            lang_summary = {
                "is_english":        lang_result.is_english,
                "dominant_language": lang_result.dominant_language,
                "english_fraction":  round(lang_result.english_fraction, 4),
                "corpus_language":   lang_result.corpus_language,
                "language_counts":   lang_result.language_counts,
            }

            if not lang_result.is_english:
                logger.info(
                    f"[{chat_key}] Language check FAILED — "
                    f"dominant='{lang_result.dominant_language}' "
                    f"english_fraction={lang_result.english_fraction:.1%}"
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
                f"[{chat_key}] Language check PASSED "
                f"english_fraction={lang_result.english_fraction:.1%} "
                f"(dominant='{lang_result.dominant_language}')"
            )

            clf_result = self.classifier.classify_batch(texts)
            logger.info(
                f"[{chat_key}] RoBERTa is_crypto={clf_result.is_crypto} "
                f"score={clf_result.score:.3f} "
                f"({clf_result.n_crypto}/{clf_result.n_messages} msgs crypto)"
            )

            meta = triage.metadata
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

            if not clf_result.is_crypto:
                self.db.mark_chat_discarded(chat_key, roberta_score=clf_result.score)
                return

            if (
                MESSAGES_FULL_COLLECTION
                and MESSAGES_FULL_COLLECTION > MESSAGES_FOR_CLASSIFICATION
            ):
                logger.info(
                    f"[{chat_key}] Collecting full batch "
                    f"(up to {MESSAGES_FULL_COLLECTION} messages)…"
                )
                full_result      = await self.collector.collect_messages(
                    username = chat_key,
                    limit    = MESSAGES_FULL_COLLECTION,
                )
                all_messages     = full_result.messages
                snowball_targets = full_result.snowball_usernames
            else:
                all_messages     = triage.messages
                snowball_targets = triage.snowball_usernames

            msg_dicts = [self._serialise_message(m) for m in all_messages]
            inserted  = self.db.bulk_insert_messages(msg_dicts)
            logger.info(f"[{chat_key}] {inserted} messages stored in MongoDB")

            self.db.mark_chat_collected(
                chat_id_str   = chat_key,
                metadata      = chat_metadata,
                roberta_score = clf_result.score,
                lang_result   = lang_summary,
            )

            if snowball_targets:
                added = self.db.bulk_upsert_pending(
                    usernames = snowball_targets,
                    source    = "snowball",
                )
                logger.info(
                    f"[{chat_key}] Snowball: {len(snowball_targets)} candidates, "
                    f"{added} newly enqueued"
                )

        except ValueError as exc:
            self.db.mark_chat_error(chat_key, str(exc))

        except Exception as exc:
            logger.exception(f"[{chat_key}] Unexpected error: {exc}")
            self.db.mark_chat_error(chat_key, str(exc))

    # Main loop

    async def run(self) -> None:
        signal.signal(signal.SIGINT,  self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        async with self.collector:
            logger.info("✓ Pipeline started. Waiting for tasks in the queue…")
            empty_polls = 0

            while not self._shutdown:
                logger.info(f"[queue] {self.db.queue_stats()}")
                chat_doc = self.db.pop_next_pending()

                if chat_doc is None:
                    empty_polls += 1
                    wait = min(30 * empty_polls, 300)  # exponential backoff up to 5 min
                    logger.info(f"[queue] Empty. Waiting {wait}s… (poll #{empty_polls})")
                    await asyncio.sleep(wait)
                    continue

                empty_polls = 0
                try:
                    await self._process_chat(chat_doc)
                except Exception as exc:
                    # Safety net: loop must never crash
                    logger.exception(f"Critical error in _process_chat: {exc}")

            logger.info("✓ Graceful shutdown complete.")
            self.db.close()


# Entry point

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
