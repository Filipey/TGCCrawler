# scripts/analyse.py
"""
Standalone analysis runner.

Processes chats with status='pending' from MongoDB one at a time:
applies the full pipeline (TTL check → language detection → RoBERTa →
crypto_fraction) and marks each chat as 'analysed'. Newly discovered
usernames from the snowball (forwards + @mentions) are enqueued as
new 'pending' entries automatically.

This script is equivalent to the main.py loop but designed to be run
on-demand or via cron, with explicit control over how many chats to
process per invocation.

Usage:
    # Process all pending chats (runs until queue is empty)
    python scripts/analyse.py

    # Process at most 100 chats and stop
    python scripts/analyse.py --limit 100

    # Use mock classifier (no GPU/model needed — for testing)
    python scripts/analyse.py --mock-classifier

    # Custom config file
    python scripts/analyse.py --config config/config.ini

    # Dry-run: show what would be processed without writing anything
    python scripts/analyse.py --dry-run --limit 10
"""

from __future__ import annotations

import argparse
import asyncio
import configparser
import logging
import os
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

# Allow running from the project root or from /scripts
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (COLLECT_DATE_FROM, COLLECT_DATE_TO,
                             LANGUAGE_ENGLISH_THRESHOLD, LANGUAGE_MIN_CHARS,
                             LANGUAGE_USE_LANGID, ROBERTA_BATCH_SIZE,
                             ROBERTA_CRYPTO_LABEL, ROBERTA_THRESHOLD,
                             SNOWBALL_FALLBACK_LIMIT, SNOWBALL_MIN_MESSAGES,
                             STATUS_ANALYSED, STATUS_PENDING)
from modules.db_manager import DBManager
from modules.language_detector import build_language_detector
from modules.roberta_classifier import build_classifier
from modules.telethon_collector import TelegramCollector

# Logging

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/analyse.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("analyse")

# Sources that come from curated seed lists (known-active chats).
# Everything else is a snowball candidate and uses the fallback policy.
_SEED_SOURCES = frozenset({"tgstats", "telegramchannels"})


# Core logic

def _load_config(path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


def _serialise_message(msg) -> dict:
    d = asdict(msg)
    for date_field in ("date", "edit_date", "forwarded_date"):
        if d.get(date_field) and not isinstance(d[date_field], datetime):
            d[date_field] = None
    return d


async def _collect_for_chat(collector: TelegramCollector, chat_key: str, source: str):
    """
    Runs the collection strategy appropriate for the chat source.

    Seed chats  → fixed 30-day window [COLLECT_DATE_FROM, COLLECT_DATE_TO].
    Snowball    → 30-day window first; if < SNOWBALL_MIN_MESSAGES messages are
                  returned, re-collect the last SNOWBALL_FALLBACK_LIMIT messages
                  (no date bound) to handle low-activity or new chats.

    Returns (CollectionResult, collect_mode)  where collect_mode is
    "window" or "fallback".
    """
    is_seed = source in _SEED_SOURCES

    result = await collector.collect_messages(
        username  = chat_key,
        date_from = COLLECT_DATE_FROM,
        date_to   = COLLECT_DATE_TO,
    )

    # Propagate TTL immediately no point checking message count
    if result.has_ttl:
        return result, "window"

    if is_seed or len(result.messages) >= SNOWBALL_MIN_MESSAGES:
        return result, "window"

    # Snowball fallback: window didn't yield enough messages
    logger.info(
        f"[{chat_key}] Window yielded only {len(result.messages)} messages "
        f"(< {SNOWBALL_MIN_MESSAGES}) — falling back to last "
        f"{SNOWBALL_FALLBACK_LIMIT} messages."
    )
    result = await collector.collect_messages(
        username = chat_key,
        limit    = SNOWBALL_FALLBACK_LIMIT,
    )
    return result, "fallback"


async def process_chat(
    chat_doc:   dict,
    db:         DBManager,
    collector:  TelegramCollector,
    lang_det,
    classifier,
    dry_run:    bool = False,
) -> str:
    """
    Runs the full analysis pipeline for a single chat.

    Returns the final status string applied to the chat:
        "analysed" | "discarded_ttl" | "discarded_language" | "error" | "dry_run"
    """
    chat_key = chat_doc["_id"]
    source   = chat_doc.get("source", "unknown")
    logger.info(f"━━ Processing '{chat_key}'  source={source!r}")

    try:
        # Collect
        result, collect_mode = await _collect_for_chat(collector, chat_key, source)

        # TTL check
        if result.has_ttl:
            logger.info(f"[{chat_key}] Discarding — disappearing messages (TTL).")
            if not dry_run:
                db.mark_chat_discarded(chat_key, reason="ttl")
            return "discarded_ttl"

        if not result.messages:
            logger.warning(f"[{chat_key}] No messages collected — marking error.")
            if not dry_run:
                db.mark_chat_error(chat_key, "No messages collected in window.")
            return "error"

        # Language detection
        lang_result  = lang_det.detect(result.messages)
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
            if not dry_run:
                db._chats.update_one(
                    {"_id": chat_key},
                    {"$set": {
                        "lang_result":  lang_summary,
                        "status":       "discarded_language",
                        "processed_at": datetime.now(timezone.utc),
                    }},
                )
            return "discarded_language"

        logger.info(
            f"[{chat_key}] Language PASSED — "
            f"en_fraction={lang_result.english_fraction:.1%}"
        )

        # RoBERTa
        texts      = [m.text for m in result.messages if m.text.strip()]
        clf_result = classifier.classify_batch(texts)

        # crypto_fraction
        n_messages_total = len(result.messages)
        n_english        = len(texts)
        n_crypto         = clf_result.n_crypto
        crypto_fraction  = n_crypto / n_english if n_english > 0 else 0.0

        logger.info(
            f"[{chat_key}] "
            f"n_total={n_messages_total}  n_english={n_english}  "
            f"n_crypto={n_crypto}  crypto_fraction={crypto_fraction:.3f}  "
            f"mode={collect_mode}"
        )

        if dry_run:
            logger.info(f"[{chat_key}] dry-run — nothing written.")
            return "dry_run"

        # Persist messages
        msg_dicts = [_serialise_message(m) for m in result.messages]
        inserted  = db.bulk_insert_messages(msg_dicts)
        logger.info(f"[{chat_key}] {inserted}/{len(msg_dicts)} messages stored.")

        # Mark analysed
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
            "n_messages_total": n_messages_total,
            "n_english":        n_english,
            "n_crypto":         n_crypto,
            "crypto_fraction":  round(crypto_fraction, 6),
            "collect_mode":     collect_mode,
            "date_from":        COLLECT_DATE_FROM if collect_mode == "window" else None,
            "date_to":          COLLECT_DATE_TO   if collect_mode == "window" else None,
        }

        db.mark_chat_analysed(
            chat_id_str      = chat_key,
            metadata         = chat_metadata,
            lang_result      = lang_summary,
            collection_stats = collection_stats,
        )

        # Snowball
        if result.snowball_usernames:
            added = db.bulk_upsert_pending(
                usernames = result.snowball_usernames,
                source    = "snowball",
            )
            logger.info(
                f"[{chat_key}] Snowball: "
                f"{len(result.snowball_usernames)} candidates, {added} newly enqueued."
            )

        return "analysed"

    except ValueError as exc:
        db.mark_chat_error(chat_key, str(exc))
        return "error"

    except Exception as exc:
        logger.exception(f"[{chat_key}] Unexpected error: {exc}")
        db.mark_chat_error(chat_key, str(exc))
        return "error"


async def run(args: argparse.Namespace) -> None:
    cfg        = _load_config(args.config)
    db         = DBManager(args.config)
    collector  = TelegramCollector(args.config)

    lang_det   = build_language_detector(
        threshold  = LANGUAGE_ENGLISH_THRESHOLD,
        min_chars  = LANGUAGE_MIN_CHARS,
        use_langid = LANGUAGE_USE_LANGID,
    )

    model_path = cfg.get("RoBERTa", "model_path", fallback="models/roberta-crypto")
    use_gpu    = cfg.getboolean("RoBERTa", "use_gpu", fallback=False)
    classifier = build_classifier(
        model_path   = model_path,
        mock         = args.mock_classifier,
        threshold    = ROBERTA_THRESHOLD,
        batch_size   = ROBERTA_BATCH_SIZE,
        crypto_label = ROBERTA_CRYPTO_LABEL,
        use_gpu      = use_gpu,
    )

    counts = {
        "analysed": 0, "discarded_ttl": 0,
        "discarded_language": 0, "error": 0, "dry_run": 0,
    }
    processed = 0

    async with collector:
        logger.info(
            f"✓ analyse.py started — "
            f"limit={args.limit or 'unlimited'}  dry_run={args.dry_run}"
        )
        logger.info(f"[queue] {db.queue_stats()}")

        while True:
            if args.limit and processed >= args.limit:
                logger.info(f"Reached --limit {args.limit}. Stopping.")
                break

            chat_doc = db.pop_next_pending()
            if chat_doc is None:
                logger.info("Queue empty — nothing more to process.")
                break

            status = await process_chat(
                chat_doc   = chat_doc,
                db         = db,
                collector  = collector,
                lang_det   = lang_det,
                classifier = classifier,
                dry_run    = args.dry_run,
            )
            counts[status] = counts.get(status, 0) + 1
            processed += 1
    logger.info("─" * 60)
    logger.info(f"Done. Processed {processed} chats.")
    for status, n in counts.items():
        if n:
            logger.info(f"  {status:<22} {n:>6}")
    logger.info(f"[queue] {db.queue_stats()}")
    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyse pending Telegram chats and compute crypto_fraction.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of chats to process (default: unlimited).",
    )
    parser.add_argument(
        "--mock-classifier",
        action="store_true",
        help="Use keyword-based mock classifier (no model/GPU needed).",
    )
    parser.add_argument(
        "--config",
        default="config/config.ini",
        help="Path to the INI configuration file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect and classify but do not write anything to MongoDB.",
    )
    asyncio.run(run(parser.parse_args()))