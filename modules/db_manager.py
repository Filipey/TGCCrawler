# modules/db_manager.py
"""
MongoDB data access layer.

Collections

chats        pipeline queue + channel/group metadata
messages     normalized collected messages (TeleScope-aligned schema)

Schemas are documented inline as comments for reference and type-checking.
"""

from __future__ import annotations

import configparser
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError

from config.settings import (COLLECTION_CHATS, COLLECTION_MESSAGES,
                             MONGO_DB_NAME, STATUS_COLLECTED, STATUS_DISCARDED,
                             STATUS_ERROR, STATUS_PENDING, STATUS_RUNNING)

logger = logging.getLogger(__name__)

# Collection schemas
"""
Collection: chats

{
  "_id":                str,          # normalized username (no @)
  "telegram_id":        int | None,   # numeric ID resolved by Telethon
  "type":               str,          # "channel" | "group" | "unknown"
  "title":              str | None,
  "username":           str | None,
  "description":        str | None,   # channel 'about' text
  "n_subscribers":      int | None,   # participants_count
  "is_scam":            bool,
  "is_fake":            bool,         # impersonation flag (TeleScope Table 4)
  "is_verified":        bool,
  "is_restricted":      bool,
  "restriction_reason": str | None,
  "creation_date":      datetime | None,
  "status":             str,          # pending | running | collected | discarded |
                                      # discarded_language | error
  "source":             str,          # "tgstats" | "telegramchannels" |
                                      # "snowball" | "manual"
  "added_at":           datetime,
  "processed_at":       datetime | None,
  "error_msg":          str | None,
  "roberta_score":      float | None, # mean crypto probability from classifier
  "lang_result":        dict | None,  # LanguageDetectionResult summary dict
}

Collection: messages

{
  "_id":                    str,       # "{chat_id}_{message_id}"
  "chat_id":                int,
  "chat_username":          str | None,
  "message_id":             int,
  "text":                   str,
  "date":                   datetime,
  "edit_date":              datetime | None,
  "is_pinned":              bool,

  # Authorship
  "author_id":              int | None,
  "is_bot_author":          bool,
  "is_verified_author":     bool,

  # Threading
  "reply_to_message_id":    int | None,

  # Forward provenance
  "is_forwarded":           bool,
  "forwarded_from_id":      int | None,
  "forwarded_from_type":    str | None,   # "channel" | "user" | "chat"
  "forwarded_from_name":    str | None,
  "forwarded_date":         datetime | None,
  "forwarded_message_id":   int | None,

  # Engagement metrics (TeleScope Sections 4.2 + 4.3)
  "views":                  int | None,
  "forwards_count":         int | None,
  "reactions":              [{"emoticon": str, "count": int, "is_chosen": bool}],

  # Media
  "has_media":              bool,
  "media_type":             str | None,

  # Entities (TeleScope Section 4.2)
  "entities":               [{"type": str, "value": str,
                              "offset": int, "length": int, "url": str|None}],
  "hashtags":               [str],
  "outbound_links":         [str],
  "outbound_tg_usernames":  [str],
}
"""


# DBManager

def _get_uri(config_file: str = "config/config.ini") -> str:
    uri = os.environ.get("MONGO_DB_URL")
    if uri:
        return uri
    cfg = configparser.ConfigParser()
    cfg.read(config_file)
    return cfg.get("MongoDB", "uri", fallback="mongodb://admin:admin@localhost:27017")


class DBManager:
    """
    Thread-safe MongoDB wrapper (one instance per worker process).

    Manages two collections:
        - chats:    pipeline queue and chat metadata
        - messages: collected message records
    """

    def __init__(self, config_file: str = "config/config.ini"):
        uri           = _get_uri(config_file)
        self._client  = MongoClient(uri)
        self._db      = self._client[MONGO_DB_NAME]
        self._chats   = self._db[COLLECTION_CHATS]
        self._messages = self._db[COLLECTION_MESSAGES]
        self._ensure_indexes()

    # Indexes

    def _ensure_indexes(self) -> None:
        self._chats.create_index("status")
        self._chats.create_index("telegram_id",  sparse=True)
        self._chats.create_index([("status", ASCENDING), ("added_at", ASCENDING)])
        self._messages.create_index("chat_id")
        self._messages.create_index("date")
        self._messages.create_index("outbound_tg_usernames")
        self._messages.create_index("hashtags")
        self._messages.create_index("author_id", sparse=True)
        logger.debug("MongoDB indexes verified.")

    # Queue operations

    def upsert_chat_pending(
        self,
        username: str,
        source:   str = "manual",
        extra:    Optional[dict] = None,
    ) -> bool:
        """
        Inserts a chat with status=pending if it does not already exist.
        Returns True if inserted, False if it already existed.
        """
        doc: dict[str, Any] = {
            "_id":                username.lower().lstrip("@"),
            "telegram_id":        None,
            "type":               "unknown",
            "title":              None,
            "username":           username.lower().lstrip("@"),
            "description":        None,
            "n_subscribers":      None,
            "is_scam":            False,
            "is_fake":            False,
            "is_verified":        False,
            "is_restricted":      False,
            "restriction_reason": None,
            "creation_date":      None,
            "status":             STATUS_PENDING,
            "source":             source,
            "added_at":           datetime.now(timezone.utc),
            "processed_at":       None,
            "error_msg":          None,
            "roberta_score":      None,
            "lang_result":        None,
        }
        if extra:
            doc.update(extra)

        try:
            self._chats.insert_one(doc)
            logger.info(f"[queue] Added '{username}' (source={source})")
            return True
        except DuplicateKeyError:
            logger.debug(f"[queue] Already exists: '{username}'")
            return False

    def bulk_upsert_pending(self, usernames: list[str], source: str) -> int:
        """Inserts multiple pending usernames. Returns count of newly inserted."""
        return sum(self.upsert_chat_pending(u, source=source) for u in usernames)

    def pop_next_pending(self) -> Optional[dict]:
        """
        Atomically fetches the next pending chat and sets status=running.
        Safe for concurrent workers: uses find_one_and_update.
        """
        return self._chats.find_one_and_update(
            {"status": STATUS_PENDING},
            {"$set": {"status": STATUS_RUNNING}},
            sort=[("added_at", ASCENDING)],
            return_document=True,
        )

    def mark_chat_collected(
        self,
        chat_id_str:  str,
        metadata:     dict,
        roberta_score: float,
        lang_result:  Optional[dict] = None,
    ) -> None:
        self._chats.update_one(
            {"_id": chat_id_str},
            {"$set": {
                **metadata,
                "status":        STATUS_COLLECTED,
                "roberta_score": roberta_score,
                "lang_result":   lang_result,
                "processed_at":  datetime.now(timezone.utc),
                "error_msg":     None,
            }},
        )
        logger.info(f"[queue] '{chat_id_str}' → collected (score={roberta_score:.3f})")

    def mark_chat_discarded(
        self,
        chat_id_str:  str,
        roberta_score: float,
        reason:       str = "not_crypto",
    ) -> None:
        status = f"discarded_{reason}" if reason != "not_crypto" else STATUS_DISCARDED
        self._chats.update_one(
            {"_id": chat_id_str},
            {"$set": {
                "status":        status,
                "roberta_score": roberta_score,
                "processed_at":  datetime.now(timezone.utc),
            }},
        )
        logger.info(f"[queue] '{chat_id_str}' → {status} (score={roberta_score:.3f})")

    def mark_chat_error(self, chat_id_str: str, error_msg: str) -> None:
        self._chats.update_one(
            {"_id": chat_id_str},
            {"$set": {
                "status":       STATUS_ERROR,
                "error_msg":    error_msg,
                "processed_at": datetime.now(timezone.utc),
            }},
        )
        logger.warning(f"[queue] '{chat_id_str}' → error: {error_msg}")

    def count_by_status(self) -> dict[str, int]:
        pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
        return {r["_id"]: r["count"] for r in self._chats.aggregate(pipeline)}

    # Messages

    def bulk_insert_messages(self, messages: list[dict]) -> int:
        """
        Batch-upserts messages using ordered=False (silently skips duplicates).
        Returns the count of newly inserted documents.
        """
        if not messages:
            return 0
        ops = [
            UpdateOne({"_id": m["_id"]}, {"$setOnInsert": m}, upsert=True)
            for m in messages
        ]
        result  = self._messages.bulk_write(ops, ordered=False)
        inserted = result.upserted_count
        logger.info(f"[messages] Inserted {inserted}/{len(messages)}")
        return inserted

    def get_messages_by_chat(self, telegram_id: int) -> list[dict]:
        return list(self._messages.find({"chat_id": telegram_id}))

    # Stats

    def queue_stats(self) -> str:
        counts = self.count_by_status()
        parts  = [
            f"{s}={counts.get(s, 0)}"
            for s in [STATUS_PENDING, STATUS_RUNNING, STATUS_COLLECTED,
                      STATUS_DISCARDED, STATUS_ERROR]
        ]
        return "  ".join(parts)

    def close(self) -> None:
        self._client.close()
