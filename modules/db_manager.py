# modules/db_manager.py
"""
MongoDB data access layer.

Collections
chats:        pipeline queue + channel/group metadata
messages:     normalised collected messages (TeleScope-aligned schema)
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
                             MONGO_DB_NAME, STATUS_ANALYSED, STATUS_COLLECTED,
                             STATUS_DISCARDED, STATUS_ERROR, STATUS_PENDING,
                             STATUS_RUNNING)

logger = logging.getLogger(__name__)

"""
Collection: chats
{
  "_id":                str,
  "telegram_id":        int | None,
  "type":               str,            # "channel" | "group" | "unknown"
  "title":              str | None,
  "username":           str | None,
  "description":        str | None,
  "n_subscribers":      int | None,
  "is_scam":            bool,
  "is_fake":            bool,
  "is_verified":        bool,
  "is_restricted":      bool,
  "restriction_reason": str | None,
  "creation_date":      datetime | None,
  "status":             str,            # pending | running | analysed | collected |
                                        # discarded | discarded_language |
                                        # discarded_ttl | error
  "source":             str,
  "added_at":           datetime,
  "processed_at":       datetime | None,
  "error_msg":          str | None,
  "lang_result":        dict | None,
  "collection_stats":   dict | None,    # set by mark_chat_analysed
  # collection_stats schema:
  #   {
  #     "n_messages_total":   int,   # all messages in the window
  #     "n_english":          int,   # messages that passed lang detection
  #     "n_crypto":           int,   # English messages classified as crypto
  #     "crypto_fraction":    float, # n_crypto / n_english  (NaN -> 0.0)
  #     "collect_mode":       str,   # "window" | "fallback"
  #     "date_from":          datetime | None,
  #     "date_to":            datetime | None,
  #   }
}

Collection: messages  (schema unchanged)
"""


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
    """

    def __init__(self, config_file: str = "config/config.ini"):
        uri            = _get_uri(config_file)
        self._client   = MongoClient(uri)
        self._db       = self._client[MONGO_DB_NAME]
        self._chats    = self._db[COLLECTION_CHATS]
        self._messages = self._db[COLLECTION_MESSAGES]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self._chats.create_index("status")
        self._chats.create_index("telegram_id", sparse=True)
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
            "lang_result":        None,
            "collection_stats":   None,
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
        return sum(self.upsert_chat_pending(u, source=source) for u in usernames)

    def pop_next_pending(self) -> Optional[dict]:
        return self._chats.find_one_and_update(
            {"status": STATUS_PENDING},
            {"$set": {"status": STATUS_RUNNING}},
            sort=[("added_at", ASCENDING)],
            return_document=True,
        )

    def mark_chat_analysed(
        self,
        chat_id_str:      str,
        metadata:         dict,
        lang_result:      dict,
        collection_stats: dict,
    ) -> None:
        """
        Marks a chat as 'analysed': the intermediate status used while the
        crypto_fraction threshold is still being determined from the CDF.

        Saves all collected metrics without making a keep/discard decision.
        The orchestrator will later re-scan 'analysed' chats and promote them
        to 'collected' or 'discarded' once the threshold is known.
        """
        self._chats.update_one(
            {"_id": chat_id_str},
            {"$set": {
                **metadata,
                "status":           STATUS_ANALYSED,
                "lang_result":      lang_result,
                "collection_stats": collection_stats,
                "processed_at":     datetime.now(timezone.utc),
                "error_msg":        None,
            }},
        )
        cf = collection_stats.get("crypto_fraction", 0.0)
        logger.info(
            f"[queue] '{chat_id_str}' → analysed  "
            f"n_msgs={collection_stats.get('n_messages_total', '?')}  "
            f"n_en={collection_stats.get('n_english', '?')}  "
            f"crypto_fraction={cf:.3f}"
        )

    def mark_chat_collected(
        self,
        chat_id_str:  str,
        metadata:     dict,
        lang_result:  Optional[dict] = None,
    ) -> None:
        """Promotes an already-analysed chat to 'collected' (post-threshold step)."""
        self._chats.update_one(
            {"_id": chat_id_str},
            {"$set": {
                **metadata,
                "status":       STATUS_COLLECTED,
                "lang_result":  lang_result,
                "processed_at": datetime.now(timezone.utc),
                "error_msg":    None,
            }},
        )
        logger.info(f"[queue] '{chat_id_str}' → collected")

    def mark_chat_discarded(
        self,
        chat_id_str: str,
        reason:      str = "not_crypto",
    ) -> None:
        from config.settings import (STATUS_DISCARDED_LANGUAGE,
                                     STATUS_DISCARDED_TTL)
        status_map = {
            "not_crypto": STATUS_DISCARDED,
            "language":   STATUS_DISCARDED_LANGUAGE,
            "ttl":        STATUS_DISCARDED_TTL,
        }
        status = status_map.get(reason, f"discarded_{reason}")
        self._chats.update_one(
            {"_id": chat_id_str},
            {"$set": {
                "status":       status,
                "processed_at": datetime.now(timezone.utc),
            }},
        )
        logger.info(f"[queue] '{chat_id_str}' → {status}")

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

    def bulk_insert_messages(self, messages: list[dict]) -> int:
        if not messages:
            return 0
        ops = [
            UpdateOne({"_id": m["_id"]}, {"$setOnInsert": m}, upsert=True)
            for m in messages
        ]
        result   = self._messages.bulk_write(ops, ordered=False)
        inserted = result.upserted_count
        logger.info(f"[messages] Inserted {inserted}/{len(messages)}")
        return inserted

    def get_messages_by_chat(self, telegram_id: int) -> list[dict]:
        return list(self._messages.find({"chat_id": telegram_id}))

    def queue_stats(self) -> str:
        from config.settings import (STATUS_ANALYSED,
                                     STATUS_DISCARDED_LANGUAGE,
                                     STATUS_DISCARDED_TTL)
        counts = self.count_by_status()
        statuses = [
            STATUS_PENDING, STATUS_RUNNING, STATUS_ANALYSED,
            STATUS_COLLECTED, STATUS_DISCARDED,
            STATUS_DISCARDED_LANGUAGE, STATUS_DISCARDED_TTL, STATUS_ERROR,
        ]
        return "  ".join(f"{s}={counts.get(s, 0)}" for s in statuses)

    def close(self) -> None:
        self._client.close()