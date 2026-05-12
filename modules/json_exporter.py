# modules/json_exporter.py
"""
JSONL export of collected messages to the local filesystem.

Files are organised as:
    data/messages/{source}/{YYYY-MM-DD}.jsonl

One file per (source, crawl-date) pair.  Each line is a complete message
document serialised as a single JSON object — identical schema to MongoDB.

Partitioning by crawl-date (the day the pipeline processed the channel)
keeps every channel's messages in one file regardless of the 30-day
collection window spanned by those messages.

Append semantics: the file is opened in "a" mode so multiple pipeline runs
on the same day accumulate in the same file without rewrites.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _json_default(obj: Any) -> Any:
    """Fallback serialiser for types json.dumps does not handle natively."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serialisable")


class JsonExporter:
    """
    Appends message documents to JSONL files partitioned by source and crawl-date.

    Args:
        base_dir: Root directory for exported files (default: ``data/messages``).
    """

    def __init__(self, base_dir: str = "data/messages") -> None:
        self.base_dir = Path(base_dir)

    def write_messages(
        self,
        messages:   list[dict],
        source:     str,
        crawl_date: date,
    ) -> int:
        """
        Appends *messages* to ``{base_dir}/{source}/{crawl_date}.jsonl``.

        Creates the source subdirectory on first use.  Returns the number of
        lines written (equals ``len(messages)``).
        """
        if not messages:
            return 0

        source_dir = self.base_dir / source
        source_dir.mkdir(parents=True, exist_ok=True)

        file_path = source_dir / f"{crawl_date.isoformat()}.jsonl"

        written = 0
        with file_path.open("a", encoding="utf-8") as fh:
            for msg in messages:
                fh.write(json.dumps(msg, default=_json_default, ensure_ascii=False))
                fh.write("\n")
                written += 1

        logger.info(
            f"[json_export] {written} messages → {file_path}"
        )
        return written
