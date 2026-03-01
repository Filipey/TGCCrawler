# scraping/storage/snapshot_store.py
"""
Daily snapshot management and delta computation.

Storage layout
data/
  snapshots/
    tgstats_groups_2025-07-01.jsonl           one record per line (JSONL)
    tgstats_channels_2025-07-01.jsonl
    telegramchannels_groups_2025-07-01.jsonl
    telegramchannels_channels_2025-07-01.jsonl
    tgstats_groups_2025-07-01.csv             parallel CSV for easy inspection
    ...
  deltas/
    delta_2025-07-01.json                     daily comparison summary
  manifests/
    latest.json                               points to the most recent snapshot

JSONL format (one ScrapeRecord per line):
  {"username":"btcnews","source":"tgstats","chat_type":"group",...,"scraped_at":"..."}

Delta JSON format:
  [
    {
      "date":        "2025-07-01",
      "prev_date":   "2025-06-30",
      "source":      "tgstats",
      "chat_type":   "group",
      "total_today": 1240,
      "total_prev":  1198,
      "new":         ["alice","bob",...],       present today, absent yesterday
      "removed":     ["charlie",...],           present yesterday, absent today
      "n_new":       42,
      "n_removed":   0,
      "net_change":  +42
    },
    ...
  ]
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from ..sources.base import ScrapeRecord

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path("data")


def _snapshot_filename(source: str, chat_type: str, run_date: date) -> str:
    return f"{source}_{chat_type}s_{run_date.isoformat()}.jsonl"


def _delta_filename(run_date: date) -> str:
    return f"delta_{run_date.isoformat()}.json"


class SnapshotStore:
    """
    Persists daily snapshots (JSONL + CSV) and computes deltas between runs.

    Args:
        data_dir: Root directory for all data files.
    """

    def __init__(self, data_dir: str | Path = DEFAULT_DATA_DIR):
        self.root         = Path(data_dir)
        self.snap_dir     = self.root / "snapshots"
        self.delta_dir    = self.root / "deltas"
        self.manifest_dir = self.root / "manifests"

        for d in (self.snap_dir, self.delta_dir, self.manifest_dir):
            d.mkdir(parents=True, exist_ok=True)

    # Save

    def save_snapshot(
        self,
        records:   list[ScrapeRecord],
        source:    str,
        chat_type: str,
        run_date:  Optional[date] = None,
    ) -> Path:
        """
        Saves records for a (source x chat_type) pair to JSONL.
        Returns the path of the created file.
        """
        run_date = run_date or date.today()
        fname    = _snapshot_filename(source, chat_type, run_date)
        fpath    = self.snap_dir / fname

        with open(fpath, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec.to_dict(), ensure_ascii=False) + "\n")

        logger.info(f"[storage] Snapshot saved → {fpath} ({len(records)} records)")
        self._update_manifest(source, chat_type, run_date, fpath, len(records))
        return fpath

    def save_csv(
        self,
        records:   list[ScrapeRecord],
        source:    str,
        chat_type: str,
        run_date:  Optional[date] = None,
    ) -> Path:
        """
        Saves records as CSV (complementary to the JSONL snapshot).
        Returns the path of the created file.
        """
        import csv
        run_date = run_date or date.today()
        fname    = f"{source}_{chat_type}s_{run_date.isoformat()}.csv"
        fpath    = self.snap_dir / fname

        if not records:
            return fpath

        fieldnames = list(records[0].to_dict().keys())
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for rec in records:
                writer.writerow(rec.to_dict())

        logger.info(f"[storage] CSV saved → {fpath} ({len(records)} records)")
        return fpath

    # Load

    def load_snapshot(
        self,
        source:    str,
        chat_type: str,
        run_date:  date,
    ) -> list[dict]:
        """Loads a JSONL snapshot for a specific date."""
        fname = _snapshot_filename(source, chat_type, run_date)
        fpath = self.snap_dir / fname

        if not fpath.exists():
            return []

        records = []
        with open(fpath, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def load_usernames(
        self,
        source:    str,
        chat_type: str,
        run_date:  date,
    ) -> set[str]:
        """Returns the set of usernames from a snapshot (fast path for delta)."""
        return {r["username"] for r in self.load_snapshot(source, chat_type, run_date)}

    def find_previous_date(
        self,
        source:    str,
        chat_type: str,
        before:    date,
    ) -> Optional[date]:
        """
        Finds the date of the most recent snapshot strictly before `before`.
        Returns None if no earlier snapshot exists.
        """
        prefix = f"{source}_{chat_type}s_"
        dates: list[date] = []

        for f in self.snap_dir.glob(f"{prefix}*.jsonl"):
            try:
                d = date.fromisoformat(f.stem.replace(prefix, ""))
                if d < before:
                    dates.append(d)
            except ValueError:
                pass

        return max(dates) if dates else None

    # Delta

    def compute_delta(
        self,
        source:    str,
        chat_type: str,
        today:     Optional[date] = None,
    ) -> dict:
        """
        Computes the delta between today's snapshot and the most recent prior one.
        Returns a dict with the structure documented in the module docstring.
        """
        today = today or date.today()
        prev  = self.find_previous_date(source, chat_type, before=today)

        today_set = self.load_usernames(source, chat_type, today)
        prev_set  = self.load_usernames(source, chat_type, prev) if prev else set()

        new_usernames     = sorted(today_set - prev_set)
        removed_usernames = sorted(prev_set - today_set)

        delta = {
            "date":        today.isoformat(),
            "prev_date":   prev.isoformat() if prev else None,
            "source":      source,
            "chat_type":   chat_type,
            "total_today": len(today_set),
            "total_prev":  len(prev_set),
            "new":         new_usernames,
            "removed":     removed_usernames,
            "n_new":       len(new_usernames),
            "n_removed":   len(removed_usernames),
            "net_change":  len(new_usernames) - len(removed_usernames),
        }

        logger.info(
            f"[delta] {source}/{chat_type} "
            f"{prev.isoformat() if prev else 'N/A'} → {today.isoformat()} | "
            f"+{delta['n_new']} new, -{delta['n_removed']} removed "
            f"(net {delta['net_change']:+d})"
        )
        return delta

    def save_delta(
        self,
        delta:    dict,
        run_date: Optional[date] = None,
    ) -> Path:
        """
        Persists the delta to JSON, accumulating multiple deltas per day
        (one per source x chat_type combination).
        """
        run_date = run_date or date.today()
        fpath    = self.delta_dir / _delta_filename(run_date)

        daily_deltas: list[dict] = []
        if fpath.exists():
            with open(fpath, encoding="utf-8") as f:
                daily_deltas = json.load(f)

        daily_deltas.append(delta)

        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(daily_deltas, f, ensure_ascii=False, indent=2)

        logger.info(f"[storage] Delta saved → {fpath}")
        return fpath

    #  Summary

    def daily_summary(self, run_date: Optional[date] = None) -> str:
        """
        Returns a formatted string with the day's delta summary:
        totals, new, and removed entries per (source × type).
        """
        run_date = run_date or date.today()
        fpath    = self.delta_dir / _delta_filename(run_date)

        if not fpath.exists():
            return f"No delta file found for {run_date.isoformat()}"

        with open(fpath, encoding="utf-8") as f:
            deltas = json.load(f)

        lines = [
            f"━━ Daily Summary {run_date.isoformat()} ━━",
            f"{'Source':<22} {'Type':<10} {'Today':>8} {'Previous':>10} "
            f"{'New':>8} {'Removed':>9} {'Net':>6}",
            "─" * 68,
        ]
        for d in deltas:
            lines.append(
                f"{d['source']:<22} {d['chat_type']:<10} "
                f"{d['total_today']:>8,} {d['total_prev']:>10,} "
                f"{d['n_new']:>+8,} {d['n_removed']:>9,} "
                f"{d['net_change']:>+6,}"
            )
        return "\n".join(lines)

    # Manifest

    def _update_manifest(
        self,
        source:    str,
        chat_type: str,
        run_date:  date,
        fpath:     Path,
        n_records: int,
    ) -> None:
        """Updates latest.json with a reference to the most recent snapshot."""
        manifest_path = self.manifest_dir / "latest.json"
        manifest: dict = {}

        if manifest_path.exists():
            with open(manifest_path, encoding="utf-8") as f:
                manifest = json.load(f)

        key = f"{source}_{chat_type}"
        manifest[key] = {
            "date":       run_date.isoformat(),
            "file":       str(fpath),
            "n_records":  n_records,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
