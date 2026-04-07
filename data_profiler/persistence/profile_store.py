"""Profile snapshot storage for incremental profiling."""

from __future__ import annotations

import dataclasses
import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

from data_profiler.persistence.serializers import _clean_profile

if TYPE_CHECKING:
    from data_profiler.workers.stats_worker import ColumnProfile, ProfiledTable


class ProfileStore:
    """Stores serialized profile snapshots in SQLite for incremental comparison."""

    def __init__(self, conn: sqlite3.Connection, lock: threading.Lock):
        self._conn = conn
        self._lock = lock
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS profile_snapshots (
                run_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                profile_json TEXT NOT NULL,
                row_count INTEGER,
                column_hash TEXT,
                watermark_value TEXT,
                database TEXT,
                schema_name TEXT,
                stored_at TEXT,
                PRIMARY KEY (run_id, table_name)
            )
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshot_lookup
            ON profile_snapshots(database, schema_name, run_id)
        """)
        self._conn.commit()

    def store_profile(
        self,
        run_id: str,
        table_name: str,
        profile: "ProfiledTable",
        column_hash: str = "",
        watermark_value: str | None = None,
        database: str | None = None,
        schema_name: str | None = None,
    ) -> None:
        """Store a profile snapshot."""
        data = _clean_profile(profile)
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO profile_snapshots
                   (run_id, table_name, profile_json, row_count, column_hash,
                    watermark_value, database, schema_name, stored_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, table_name, json.dumps(data, default=str),
                 profile.total_row_count, column_hash, watermark_value,
                 database, schema_name, now),
            )
            self._conn.commit()

    def load_profile(self, run_id: str, table_name: str) -> "ProfiledTable | None":
        """Load a stored profile snapshot, or None if not found."""
        cursor = self._conn.execute(
            "SELECT profile_json FROM profile_snapshots WHERE run_id = ? AND table_name = ?",
            (run_id, table_name),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return _dict_to_profile(json.loads(row[0]))

    def load_all_profiles(self, run_id: str) -> dict[str, "ProfiledTable"]:
        """Load all profiles for a run."""
        cursor = self._conn.execute(
            "SELECT table_name, profile_json FROM profile_snapshots WHERE run_id = ?",
            (run_id,),
        )
        result = {}
        for table_name, profile_json in cursor.fetchall():
            result[table_name] = _dict_to_profile(json.loads(profile_json))
        return result

    def get_prior_metadata(self, run_id: str, table_name: str) -> dict[str, Any] | None:
        """Get lightweight metadata without deserializing the full profile."""
        cursor = self._conn.execute(
            "SELECT row_count, column_hash, watermark_value FROM profile_snapshots "
            "WHERE run_id = ? AND table_name = ?",
            (run_id, table_name),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return {
            "row_count": row[0],
            "column_hash": row[1],
            "watermark_value": row[2],
        }

    def get_latest_run_id(self, database: str | None = None, schema_name: str | None = None) -> str | None:
        """Find the most recent run ID for the given database/schema."""
        if database or schema_name:
            cursor = self._conn.execute(
                "SELECT run_id FROM profile_snapshots "
                "WHERE database = ? AND schema_name = ? "
                "ORDER BY stored_at DESC LIMIT 1",
                (database, schema_name),
            )
        else:
            cursor = self._conn.execute(
                "SELECT run_id FROM profile_snapshots ORDER BY stored_at DESC LIMIT 1",
            )
        row = cursor.fetchone()
        return row[0] if row else None


def _dict_to_column(d: dict[str, Any]) -> "ColumnProfile":
    """Reconstruct a ColumnProfile from a dict."""
    from data_profiler.workers.stats_worker import ColumnProfile
    field_names = {f.name for f in dataclasses.fields(ColumnProfile)}
    filtered = {k: v for k, v in d.items() if k in field_names}
    return ColumnProfile(**filtered)


def _dict_to_profile(d: dict[str, Any]) -> "ProfiledTable":
    """Reconstruct a ProfiledTable from a dict (including nested ColumnProfiles)."""
    from data_profiler.workers.stats_worker import ProfiledTable
    col_dicts = d.pop("columns", [])
    columns = [_dict_to_column(c) for c in col_dicts]
    field_names = {f.name for f in dataclasses.fields(ProfiledTable)}
    filtered = {k: v for k, v in d.items() if k in field_names}
    filtered["columns"] = columns
    return ProfiledTable(**filtered)
