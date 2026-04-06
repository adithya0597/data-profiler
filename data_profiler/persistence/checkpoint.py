"""SQLite-based checkpoint for resumable profiling runs."""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path


class CheckpointDB:
    """Tracks profiling progress per table for resume support."""

    def __init__(self, path: str = "profiler_checkpoint.db"):
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS profiled_tables (
                run_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                error_message TEXT,
                PRIMARY KEY (run_id, table_name)
            )
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_run_status
            ON profiled_tables(run_id, status)
        """)
        self._conn.commit()

    def mark_started(self, run_id: str, table_name: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO profiled_tables
                   (run_id, table_name, started_at, status)
                   VALUES (?, ?, ?, 'in_progress')""",
                (run_id, table_name, now),
            )
            self._conn.commit()

    def mark_done(self, run_id: str, table_name: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                """UPDATE profiled_tables
                   SET completed_at = ?, status = 'done'
                   WHERE run_id = ? AND table_name = ?""",
                (now, run_id, table_name),
            )
            self._conn.commit()

    def mark_error(self, run_id: str, table_name: str, error: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                """UPDATE profiled_tables
                   SET completed_at = ?, status = 'error', error_message = ?
                   WHERE run_id = ? AND table_name = ?""",
                (now, error, run_id, table_name),
            )
            self._conn.commit()

    def get_completed_tables(self, run_id: str) -> set[str]:
        """Get tables that completed successfully (skip on resume)."""
        cursor = self._conn.execute(
            "SELECT table_name FROM profiled_tables WHERE run_id = ? AND status IN ('done', 'skipped')",
            (run_id,),
        )
        return {row[0] for row in cursor.fetchall()}

    def get_incomplete_tables(self, run_id: str) -> set[str]:
        """Get tables that need retry (in_progress or error)."""
        cursor = self._conn.execute(
            "SELECT table_name FROM profiled_tables WHERE run_id = ? AND status IN ('in_progress', 'error')",
            (run_id,),
        )
        return {row[0] for row in cursor.fetchall()}

    def close(self) -> None:
        self._conn.close()
