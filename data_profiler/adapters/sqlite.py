"""SQLite adapter: ROWID-based sampling, exact distinct, Python-side STDDEV."""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, Engine

from data_profiler.adapters.base import BaseAdapter


class SQLiteAdapter(BaseAdapter):
    engine_name = "sqlite"

    def __init__(self, dsn: str, **kwargs: Any):
        super().__init__(dsn, **kwargs)

    def connect(self) -> Engine:
        self._engine = create_engine(self.dsn)
        return self._engine

    def sample_clause(self, table_name: str, sample_size: int, total_rows: int) -> str:
        if sample_size == 0 or sample_size >= total_rows:
            return ""
        qt = self.quote_identifier(table_name)
        return (
            f"WHERE ROWID IN ("
            f"SELECT ROWID FROM {qt} ORDER BY RANDOM() LIMIT {sample_size}"
            f")"
        )

    def approx_distinct_sql(self, column: str, alias: str) -> str:
        # SQLite has no HLL; always exact
        return f"COUNT(DISTINCT {column}) AS {alias}"

    def stddev_sql(self, column: str, alias: str) -> str | None:
        # No native STDDEV in SQLite; computed in Python
        return None

    def supports_native_stddev(self) -> bool:
        return False

    def supports_percentiles(self) -> bool:
        return False

    def supports_constraints(self) -> bool:
        return False

    def distinct_mode(self) -> str:
        return "exact"
