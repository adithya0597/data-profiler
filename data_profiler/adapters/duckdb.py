"""DuckDB adapter: in-process, reservoir sampling, native HLL + STDDEV."""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, Engine

from data_profiler.adapters.base import BaseAdapter


class DuckDBAdapter(BaseAdapter):
    engine_name = "duckdb"

    def __init__(self, dsn: str, **kwargs: Any):
        super().__init__(dsn, **kwargs)

    def connect(self) -> Engine:
        self._engine = create_engine(self.dsn)
        return self._engine

    def sample_clause(self, table_name: str, sample_size: int, total_rows: int) -> str:
        if sample_size == 0 or sample_size >= total_rows:
            return ""
        return f"USING SAMPLE reservoir({sample_size} ROWS) REPEATABLE (42)"

    def approx_distinct_sql(self, column: str, alias: str) -> str:
        return f"approx_count_distinct({column}) AS {alias}"

    def stddev_sql(self, column: str, alias: str) -> str | None:
        return f"stddev({column}) AS {alias}"

    def skewness_sql(self, column: str) -> str | None:
        return f"skewness({column})"

    def kurtosis_sql(self, column: str) -> str | None:
        return f"kurtosis({column})"

    def correlation_sql(self, col1: str, col2: str) -> str | None:
        return f"corr({col1}, {col2})"
