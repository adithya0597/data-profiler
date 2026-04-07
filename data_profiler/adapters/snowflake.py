"""Snowflake adapter: row-based sampling, native HLL + STDDEV."""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, Engine

from data_profiler.adapters.base import BaseAdapter


class SnowflakeAdapter(BaseAdapter):
    engine_name = "snowflake"

    def __init__(self, dsn: str, **kwargs: Any):
        super().__init__(dsn, **kwargs)

    def connect(self) -> Engine:
        self._engine = create_engine(self.dsn)
        return self._engine

    def sample_clause(self, table_name: str, sample_size: int, total_rows: int) -> str:
        if sample_size == 0 or sample_size >= total_rows:
            return ""
        # Snowflake only supports SEED with BERNOULLI/SYSTEM sampling, not row-based
        pct = min((sample_size / total_rows) * 100, 100.0)
        return f"SAMPLE BERNOULLI ({pct:.4f}) SEED (42)"

    def set_session_params(self, engine: Engine, config: "ProfilerConfig") -> None:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {int(config.query_timeout)}"))
            conn.execute(text("ALTER SESSION SET QUERY_TAG = 'data_profiler'"))
            conn.commit()

    def approx_distinct_sql(self, column: str, alias: str) -> str:
        return f"APPROX_COUNT_DISTINCT({column}) AS {alias}"

    def stddev_sql(self, column: str, alias: str) -> str | None:
        return f"STDDEV({column}) AS {alias}"

    def skewness_sql(self, column: str) -> str | None:
        return f"SKEW({column})"

    def kurtosis_sql(self, column: str) -> str | None:
        return f"KURTOSIS({column})"

    def correlation_sql(self, col1: str, col2: str) -> str | None:
        return f"CORR({col1}, {col2})"

    def percentile_sql(self, column: str, quantiles: list[float], aliases: list[str]) -> list[str]:
        return [
            f"APPROX_PERCENTILE({column}, {q}) AS {alias}"
            for q, alias in zip(quantiles, aliases)
        ]
