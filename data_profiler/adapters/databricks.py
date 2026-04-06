"""Databricks adapter: Bernoulli sampling, native HLL + STDDEV."""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, Engine

from data_profiler.adapters.base import BaseAdapter


class DatabricksAdapter(BaseAdapter):
    engine_name = "databricks"

    def __init__(self, dsn: str, **kwargs: Any):
        super().__init__(dsn, **kwargs)

    def connect(self) -> Engine:
        self._engine = create_engine(self.dsn)
        return self._engine

    def sample_clause(self, table_name: str, sample_size: int, total_rows: int) -> str:
        if sample_size == 0 or sample_size >= total_rows:
            return ""
        # Databricks uses percentage-based Bernoulli sampling
        pct = min((sample_size / total_rows) * 100, 100.0)
        return f"TABLESAMPLE ({pct:.2f} PERCENT) REPEATABLE (42)"

    def approx_distinct_sql(self, column: str, alias: str) -> str:
        return f"APPROX_COUNT_DISTINCT({column}) AS {alias}"

    def stddev_sql(self, column: str, alias: str) -> str | None:
        return f"STDDEV({column}) AS {alias}"

    def skewness_sql(self, column: str) -> str | None:
        return f"SKEWNESS({column})"

    def kurtosis_sql(self, column: str) -> str | None:
        return f"KURTOSIS({column})"

    def correlation_sql(self, col1: str, col2: str) -> str | None:
        return f"CORR({col1}, {col2})"

    def quote_identifier(self, name: str) -> str:
        """Databricks uses backtick quoting."""
        escaped = name.replace("`", "``")
        return f"`{escaped}`"

    def percentile_sql(self, column: str, quantiles: list[float], aliases: list[str]) -> list[str]:
        return [
            f"PERCENTILE_APPROX({column}, {q}) AS {alias}"
            for q, alias in zip(quantiles, aliases)
        ]
