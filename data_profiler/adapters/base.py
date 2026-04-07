"""Abstract base adapter for database engines."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from sqlalchemy import Engine


class BaseAdapter(ABC):
    """Abstract interface that each engine adapter must implement."""

    engine_name: str  # "snowflake", "databricks", "duckdb", "sqlite"

    def __init__(self, dsn: str, **kwargs: Any):
        self.dsn = dsn
        self._engine: Engine | None = None

    @abstractmethod
    def connect(self) -> Engine:
        """Create and return a SQLAlchemy Engine."""

    @abstractmethod
    def sample_clause(self, table_name: str, sample_size: int, total_rows: int) -> str:
        """Return engine-specific sampling SQL clause.

        Returns empty string when sample_size == 0 or sample_size >= total_rows (full scan).
        """

    @abstractmethod
    def approx_distinct_sql(self, column: str, alias: str) -> str:
        """Return the HLL approximate distinct count expression for this engine."""

    @abstractmethod
    def stddev_sql(self, column: str, alias: str) -> str | None:
        """Return STDDEV SQL expression, or None if not supported natively."""

    def get_engine(self) -> Engine:
        """Get or create the SQLAlchemy engine."""
        if self._engine is None:
            self._engine = self.connect()
        return self._engine

    def supports_native_stddev(self) -> bool:
        """Whether this engine supports STDDEV natively in SQL."""
        return True

    def supports_percentiles(self) -> bool:
        """Whether this engine supports approximate percentile functions."""
        return True

    def percentile_sql(self, column: str, quantiles: list[float], aliases: list[str]) -> list[str]:
        """Return SQL expressions for approximate percentiles.

        Default uses approx_quantile (DuckDB/Snowflake compatible).
        Override for engine-specific syntax.
        """
        return [
            f"approx_quantile({column}, {q}) AS {alias}"
            for q, alias in zip(quantiles, aliases)
        ]

    def distinct_mode(self) -> str:
        """Return 'approx' or 'exact' for this engine's distinct counting."""
        return "approx"

    def skewness_sql(self, column: str) -> str | None:
        """Return SKEWNESS SQL expression, or None if not supported."""
        return None

    def kurtosis_sql(self, column: str) -> str | None:
        """Return KURTOSIS SQL expression, or None if not supported."""
        return None

    def correlation_sql(self, col1: str, col2: str) -> str | None:
        """Return CORR(col1, col2) SQL expression, or None if not supported."""
        return None

    def quote_identifier(self, name: str) -> str:
        """Quote a SQL identifier to prevent injection and handle special characters.

        Default uses ANSI double-quotes. Override for engine-specific quoting
        (e.g., backticks for Databricks/MySQL).
        """
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def set_session_params(self, engine: Engine, config: Any) -> None:
        """Set engine-specific session parameters (timeout, query tag, etc.)."""
        pass

    def supports_constraints(self) -> bool:
        """Whether this engine supports constraint introspection via Inspector.

        DuckDB and SQLite have partial support. Snowflake and Databricks
        support full constraint metadata. Override to return False if the
        engine's Inspector methods are unreliable.
        """
        return True
