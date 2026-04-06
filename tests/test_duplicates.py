"""Unit tests for duplicate row detection."""

import pytest
from sqlalchemy import create_engine, text

from data_profiler.adapters.duckdb import DuckDBAdapter
from data_profiler.config import ProfilerConfig
from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
from data_profiler.workers.stats_worker import profile_table


def _config(**overrides) -> ProfilerConfig:
    defaults = dict(
        engine="duckdb",
        dsn="duckdb:///:memory:",
        sample_size=0,  # full scan
        concurrency=1,
        detect_duplicates=True,
    )
    defaults.update(overrides)
    return ProfilerConfig(**defaults)


def _schema(table_name: str) -> TableSchema:
    """Build schema manually — SQLAlchemy reflection fails on DuckDB in-memory DBs."""
    return TableSchema(name=table_name, columns=[
        ColumnSchema(name="id", engine_type="INTEGER", canonical_type="integer", nullable=True),
        ColumnSchema(name="name", engine_type="VARCHAR", canonical_type="string", nullable=True),
    ])


@pytest.fixture
def dup_engine():
    """Create a DuckDB in-memory engine with a table containing duplicates."""
    engine = create_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE with_dups (id INTEGER, name VARCHAR)"))
        conn.execute(text(
            "INSERT INTO with_dups VALUES "
            "(1, 'alice'), (2, 'bob'), (3, 'carol'), "
            "(1, 'alice'), (2, 'bob')"  # 2 duplicate rows
        ))
        conn.commit()

        conn.execute(text("CREATE TABLE no_dups (id INTEGER, name VARCHAR)"))
        conn.execute(text(
            "INSERT INTO no_dups VALUES "
            "(1, 'alice'), (2, 'bob'), (3, 'carol')"
        ))
        conn.commit()
    return engine


class TestDuplicateDetection:
    def test_detects_duplicates(self, dup_engine):
        adapter = DuckDBAdapter("duckdb:///:memory:")
        adapter._engine = dup_engine
        config = _config()
        result = profile_table(adapter, _schema("with_dups"), config)
        assert result.duplicate_row_count == 2
        assert result.duplicate_rate == pytest.approx(2 / 5)

    def test_no_duplicates(self, dup_engine):
        adapter = DuckDBAdapter("duckdb:///:memory:")
        adapter._engine = dup_engine
        config = _config()
        result = profile_table(adapter, _schema("no_dups"), config)
        assert result.duplicate_row_count == 0
        assert result.duplicate_rate == 0.0

    def test_disabled_by_config(self, dup_engine):
        adapter = DuckDBAdapter("duckdb:///:memory:")
        adapter._engine = dup_engine
        config = _config(detect_duplicates=False)
        result = profile_table(adapter, _schema("with_dups"), config)
        assert result.duplicate_row_count == 0  # Not computed

    def test_skipped_for_wide_tables(self, dup_engine):
        adapter = DuckDBAdapter("duckdb:///:memory:")
        adapter._engine = dup_engine
        config = _config(duplicate_column_limit=1)  # Table has 2 cols
        result = profile_table(adapter, _schema("with_dups"), config)
        assert result.duplicate_row_count == 0  # Skipped
