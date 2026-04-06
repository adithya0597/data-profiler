"""Cross-engine consistency tests: same dataset profiled via DuckDB and SQLite must agree."""

from __future__ import annotations

import math
import pytest

from data_profiler.adapters.duckdb import DuckDBAdapter
from data_profiler.adapters.sqlite import SQLiteAdapter
from data_profiler.config import ProfilerConfig
from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
from data_profiler.workers.stats_worker import profile_table


def _cross_test_schema() -> TableSchema:
    """Known schema for cross_test table (avoids SQLAlchemy reflection issues)."""
    return TableSchema(
        name="cross_test",
        columns=[
            ColumnSchema("id", "INTEGER", "integer", True),
            ColumnSchema("name", "VARCHAR", "string", True),
            ColumnSchema("score", "DOUBLE", "float", True),
            ColumnSchema("active", "INTEGER", "integer", True),
        ],
    )


# ---------------------------------------------------------------------------
# Shared fixture: create an identical table in both DuckDB and SQLite
# ---------------------------------------------------------------------------

ROWS = [
    (1,  "alice",   10.5,  1),
    (2,  "bob",     20.0,  0),
    (3,  "carol",   30.75, 1),
    (4,  None,      None,  0),
    (5,  "eve",     50.0,  1),
    (6,  "",        60.25, 0),
    (7,  "grace",   70.0,  None),
    (8,  "heidi",   80.5,  1),
    (9,  None,      90.0,  0),
    (10, "ivan",    100.0, 1),
]
# id: integer, name: string, score: float, active: boolean


@pytest.fixture()
def duckdb_engine(tmp_path):
    import duckdb
    from sqlalchemy import create_engine
    db_path = str(tmp_path / "cross.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("""
        CREATE TABLE cross_test (
            id INTEGER, name VARCHAR, score DOUBLE, active INTEGER
        )
    """)
    for row in ROWS:
        vals = tuple(row)
        conn.execute("INSERT INTO cross_test VALUES (?, ?, ?, ?)", vals)
    conn.close()
    return create_engine(f"duckdb:///{db_path}")


@pytest.fixture()
def sqlite_engine(tmp_path):
    from sqlalchemy import create_engine, text
    engine = create_engine(f"sqlite:///{tmp_path}/cross.db")
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE cross_test (
                id INTEGER, name TEXT, score REAL, active INTEGER
            )
        """))
        for row in ROWS:
            conn.execute(text("INSERT INTO cross_test VALUES (:a, :b, :c, :d)"),
                         {"a": row[0], "b": row[1], "c": row[2], "d": row[3]})
        conn.commit()
    return engine


@pytest.fixture()
def duckdb_profile(duckdb_engine):
    adapter = DuckDBAdapter.__new__(DuckDBAdapter)
    adapter._engine = duckdb_engine
    adapter._dsn = "duckdb:///cross.duckdb"
    config = ProfilerConfig(
        engine="duckdb", dsn="duckdb:///:memory:",
        sample_size=0, concurrency=1, stats_depth="full",
        enable_histogram=False, enable_benford=False,
        enable_correlation=False, enable_patterns=False,
        detect_duplicates=False,
    )
    return profile_table(adapter, _cross_test_schema(), config)


@pytest.fixture()
def sqlite_profile(sqlite_engine):
    adapter = SQLiteAdapter.__new__(SQLiteAdapter)
    adapter._engine = sqlite_engine
    adapter._dsn = "sqlite:///cross.db"
    config = ProfilerConfig(
        engine="sqlite", dsn="sqlite:///:memory:",
        sample_size=0, concurrency=1, stats_depth="full",
        enable_histogram=False, enable_benford=False,
        enable_correlation=False, enable_patterns=False,
        detect_duplicates=False,
    )
    return profile_table(adapter, _cross_test_schema(), config)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def col(profile, name):
    return next(c for c in profile.columns if c.name == name)


# ---------------------------------------------------------------------------
# Tests: exact matches (integer arithmetic, nulls, booleans)
# ---------------------------------------------------------------------------

class TestExactMatches:
    def test_row_count_matches(self, duckdb_profile, sqlite_profile):
        """Both engines must agree on total and sampled row counts."""
        assert duckdb_profile.total_row_count == sqlite_profile.total_row_count == 10
        assert duckdb_profile.sampled_row_count == sqlite_profile.sampled_row_count == 10

    def test_null_count_integer_matches(self, duckdb_profile, sqlite_profile):
        """Integer column 'id' has no nulls — both engines must agree."""
        d = col(duckdb_profile, "id")
        s = col(sqlite_profile, "id")
        assert d.null_count == s.null_count == 0
        assert d.null_rate == s.null_rate == 0.0

    def test_null_count_string_matches(self, duckdb_profile, sqlite_profile):
        """String column 'name' has 2 nulls — both engines must agree."""
        d = col(duckdb_profile, "name")
        s = col(sqlite_profile, "name")
        assert d.null_count == s.null_count == 2

    def test_null_count_float_matches(self, duckdb_profile, sqlite_profile):
        """Float column 'score' has 1 null — both engines must agree."""
        d = col(duckdb_profile, "score")
        s = col(sqlite_profile, "score")
        assert d.null_count == s.null_count == 1

    def test_min_max_integer_matches(self, duckdb_profile, sqlite_profile):
        """min=1, max=10 for integer id column."""
        d = col(duckdb_profile, "id")
        s = col(sqlite_profile, "id")
        assert d.min == s.min == 1
        assert d.max == s.max == 10

    def test_min_max_float_matches(self, duckdb_profile, sqlite_profile):
        """min=10.5, max=100.0 for float score column."""
        d = col(duckdb_profile, "score")
        s = col(sqlite_profile, "score")
        assert d.min == s.min
        assert d.max == s.max

    def test_empty_count_string_matches(self, duckdb_profile, sqlite_profile):
        """One empty string in 'name' — both engines must agree."""
        d = col(duckdb_profile, "name")
        s = col(sqlite_profile, "name")
        assert d.empty_count == s.empty_count == 1


# ---------------------------------------------------------------------------
# Tests: approximate matches (floating point, within tolerance)
# ---------------------------------------------------------------------------

class TestApproximateMatches:
    MEAN_TOL = 0.001   # 0.1% relative tolerance
    STDDEV_TOL = 0.01  # 1% relative tolerance

    def test_mean_integer_approx(self, duckdb_profile, sqlite_profile):
        """Mean of id [1..10] = 5.5. Both engines within 0.1%."""
        d = col(duckdb_profile, "id")
        s = col(sqlite_profile, "id")
        assert d.mean is not None and s.mean is not None
        assert abs(d.mean - 5.5) < 0.01
        # Cross-engine: relative difference
        rel_diff = abs(d.mean - s.mean) / max(abs(d.mean), 1e-10)
        assert rel_diff < self.MEAN_TOL

    def test_mean_float_approx(self, duckdb_profile, sqlite_profile):
        """Mean of score [10.5, 20.0, 30.75, 50.0, 60.25, 70.0, 80.5, 90.0, 100.0]
        (9 non-null values). Both engines within 0.1%."""
        d = col(duckdb_profile, "score")
        s = col(sqlite_profile, "score")
        assert d.mean is not None and s.mean is not None
        expected = (10.5 + 20.0 + 30.75 + 50.0 + 60.25 + 70.0 + 80.5 + 90.0 + 100.0) / 9
        assert abs(d.mean - expected) < 0.01
        rel_diff = abs(d.mean - s.mean) / max(abs(d.mean), 1e-10)
        assert rel_diff < self.MEAN_TOL

    def test_sum_integer_matches(self, duckdb_profile, sqlite_profile):
        """Sum of id [1..10] = 55. Both engines must agree within 0.01%."""
        d = col(duckdb_profile, "id")
        s = col(sqlite_profile, "id")
        assert d.sum is not None and s.sum is not None
        assert abs(d.sum - 55.0) < 0.1
        assert abs(d.sum - s.sum) < 0.1

    def test_stddev_integer_approx(self, duckdb_profile, sqlite_profile):
        """Stddev of id [1..10] ≈ 3.0277. Both engines within 1%."""
        d = col(duckdb_profile, "id")
        s = col(sqlite_profile, "id")
        assert d.stddev is not None and s.stddev is not None
        expected_stddev = math.sqrt(sum((i - 5.5) ** 2 for i in range(1, 11)) / 9)
        assert abs(d.stddev - expected_stddev) < 0.05
        rel_diff = abs(d.stddev - s.stddev) / max(abs(d.stddev), 1e-10)
        assert rel_diff < self.STDDEV_TOL

    def test_zero_count_matches(self, duckdb_profile, sqlite_profile):
        """id column has no zero values; both engines must agree."""
        d = col(duckdb_profile, "id")
        s = col(sqlite_profile, "id")
        assert d.zero_count == s.zero_count == 0


# ---------------------------------------------------------------------------
# Tests: string column specifics
# ---------------------------------------------------------------------------

class TestStringColumnConsistency:
    def test_min_max_length_matches(self, duckdb_profile, sqlite_profile):
        """min_length, max_length, avg_length for 'name' must be consistent."""
        d = col(duckdb_profile, "name")
        s = col(sqlite_profile, "name")
        assert d.min_length == s.min_length  # empty string = 0
        assert d.max_length == s.max_length  # "heidi" = 5

    def test_approx_distinct_consistent(self, duckdb_profile, sqlite_profile):
        """Both engines should report similar distinct counts for 'name'."""
        d = col(duckdb_profile, "name")
        s = col(sqlite_profile, "name")
        # 8 non-null + 1 empty = 8 distinct non-null values
        # Allow small variance between HLL (DuckDB) and exact (SQLite)
        assert abs(d.approx_distinct - s.approx_distinct) <= 1


# ---------------------------------------------------------------------------
# Tests: mathematical verification against Python reference
# ---------------------------------------------------------------------------

class TestMathematicalVerification:
    """Verify stats against Python-computed reference values from known data."""

    # id column: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] — all non-null
    ID_VALUES = list(range(1, 11))
    # score column: 9 non-null values
    SCORE_VALUES = [10.5, 20.0, 30.75, 50.0, 60.25, 70.0, 80.5, 90.0, 100.0]

    def test_null_rate_formula(self, duckdb_profile):
        """null_rate == null_count / sampled_row_count."""
        for cp in duckdb_profile.columns:
            expected = cp.null_count / duckdb_profile.sampled_row_count
            assert abs(cp.null_rate - expected) < 1e-10

    def test_mean_formula(self, duckdb_profile):
        """mean == sum(values) / count(non_null)."""
        import statistics
        d = col(duckdb_profile, "id")
        expected = statistics.mean(self.ID_VALUES)
        assert abs(d.mean - expected) < 0.001

    def test_stddev_formula(self, duckdb_profile):
        """stddev matches Python statistics.stdev (sample, N-1 denominator)."""
        import statistics
        d = col(duckdb_profile, "id")
        expected = statistics.stdev(self.ID_VALUES)
        assert abs(d.stddev - expected) < 0.01

    def test_min_max_formula(self, duckdb_profile):
        """min and max match Python min/max."""
        d = col(duckdb_profile, "id")
        assert d.min == min(self.ID_VALUES)
        assert d.max == max(self.ID_VALUES)

    def test_sum_formula(self, duckdb_profile):
        """sum matches Python sum."""
        d = col(duckdb_profile, "id")
        assert abs(d.sum - sum(self.ID_VALUES)) < 0.001
