"""Unit tests for type-aware aggregate dispatch."""

import pytest
from data_profiler.workers.stats_worker import AGGREGATE_MAP, _build_select_exprs, _build_distinct_exprs
from data_profiler.workers.schema_worker import ColumnSchema
from data_profiler.adapters.duckdb import DuckDBAdapter
from data_profiler.adapters.sqlite import SQLiteAdapter
from data_profiler.config import ProfilerConfig


def _config(**overrides) -> ProfilerConfig:
    defaults = dict(engine="duckdb", dsn="duckdb:///:memory:", sample_size=1000, concurrency=1)
    defaults.update(overrides)
    return ProfilerConfig(**defaults)


class TestAggregateMap:
    """Verify that each canonical type has the correct aggregates."""

    def test_numeric_types_have_min_max_mean(self):
        for ct in ("integer", "float"):
            suffixes = [s for _, s in AGGREGATE_MAP[ct]]
            assert "non_null" in suffixes
            assert "min" in suffixes
            assert "max" in suffixes
            assert "mean" in suffixes
            assert "zero_count" in suffixes
            assert "negative_count" in suffixes

    def test_string_has_max_length_and_avg_length(self):
        suffixes = [s for _, s in AGGREGATE_MAP["string"]]
        assert "min_length" in suffixes
        assert "max_length" in suffixes
        assert "avg_length" in suffixes
        assert "whitespace_count" in suffixes
        assert "leading_trailing_whitespace_count" in suffixes
        assert "mean" not in suffixes  # No AVG on strings

    def test_float_has_infinite_count(self):
        suffixes = [s for _, s in AGGREGATE_MAP["float"]]
        assert "infinite_count" in suffixes

    def test_boolean_has_true_count(self):
        suffixes = [s for _, s in AGGREGATE_MAP["boolean"]]
        assert "true_count" in suffixes
        assert "mean" not in suffixes

    def test_date_has_no_mean(self):
        for ct in ("date", "datetime"):
            suffixes = [s for _, s in AGGREGATE_MAP[ct]]
            assert "mean" not in suffixes
            assert "min" in suffixes
            assert "max" in suffixes

    def test_binary_has_only_count(self):
        suffixes = [s for _, s in AGGREGATE_MAP["binary"]]
        assert suffixes == ["non_null"]

    def test_unknown_has_only_count(self):
        suffixes = [s for _, s in AGGREGATE_MAP["unknown"]]
        assert suffixes == ["non_null"]


class TestBuildSelectExprs:
    """Verify SQL expression generation per engine."""

    def test_duckdb_integer_includes_stddev_and_percentiles(self):
        adapter = DuckDBAdapter("duckdb:///:memory:")
        cols = [ColumnSchema("age", "INTEGER", "integer", True)]
        exprs = _build_select_exprs(cols, adapter, _config(), full_scan=True)
        expr_str = " ".join(exprs)
        assert 'stddev("age")' in expr_str
        assert 'approx_count_distinct("age")' in expr_str
        assert 'approx_quantile("age", 0.05)' in expr_str  # p5
        assert 'approx_quantile("age", 0.25)' in expr_str  # p25
        assert 'approx_quantile("age", 0.5)' in expr_str   # median
        assert 'approx_quantile("age", 0.75)' in expr_str  # p75
        assert 'approx_quantile("age", 0.95)' in expr_str  # p95

    def test_duckdb_sampled_no_distinct_in_main_query(self):
        """When not full_scan, distinct should NOT be in the main select."""
        adapter = DuckDBAdapter("duckdb:///:memory:")
        cols = [ColumnSchema("age", "INTEGER", "integer", True)]
        exprs = _build_select_exprs(cols, adapter, _config(), full_scan=False)
        expr_str = " ".join(exprs)
        assert "approx_count_distinct" not in expr_str
        assert "COUNT(DISTINCT" not in expr_str

    def test_distinct_exprs_separate_query(self):
        """Distinct expressions are built by _build_distinct_exprs for full-table query."""
        adapter = DuckDBAdapter("duckdb:///:memory:")
        cols = [ColumnSchema("age", "INTEGER", "integer", True)]
        d_exprs = _build_distinct_exprs(cols, adapter, _config())
        expr_str = " ".join(d_exprs)
        assert 'approx_count_distinct("age")' in expr_str

    def test_sqlite_integer_no_stddev(self):
        adapter = SQLiteAdapter("sqlite:///:memory:")
        cols = [ColumnSchema("age", "INTEGER", "integer", True)]
        exprs = _build_select_exprs(cols, adapter, _config(engine="sqlite", dsn="sqlite:///:memory:"), full_scan=True)
        expr_str = " ".join(exprs)
        assert "stddev" not in expr_str.lower()
        assert 'COUNT(DISTINCT "age")' in expr_str  # exact, not HLL
        # SQLite does not support percentiles
        assert "approx_quantile" not in expr_str

    def test_string_column_has_avg_length(self):
        adapter = DuckDBAdapter("duckdb:///:memory:")
        cols = [ColumnSchema("name", "VARCHAR", "string", True)]
        exprs = _build_select_exprs(cols, adapter, _config(), full_scan=True)
        expr_str = " ".join(exprs)
        assert 'AVG(LENGTH("name"))' in expr_str
        assert 'MAX(LENGTH("name"))' in expr_str

    def test_boolean_column_has_cast_sum(self):
        adapter = DuckDBAdapter("duckdb:///:memory:")
        cols = [ColumnSchema("active", "BOOLEAN", "boolean", True)]
        exprs = _build_select_exprs(cols, adapter, _config(), full_scan=True)
        expr_str = " ".join(exprs)
        assert 'SUM(CAST("active" AS INT))' in expr_str

    def test_exact_distinct_flag_on_full_scan(self):
        adapter = DuckDBAdapter("duckdb:///:memory:")
        cols = [ColumnSchema("id", "INTEGER", "integer", True)]
        exprs = _build_select_exprs(cols, adapter, _config(exact_distinct=True), full_scan=True)
        expr_str = " ".join(exprs)
        assert 'COUNT(DISTINCT "id")' in expr_str
        assert "approx_count_distinct" not in expr_str

    def test_numeric_has_zero_and_negative_count(self):
        adapter = DuckDBAdapter("duckdb:///:memory:")
        cols = [ColumnSchema("amount", "INTEGER", "integer", True)]
        exprs = _build_select_exprs(cols, adapter, _config(), full_scan=True)
        expr_str = " ".join(exprs)
        assert 'CASE WHEN "amount" = 0 THEN 1 ELSE 0 END' in expr_str
        assert 'CASE WHEN "amount" < 0 THEN 1 ELSE 0 END' in expr_str
