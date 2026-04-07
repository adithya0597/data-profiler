"""Unit tests for incremental profiling: ProfileStore, delta detection, merge worker."""

import math
import sqlite3
import threading

import pytest

from data_profiler.persistence.profile_store import ProfileStore, _dict_to_profile
from data_profiler.workers.delta_worker import DeltaResult, check_delta, compute_column_hash
from data_profiler.workers.merge_worker import merge_profiles, _safe_min, _safe_max, _safe_add
from data_profiler.workers.schema_worker import ColumnSchema
from data_profiler.workers.stats_worker import ColumnProfile, ProfiledTable


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_col(name, canonical_type="integer", **kwargs):
    defaults = dict(
        engine_type="INTEGER", comment=None, nullable=True, anomalies=[],
    )
    defaults.update(kwargs)
    return ColumnProfile(name=name, canonical_type=canonical_type, **defaults)


def _make_table(name, columns=None, **kwargs):
    defaults = dict(
        comment=None, total_row_count=1000, sampled_row_count=1000,
        columns=columns or [],
    )
    defaults.update(kwargs)
    return ProfiledTable(name=name, **defaults)


def _make_store():
    """Create an in-memory ProfileStore for testing."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    lock = threading.Lock()
    return ProfileStore(conn, lock)


def _make_schema_cols(*names):
    return [ColumnSchema(name=n, engine_type="INTEGER", canonical_type="integer", nullable=True) for n in names]


# ---------------------------------------------------------------------------
# ProfileStore
# ---------------------------------------------------------------------------

class TestProfileStore:
    def test_store_and_load_roundtrip(self):
        store = _make_store()
        col = _make_col("id", approx_distinct=100, null_rate=0.05, mean=50.0, stddev=10.0)
        table = _make_table("users", columns=[col], total_row_count=500)
        store.store_profile("run1", "users", table, column_hash="abc123")

        loaded = store.load_profile("run1", "users")
        assert loaded is not None
        assert loaded.name == "users"
        assert loaded.total_row_count == 500
        assert len(loaded.columns) == 1
        assert loaded.columns[0].name == "id"
        assert loaded.columns[0].approx_distinct == 100

    def test_load_nonexistent_returns_none(self):
        store = _make_store()
        assert store.load_profile("run1", "nonexistent") is None

    def test_get_prior_metadata(self):
        store = _make_store()
        table = _make_table("t", total_row_count=42)
        store.store_profile("r1", "t", table, column_hash="xyz", watermark_value="2026-01-01")

        meta = store.get_prior_metadata("r1", "t")
        assert meta is not None
        assert meta["row_count"] == 42
        assert meta["column_hash"] == "xyz"
        assert meta["watermark_value"] == "2026-01-01"

    def test_get_prior_metadata_missing(self):
        store = _make_store()
        assert store.get_prior_metadata("r1", "missing") is None

    def test_load_all_profiles(self):
        store = _make_store()
        store.store_profile("r1", "a", _make_table("a"))
        store.store_profile("r1", "b", _make_table("b"))
        store.store_profile("r2", "c", _make_table("c"))

        all_r1 = store.load_all_profiles("r1")
        assert len(all_r1) == 2
        assert "a" in all_r1
        assert "b" in all_r1

    def test_get_latest_run_id(self):
        store = _make_store()
        store.store_profile("r1", "t", _make_table("t"), database="db", schema_name="s")
        store.store_profile("r2", "t", _make_table("t"), database="db", schema_name="s")

        latest = store.get_latest_run_id(database="db", schema_name="s")
        assert latest == "r2"

    def test_get_latest_run_id_no_filter(self):
        store = _make_store()
        store.store_profile("r1", "t", _make_table("t"))
        latest = store.get_latest_run_id()
        assert latest == "r1"

    def test_get_latest_run_id_empty(self):
        store = _make_store()
        assert store.get_latest_run_id() is None

    def test_store_replaces_existing(self):
        store = _make_store()
        store.store_profile("r1", "t", _make_table("t", total_row_count=100))
        store.store_profile("r1", "t", _make_table("t", total_row_count=200))

        loaded = store.load_profile("r1", "t")
        assert loaded.total_row_count == 200


# ---------------------------------------------------------------------------
# Column Hash
# ---------------------------------------------------------------------------

class TestColumnHash:
    def test_deterministic(self):
        cols = _make_schema_cols("a", "b", "c")
        h1 = compute_column_hash(cols)
        h2 = compute_column_hash(cols)
        assert h1 == h2

    def test_order_independent(self):
        h1 = compute_column_hash(_make_schema_cols("a", "b"))
        h2 = compute_column_hash(_make_schema_cols("b", "a"))
        assert h1 == h2

    def test_changes_on_schema_change(self):
        h1 = compute_column_hash(_make_schema_cols("a", "b"))
        cols = [
            ColumnSchema(name="a", engine_type="INTEGER", canonical_type="integer", nullable=True),
            ColumnSchema(name="b", engine_type="VARCHAR", canonical_type="string", nullable=True),
        ]
        h2 = compute_column_hash(cols)
        assert h1 != h2

    def test_changes_on_column_add(self):
        h1 = compute_column_hash(_make_schema_cols("a"))
        h2 = compute_column_hash(_make_schema_cols("a", "b"))
        assert h1 != h2


# ---------------------------------------------------------------------------
# Delta Detection
# ---------------------------------------------------------------------------

class TestDeltaDetection:
    def test_new_table(self):
        result = check_delta(
            engine=None, table_name="t", schema=None,
            prior_metadata=None, prior_profile=None,
            watermark_column=None, current_columns=[], quote_fn=None,
        )
        assert result.needs_profiling is True
        assert result.reason == "new_table"

    def test_schema_changed(self):
        cols = _make_schema_cols("a", "b")
        prior_meta = {"column_hash": "old_hash", "row_count": 100, "watermark_value": None}
        result = check_delta(
            engine=None, table_name="t", schema=None,
            prior_metadata=prior_meta, prior_profile=None,
            watermark_column=None, current_columns=cols, quote_fn=None,
        )
        assert result.needs_profiling is True
        assert result.reason == "schema_changed"

    def test_unchanged(self, tmp_path):
        """Use a real SQLite DB to test row count check path."""
        import sqlalchemy
        db_path = str(tmp_path / "test.db")
        eng = sqlalchemy.create_engine(f"sqlite:///{db_path}")
        with eng.connect() as conn:
            conn.execute(sqlalchemy.text("CREATE TABLE t (a INTEGER, b TEXT)"))
            conn.execute(sqlalchemy.text("INSERT INTO t VALUES (1, 'x'), (2, 'y')"))
            conn.commit()

        cols = [
            ColumnSchema(name="a", engine_type="INTEGER", canonical_type="integer", nullable=True),
            ColumnSchema(name="b", engine_type="TEXT", canonical_type="string", nullable=True),
        ]
        col_hash = compute_column_hash(cols)
        prior_meta = {"column_hash": col_hash, "row_count": 2, "watermark_value": None}

        result = check_delta(
            engine=eng, table_name="t", schema=None,
            prior_metadata=prior_meta, prior_profile=_make_table("t"),
            watermark_column=None, current_columns=cols,
        )
        assert result.needs_profiling is False
        assert result.reason == "unchanged"
        assert result.prior_profile is not None

    def test_row_count_changed(self, tmp_path):
        import sqlalchemy
        db_path = str(tmp_path / "test.db")
        eng = sqlalchemy.create_engine(f"sqlite:///{db_path}")
        with eng.connect() as conn:
            conn.execute(sqlalchemy.text("CREATE TABLE t (a INTEGER)"))
            conn.execute(sqlalchemy.text("INSERT INTO t VALUES (1), (2), (3)"))
            conn.commit()

        cols = _make_schema_cols("a")
        col_hash = compute_column_hash(cols)
        prior_meta = {"column_hash": col_hash, "row_count": 2, "watermark_value": None}

        result = check_delta(
            engine=eng, table_name="t", schema=None,
            prior_metadata=prior_meta, prior_profile=None,
            watermark_column=None, current_columns=cols,
        )
        assert result.needs_profiling is True
        assert result.reason == "row_count_changed"

    def test_watermark_advanced(self, tmp_path):
        import sqlalchemy
        db_path = str(tmp_path / "test.db")
        eng = sqlalchemy.create_engine(f"sqlite:///{db_path}")
        with eng.connect() as conn:
            conn.execute(sqlalchemy.text("CREATE TABLE t (a INTEGER, ts TEXT)"))
            conn.execute(sqlalchemy.text("INSERT INTO t VALUES (1, '2026-01-01'), (2, '2026-02-01')"))
            conn.commit()

        cols = [
            ColumnSchema(name="a", engine_type="INTEGER", canonical_type="integer", nullable=True),
            ColumnSchema(name="ts", engine_type="TEXT", canonical_type="string", nullable=True),
        ]
        col_hash = compute_column_hash(cols)
        prior_meta = {"column_hash": col_hash, "row_count": 2, "watermark_value": "2026-01-01"}
        prior_profile = _make_table("t")

        result = check_delta(
            engine=eng, table_name="t", schema=None,
            prior_metadata=prior_meta, prior_profile=prior_profile,
            watermark_column="ts", current_columns=cols,
        )
        assert result.needs_profiling is True
        assert result.reason == "watermark_advanced"
        assert result.prior_profile is prior_profile
        assert result.watermark_filter is not None
        assert "'2026-01-01'" in result.watermark_filter


# ---------------------------------------------------------------------------
# Merge Worker
# ---------------------------------------------------------------------------

class TestMergeWorker:
    def test_null_counts_additive(self):
        prior = _make_table("t", columns=[_make_col("a", null_count=10)], total_row_count=100)
        delta = _make_table("t", columns=[_make_col("a", null_count=5)], total_row_count=50)
        merged = merge_profiles(prior, delta)
        assert merged.total_row_count == 150
        assert merged.columns[0].null_count == 15

    def test_min_max_correct(self):
        prior = _make_table("t", columns=[_make_col("a", min=5, max=100)], total_row_count=100)
        delta = _make_table("t", columns=[_make_col("a", min=1, max=80)], total_row_count=50)
        merged = merge_profiles(prior, delta)
        assert merged.columns[0].min == 1
        assert merged.columns[0].max == 100

    def test_weighted_mean(self):
        prior = _make_table("t", columns=[_make_col("a", mean=10.0, variance=0.0)], total_row_count=100)
        delta = _make_table("t", columns=[_make_col("a", mean=20.0, variance=0.0)], total_row_count=100)
        merged = merge_profiles(prior, delta)
        assert merged.columns[0].mean == pytest.approx(15.0)

    def test_welford_stddev(self):
        # prior: 100 rows, mean=10, variance=4 (stddev=2)
        # delta: 100 rows, mean=10, variance=4 (stddev=2)
        # same mean → combined variance should also be 4
        prior = _make_table("t", columns=[_make_col("a", mean=10.0, variance=4.0, stddev=2.0)], total_row_count=100)
        delta = _make_table("t", columns=[_make_col("a", mean=10.0, variance=4.0, stddev=2.0)], total_row_count=100)
        merged = merge_profiles(prior, delta)
        assert merged.columns[0].variance == pytest.approx(4.0)
        assert merged.columns[0].stddev == pytest.approx(2.0)

    def test_welford_stddev_different_means(self):
        # prior: 50 rows, mean=0, variance=0
        # delta: 50 rows, mean=10, variance=0
        # combined mean=5, M2 = 0 + 0 + (10-0)² * 50*50/100 = 2500
        # combined variance = 2500/100 = 25, stddev = 5
        prior = _make_table("t", columns=[_make_col("a", mean=0.0, variance=0.0)], total_row_count=50)
        delta = _make_table("t", columns=[_make_col("a", mean=10.0, variance=0.0)], total_row_count=50)
        merged = merge_profiles(prior, delta)
        assert merged.columns[0].mean == pytest.approx(5.0)
        assert merged.columns[0].variance == pytest.approx(25.0)
        assert merged.columns[0].stddev == pytest.approx(5.0)

    def test_sum_additive(self):
        prior = _make_table("t", columns=[_make_col("a", sum=100)], total_row_count=100)
        delta = _make_table("t", columns=[_make_col("a", sum=50)], total_row_count=50)
        merged = merge_profiles(prior, delta)
        assert merged.columns[0].sum == 150

    def test_approx_distinct_conservative(self):
        prior = _make_table("t", columns=[_make_col("a", approx_distinct=80)], total_row_count=100)
        delta = _make_table("t", columns=[_make_col("a", approx_distinct=50)], total_row_count=50)
        merged = merge_profiles(prior, delta)
        assert merged.columns[0].approx_distinct == 80

    def test_new_column_included(self):
        prior = _make_table("t", columns=[_make_col("a")], total_row_count=100)
        delta = _make_table("t", columns=[_make_col("a"), _make_col("b")], total_row_count=50)
        merged = merge_profiles(prior, delta)
        col_names = [c.name for c in merged.columns]
        assert "a" in col_names
        assert "b" in col_names

    def test_dropped_column_omitted(self):
        prior = _make_table("t", columns=[_make_col("a"), _make_col("b")], total_row_count=100)
        delta = _make_table("t", columns=[_make_col("a")], total_row_count=50)
        merged = merge_profiles(prior, delta)
        col_names = [c.name for c in merged.columns]
        assert "a" in col_names
        assert "b" not in col_names

    def test_pattern_union(self):
        prior = _make_table("t", columns=[_make_col("a", patterns=["p1", "p2"])], total_row_count=100)
        delta = _make_table("t", columns=[_make_col("a", patterns=["p2", "p3"])], total_row_count=50)
        merged = merge_profiles(prior, delta)
        assert set(merged.columns[0].patterns) == {"p1", "p2", "p3"}

    def test_anomaly_union(self):
        prior = _make_table("t", columns=[_make_col("a", anomalies=["high_null"])], total_row_count=100)
        delta = _make_table("t", columns=[_make_col("a", anomalies=["near_constant"])], total_row_count=50)
        merged = merge_profiles(prior, delta)
        assert set(merged.columns[0].anomalies) == {"high_null", "near_constant"}

    def test_duration_additive(self):
        prior = _make_table("t", columns=[], total_row_count=100, duration_seconds=5.0)
        delta = _make_table("t", columns=[], total_row_count=50, duration_seconds=3.0)
        merged = merge_profiles(prior, delta)
        assert merged.duration_seconds == 8.0

    def test_sampled_row_count_additive(self):
        prior = _make_table("t", columns=[], total_row_count=100, sampled_row_count=80)
        delta = _make_table("t", columns=[], total_row_count=50, sampled_row_count=50)
        merged = merge_profiles(prior, delta)
        assert merged.sampled_row_count == 130


# ---------------------------------------------------------------------------
# Safe Helpers
# ---------------------------------------------------------------------------

class TestSafeHelpers:
    def test_safe_min_both_none(self):
        assert _safe_min(None, None) is None

    def test_safe_min_one_none(self):
        assert _safe_min(None, 5) == 5
        assert _safe_min(3, None) == 3

    def test_safe_min_both_values(self):
        assert _safe_min(3, 5) == 3

    def test_safe_max_both_none(self):
        assert _safe_max(None, None) is None

    def test_safe_max_one_none(self):
        assert _safe_max(None, 5) == 5
        assert _safe_max(3, None) == 3

    def test_safe_max_both_values(self):
        assert _safe_max(3, 5) == 5

    def test_safe_add_both_none(self):
        assert _safe_add(None, None) is None

    def test_safe_add_one_none(self):
        assert _safe_add(None, 5) == 5
        assert _safe_add(3, None) == 3

    def test_safe_add_both_values(self):
        assert _safe_add(3, 5) == 8


# ---------------------------------------------------------------------------
# Dict-to-Profile Roundtrip
# ---------------------------------------------------------------------------

class TestDictRoundtrip:
    def test_dict_to_profile_roundtrip(self):
        import dataclasses
        col = _make_col("id", approx_distinct=42, mean=10.5, patterns=["uuid"])
        table = _make_table("users", columns=[col], total_row_count=100)

        d = dataclasses.asdict(table)
        restored = _dict_to_profile(d)
        assert restored.name == "users"
        assert restored.total_row_count == 100
        assert len(restored.columns) == 1
        assert restored.columns[0].name == "id"
        assert restored.columns[0].approx_distinct == 42
        assert restored.columns[0].mean == 10.5
        assert restored.columns[0].patterns == ["uuid"]
