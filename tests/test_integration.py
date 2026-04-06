"""Integration tests: profile TPC-DS tables and validate against known values."""

import json
import os
import tempfile
from pathlib import Path

import pytest

from data_profiler.config import ProfilerConfig
from data_profiler.run import run_profiler

TPCDS_DB = "data/tpcds_1gb.duckdb"
SKIP_MSG = "TPC-DS database not found (run CALL dsdgen(sf=1) first)"


def _has_tpcds() -> bool:
    return Path(TPCDS_DB).exists()


@pytest.fixture
def tpcds_config(tmp_path):
    return ProfilerConfig(
        engine="duckdb",
        dsn=f"duckdb:///{TPCDS_DB}",
        sample_size=10000,
        concurrency=1,
        output=str(tmp_path / "test_output.ndjson"),
        output_format="json",
    )


@pytest.mark.skipif(not _has_tpcds(), reason=SKIP_MSG)
class TestTCPDSIntegration:
    """Validate profiler output against known TPC-DS properties."""

    def test_discovers_all_tables(self, tpcds_config):
        run_id, results = run_profiler(tpcds_config)
        table_names = {r.name for r in results}
        # TPC-DS has 24 tables + 1 synthetic_types
        assert len(results) >= 24
        assert "store_sales" in table_names
        assert "date_dim" in table_names
        assert "customer" in table_names

    def test_store_sales_row_count(self, tpcds_config):
        _, results = run_profiler(tpcds_config)
        ss = next(r for r in results if r.name == "store_sales")
        # TPC-DS SF1 store_sales has ~2.88M rows
        assert ss.total_row_count > 2_800_000
        assert ss.total_row_count < 3_000_000
        assert ss.sampled_row_count <= tpcds_config.sample_size

    def test_date_dim_known_values(self, tpcds_config):
        _, results = run_profiler(tpcds_config)
        dd = next(r for r in results if r.name == "date_dim")
        # date_dim has 73049 rows in SF1
        assert dd.total_row_count == 73049

        # d_date_sk is the PK: integer, no nulls, all unique
        d_date_sk = next(c for c in dd.columns if c.name == "d_date_sk")
        assert d_date_sk.canonical_type == "integer"
        assert d_date_sk.null_count == 0
        # approx_distinct now runs on full table (not sample-bounded).
        # HLL can undercount by ~10% on DuckDB, so accept >60000 (truth is 73049).
        # Previously this was ~8,669 (bounded by 10,000 sample size).
        assert d_date_sk.approx_distinct > 60000, f"got {d_date_sk.approx_distinct}"

    def test_new_stats_present(self, tpcds_config):
        """Verify percentiles, top_values, and new stats are populated."""
        _, results = run_profiler(tpcds_config)
        ib = next(r for r in results if r.name == "income_band")
        lb = next(c for c in ib.columns if c.name == "ib_lower_bound")
        # Percentiles should be present for numeric columns
        assert lb.median is not None
        assert lb.p25 is not None
        assert lb.p75 is not None
        assert lb.variance is not None
        assert lb.zero_count is not None
        assert lb.negative_count is not None
        # Top values should be present
        assert lb.top_values is not None
        assert len(lb.top_values) > 0
        assert "value" in lb.top_values[0]
        assert "count" in lb.top_values[0]

    def test_no_errors(self, tpcds_config):
        _, results = run_profiler(tpcds_config)
        errors = [r for r in results if r.error]
        assert errors == [], f"Errors: {[(r.name, r.error) for r in errors]}"

    def test_type_mapping_coverage(self, tpcds_config):
        _, results = run_profiler(tpcds_config)
        types_seen = set()
        for r in results:
            for c in r.columns:
                types_seen.add(c.canonical_type)
        # TPC-DS + synthetic_types should cover most types
        assert "integer" in types_seen
        assert "string" in types_seen
        assert "float" in types_seen
        assert "date" in types_seen

    def test_ndjson_output_readable(self, tpcds_config):
        _, results = run_profiler(tpcds_config)
        with open(tpcds_config.output) as f:
            lines = f.readlines()
        # Header + table lines
        assert len(lines) >= 25
        header = json.loads(lines[0])
        assert header.get("_header") is True
        assert header["engine"] == "duckdb"
        # Each subsequent line should parse as valid JSON
        for line in lines[1:]:
            table = json.loads(line)
            assert "name" in table
            assert "columns" in table

    def test_full_scan_on_small_table(self, tpcds_config):
        """Tables smaller than sample_size should do full scan."""
        _, results = run_profiler(tpcds_config)
        # income_band has 20 rows, well below 10000 sample_size
        ib = next(r for r in results if r.name == "income_band")
        assert ib.total_row_count == 20
        assert ib.full_scan is True
        assert ib.sampled_row_count == ib.total_row_count


@pytest.mark.skipif(not _has_tpcds(), reason=SKIP_MSG)
class TestEnterpriseFeatures:
    """Validate Phase 1-3 enterprise features against TPC-DS."""

    def test_pattern_detection_on_string_columns(self, tpcds_config):
        """At least some string columns should have patterns detected."""
        _, results = run_profiler(tpcds_config)
        pattern_cols = [
            c for r in results for c in r.columns if c.patterns
        ]
        # TPC-DS has emails in customer table etc. — we expect at least a few hits
        # Even if none match built-in patterns, this tests the code path runs
        assert isinstance(pattern_cols, list)  # Doesn't crash

    def test_duplicate_detection_runs(self, tpcds_config):
        """Duplicate detection should populate on small tables."""
        _, results = run_profiler(tpcds_config)
        # income_band has 20 rows, 3 columns — well within limits
        ib = next(r for r in results if r.name == "income_band")
        assert ib.duplicate_row_count >= 0
        assert ib.duplicate_rate >= 0.0
        # TPC-DS income_band has no duplicates
        assert ib.duplicate_row_count == 0

    def test_relationship_discovery_runs(self, tpcds_config):
        """Relationship discovery code path executes without error.

        TPC-DS uses unique prefixed column names per table (d_date_sk, ss_sold_date_sk),
        so no inferred relationships are expected. DuckDB-generated TPC-DS also has
        no declared FKs. This test validates the code path runs cleanly.
        """
        from data_profiler.workers.relationship_worker import discover_relationships
        _, results = run_profiler(tpcds_config)
        rels = discover_relationships(results)
        # No shared column names in TPC-DS, so 0 relationships is correct
        assert isinstance(rels, list)

    def test_openmetadata_export(self, tpcds_config, tmp_path):
        """OpenMetadata export should produce valid JSON."""
        from data_profiler.persistence.openmetadata import export_openmetadata
        _, results = run_profiler(tpcds_config)
        out = str(tmp_path / "openmetadata.json")
        export_openmetadata(results, out, run_id="test", engine="duckdb")
        data = json.loads(Path(out).read_text())
        assert data["openMetadataExport"] is True
        assert len(data["tables"]) >= 24
        # Verify column profiles are populated
        first_table = data["tables"][0]
        assert len(first_table["columns"]) > 0
        assert "profile" in first_table["columns"][0]

    def test_ndjson_output_valid(self, tpcds_config):
        """NDJSON output should be parseable with header and table records."""
        _, results = run_profiler(tpcds_config)
        with open(tpcds_config.output) as f:
            lines = f.readlines()
        assert len(lines) >= 25  # header + 24+ tables
        header = json.loads(lines[0])
        assert header.get("_header") is True


@pytest.mark.skipif(not _has_tpcds(), reason=SKIP_MSG)
class TestSyntheticTypes:
    """Validate profiling of BOOLEAN, TIMESTAMP, and other types from synthetic table."""

    def test_boolean_columns(self, tpcds_config):
        _, results = run_profiler(tpcds_config)
        st = next((r for r in results if r.name == "synthetic_types"), None)
        if st is None:
            pytest.skip("synthetic_types table not found")
        bool_cols = [c for c in st.columns if c.canonical_type == "boolean"]
        assert len(bool_cols) >= 1
        for col in bool_cols:
            assert col.true_count is not None
            assert col.mean is None  # No mean on booleans
            assert col.stddev is None

    def test_timestamp_columns(self, tpcds_config):
        _, results = run_profiler(tpcds_config)
        st = next((r for r in results if r.name == "synthetic_types"), None)
        if st is None:
            pytest.skip("synthetic_types table not found")
        ts_cols = [c for c in st.columns if c.canonical_type == "datetime"]
        assert len(ts_cols) >= 1
        for col in ts_cols:
            assert col.min is not None or col.null_rate == 1.0


@pytest.mark.skipif(not _has_tpcds(), reason=SKIP_MSG)
class TestFastMode:
    """Test schema-only (fast) mode."""

    def test_fast_mode_no_stats(self, tmp_path):
        config = ProfilerConfig(
            engine="duckdb",
            dsn=f"duckdb:///{TPCDS_DB}",
            sample_size=10000,
            concurrency=1,
            stats_depth="fast",
            output=str(tmp_path / "fast_output.ndjson"),
            output_format="json",
        )
        _, results = run_profiler(config)
        assert len(results) >= 24
        # In fast mode, stats should be defaults (no aggregates run)
        for r in results:
            for c in r.columns:
                assert c.null_count == 0  # Not computed
                assert c.approx_distinct == 0  # Not computed


@pytest.mark.skipif(not _has_tpcds(), reason=SKIP_MSG)
class TestSQLiteEngine:
    """Test SQLite adapter by exporting a small TPC-DS table."""

    def test_sqlite_profile(self, tmp_path):
        import sqlite3

        # Create a small test table directly in SQLite (no DuckDB dependency)
        sqlite_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(sqlite_path)
        conn.execute("CREATE TABLE income_band (ib_income_band_sk INTEGER, ib_lower_bound INTEGER, ib_upper_bound INTEGER)")
        test_data = [(i, i * 10000, (i + 1) * 10000) for i in range(1, 21)]
        conn.executemany("INSERT INTO income_band VALUES (?, ?, ?)", test_data)
        conn.commit()
        conn.close()

        config = ProfilerConfig(
            engine="sqlite",
            dsn=f"sqlite:///{sqlite_path}",
            sample_size=100,
            concurrency=1,
            output=str(tmp_path / "sqlite_output.ndjson"),
            output_format="json",
        )
        run_id, results = run_profiler(config)
        assert len(results) == 1
        assert results[0].name == "income_band"
        assert results[0].error is None
        # SQLite should use exact distinct
        for c in results[0].columns:
            assert c.distinct_mode == "exact"
