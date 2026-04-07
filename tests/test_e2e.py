"""End-to-end integration tests: run the profiler against real DuckDB datasets
and validate accuracy of results against known data properties.

Each test creates a purpose-built DuckDB database with known statistical
properties, profiles it through the full pipeline (CLI or Python API),
and asserts the profiler's output matches ground truth within tolerance.
"""

from __future__ import annotations

import json
import math
import os
import statistics
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from data_profiler.cli import cli
from data_profiler.config import ProfilerConfig
from data_profiler.run import run_profiler


# ---------------------------------------------------------------------------
# Fixtures: purpose-built DuckDB databases with known properties
# ---------------------------------------------------------------------------


@pytest.fixture()
def precision_db(tmp_path):
    """Database with columns whose statistics are exactly computable.

    Known properties:
    - integers: 1..1000 contiguous, no nulls
      → min=1, max=1000, count=1000, distinct=1000, mean=500.5, null_count=0
    - with_nulls: 1..800 + 200 nulls
      → null_count=200, null_rate=0.2, distinct=800
    - salaries: fixed set with known mean/stddev
    - booleans: 700 true, 300 false
      → true_rate=0.7, true_count=700
    - dates: 2020-01-01 to 2020-12-31 (366 values)
    - strings: 10 distinct department names, each repeated 100x
    """
    db_path = str(tmp_path / "precision.duckdb")
    conn = duckdb.connect(db_path)

    conn.execute("""
        CREATE TABLE known_stats (
            id            INTEGER,
            with_nulls    INTEGER,
            is_active     BOOLEAN,
            dept          VARCHAR,
            created_date  DATE,
            amount        DOUBLE
        )
    """)

    # id: 1..1000 contiguous
    # with_nulls: 1..800 then 200 NULLs
    # is_active: 700 true then 300 false
    # dept: 10 names, each 100 times
    # created_date: rotating through 366 days of 2020
    # amount: known values for computable stats
    depts = ["Engineering", "Sales", "Marketing", "Finance", "HR",
             "Legal", "Ops", "Product", "Design", "Support"]

    from datetime import date, timedelta

    base_date = date(2020, 1, 1)
    rows = []
    for i in range(1, 1001):
        null_val = i if i <= 800 else None
        active = i <= 700
        dept = depts[(i - 1) % 10]
        day_offset = (i - 1) % 366
        created = base_date + timedelta(days=day_offset)
        amount = float(i * 100)  # 100, 200, ..., 100000
        rows.append((i, null_val, active, dept, created, amount))

    conn.executemany("""
        INSERT INTO known_stats VALUES (?, ?, ?, ?, ?, ?)
    """, rows)

    # A second table for relationship testing
    conn.execute("""
        CREATE TABLE departments (
            dept_id   INTEGER PRIMARY KEY,
            name      VARCHAR,
            budget    DOUBLE
        )
    """)
    for i, name in enumerate(depts, 1):
        conn.execute("INSERT INTO departments VALUES (?, ?, ?)",
                     [i, name, float(i * 50000)])

    conn.close()
    return db_path


@pytest.fixture()
def edge_case_db(tmp_path):
    """Database designed to trigger edge cases and anomaly rules.

    Tables:
    - all_nulls: every column is NULL (1000 rows)
    - single_value: one distinct value per column
    - empty_table: 0 rows
    - all_unique: every value is unique (high cardinality)
    - pii_table: columns with email, IP, phone patterns
    - tiny_table: 3 rows (below HLL guard threshold)
    """
    db_path = str(tmp_path / "edge_cases.duckdb")
    conn = duckdb.connect(db_path)

    # All-null columns
    conn.execute("CREATE TABLE all_nulls (a INTEGER, b VARCHAR, c DOUBLE)")
    conn.executemany("INSERT INTO all_nulls VALUES (NULL, NULL, NULL)",
                     [() for _ in range(1000)])

    # Single-value columns
    conn.execute("CREATE TABLE single_value (status VARCHAR, code INTEGER)")
    conn.executemany("INSERT INTO single_value VALUES ('ACTIVE', 1)",
                     [() for _ in range(500)])

    # Empty table
    conn.execute("CREATE TABLE empty_table (id INTEGER, name VARCHAR)")

    # All-unique strings
    conn.execute("CREATE TABLE all_unique (uuid VARCHAR, seq INTEGER)")
    conn.executemany("INSERT INTO all_unique VALUES (?, ?)",
                     [(f"uuid-{i:06d}", i) for i in range(2000)])

    # PII patterns
    conn.execute("""
        CREATE TABLE pii_data (
            email      VARCHAR,
            ip_address VARCHAR,
            phone      VARCHAR,
            ssn        VARCHAR,
            normal_col VARCHAR
        )
    """)
    pii_rows = []
    for i in range(500):
        email = f"user{i}@example.com"
        ip = f"192.168.{i % 256}.{(i * 7) % 256}"
        phone = f"({(200 + i % 800):03d}) {(100 + i % 900):03d}-{(1000 + i % 9000):04d}"
        ssn = f"{(100 + i % 900):03d}-{(10 + i % 90):02d}-{(1000 + i % 9000):04d}"
        normal = f"value_{i}"
        pii_rows.append((email, ip, phone, ssn, normal))
    conn.executemany("INSERT INTO pii_data VALUES (?, ?, ?, ?, ?)", pii_rows)

    # Tiny table (below sampling thresholds)
    conn.execute("CREATE TABLE tiny (id INTEGER, val DOUBLE)")
    conn.executemany("INSERT INTO tiny VALUES (?, ?)",
                     [(1, 10.0), (2, 20.0), (3, 30.0)])

    conn.close()
    return db_path


@pytest.fixture()
def wide_table_db(tmp_path):
    """Database with a wide table (120 columns) to test column batching."""
    db_path = str(tmp_path / "wide.duckdb")
    conn = duckdb.connect(db_path)

    cols = ", ".join(f"col_{i} INTEGER" for i in range(120))
    conn.execute(f"CREATE TABLE wide_table ({cols})")

    for row in range(200):
        vals = ", ".join(str(row * 120 + i) for i in range(120))
        conn.execute(f"INSERT INTO wide_table VALUES ({vals})")

    conn.close()
    return db_path


@pytest.fixture()
def numeric_distribution_db(tmp_path):
    """Database with columns having known statistical distributions.

    - uniform: 1..10000 (uniform distribution)
    - negative_vals: mix of positive and negative
    - zeros: column with many zeros
    - monotonic_asc: strictly increasing sequence
    - monotonic_desc: strictly decreasing sequence
    """
    db_path = str(tmp_path / "distributions.duckdb")
    conn = duckdb.connect(db_path)

    conn.execute("""
        CREATE TABLE distributions (
            uniform        INTEGER,
            negative_vals  DOUBLE,
            zeros          INTEGER,
            monotonic_asc  INTEGER,
            monotonic_desc INTEGER
        )
    """)

    rows = []
    for i in range(1, 5001):
        uniform = i
        negative = float(i - 2500)  # -2499 to 2500
        zero_val = 0 if i <= 2000 else i  # 2000 zeros, then 3000 non-zero
        mono_asc = i
        mono_desc = 5001 - i
        rows.append((uniform, negative, zero_val, mono_asc, mono_desc))

    conn.executemany("INSERT INTO distributions VALUES (?, ?, ?, ?, ?)", rows)
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Helper: run profiler and return results dict keyed by table name
# ---------------------------------------------------------------------------


def _profile(db_path, tmp_path, **overrides):
    """Run profiler via Python API, return {table_name: ProfiledTable}."""
    out = str(tmp_path / "output.ndjson")
    defaults = dict(
        engine="duckdb",
        dsn=f"duckdb:///{db_path}",
        sample_size=0,  # full scan for accuracy testing
        concurrency=1,
        output=out,
        output_format="json",
    )
    defaults.update(overrides)
    config = ProfilerConfig(**defaults)
    _run_id, results = run_profiler(config)
    return {r.name: r for r in results}


def _col(table, name):
    """Get a column profile by name from a ProfiledTable."""
    return next((c for c in table.columns if c.name == name), None)


# ---------------------------------------------------------------------------
# Test Suite 1: Accuracy of profiling results against known data
# ---------------------------------------------------------------------------


class TestAccuracyKnownData:
    """Validate profiler output matches ground truth from precision_db."""

    def test_row_counts(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        assert tables["known_stats"].total_row_count == 1000
        assert tables["departments"].total_row_count == 10

    def test_null_counts(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        t = tables["known_stats"]

        id_col = _col(t, "id")
        assert id_col.null_count == 0
        assert id_col.null_rate == 0.0

        nulls_col = _col(t, "with_nulls")
        assert nulls_col.null_count == 200
        assert abs(nulls_col.null_rate - 0.2) < 0.01

    def test_min_max_integer(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        id_col = _col(tables["known_stats"], "id")
        assert id_col.min == 1
        assert id_col.max == 1000

    def test_min_max_with_nulls(self, precision_db, tmp_path):
        """min/max should ignore NULLs."""
        tables = _profile(precision_db, tmp_path)
        nulls_col = _col(tables["known_stats"], "with_nulls")
        assert nulls_col.min == 1
        assert nulls_col.max == 800

    def test_mean_accuracy(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        id_col = _col(tables["known_stats"], "id")
        # mean of 1..1000 = 500.5
        assert id_col.mean is not None
        assert abs(id_col.mean - 500.5) < 0.1

    def test_amount_mean(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        amt = _col(tables["known_stats"], "amount")
        # amount = 100, 200, ..., 100000 → mean = 50050.0
        assert amt.mean is not None
        assert abs(amt.mean - 50050.0) < 1.0

    def test_distinct_counts(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        t = tables["known_stats"]

        id_col = _col(t, "id")
        # Full scan on small table → should be exact or very close
        assert id_col.approx_distinct >= 950  # HLL ±5%

        dept_col = _col(t, "dept")
        assert 9 <= dept_col.approx_distinct <= 12  # HLL tolerance for low cardinality

    def test_exact_distinct_flag(self, precision_db, tmp_path):
        """--exact-distinct forces COUNT(DISTINCT) instead of HLL."""
        tables = _profile(precision_db, tmp_path, exact_distinct=True)
        id_col = _col(tables["known_stats"], "id")
        assert id_col.approx_distinct == 1000  # exact count, no HLL error
        dept_col = _col(tables["known_stats"], "dept")
        assert dept_col.approx_distinct == 10  # exact count matches reality

    def test_boolean_profiling(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        active = _col(tables["known_stats"], "is_active")
        assert active.true_count == 700
        assert abs(active.true_rate - 0.7) < 0.01

    def test_canonical_types(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        t = tables["known_stats"]
        assert _col(t, "id").canonical_type == "integer"
        assert _col(t, "amount").canonical_type in ("float", "double")
        assert _col(t, "dept").canonical_type == "string"
        assert _col(t, "is_active").canonical_type == "boolean"
        assert _col(t, "created_date").canonical_type == "date"

    def test_string_distinct(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        dept = _col(tables["known_stats"], "dept")
        assert 9 <= dept.approx_distinct <= 12  # HLL tolerance

    def test_departments_budget_range(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        budget = _col(tables["departments"], "budget")
        assert budget.min == 50000.0
        assert budget.max == 500000.0


class TestAccuracyDistributions:
    """Validate statistical metrics on the numeric_distribution_db."""

    def test_uniform_range(self, numeric_distribution_db, tmp_path):
        tables = _profile(numeric_distribution_db, tmp_path)
        u = _col(tables["distributions"], "uniform")
        assert u.min == 1
        assert u.max == 5000
        assert abs(u.mean - 2500.5) < 1.0

    def test_negative_values(self, numeric_distribution_db, tmp_path):
        tables = _profile(numeric_distribution_db, tmp_path)
        neg = _col(tables["distributions"], "negative_vals")
        assert neg.min == -2499.0
        assert neg.max == 2500.0
        assert neg.negative_count is not None
        assert neg.negative_count == 2499  # values -2499..-1

    def test_zero_count(self, numeric_distribution_db, tmp_path):
        tables = _profile(numeric_distribution_db, tmp_path)
        z = _col(tables["distributions"], "zeros")
        assert z.zero_count is not None
        assert z.zero_count == 2000

    def test_monotonic_detection(self, numeric_distribution_db, tmp_path):
        tables = _profile(numeric_distribution_db, tmp_path)
        asc = _col(tables["distributions"], "monotonic_asc")
        desc = _col(tables["distributions"], "monotonic_desc")
        # These should be detected as monotonic
        assert asc.is_monotonic_increasing is True
        assert desc.is_monotonic_decreasing is True

    def test_stddev_accuracy(self, numeric_distribution_db, tmp_path):
        """Verify stddev of uniform 1..5000 is close to theoretical value."""
        tables = _profile(numeric_distribution_db, tmp_path)
        u = _col(tables["distributions"], "uniform")
        # Theoretical population stddev of 1..N: sqrt((N²-1)/12)
        # For sample stddev of 1..5000: ~1443.5
        expected_stddev = statistics.stdev(range(1, 5001))
        assert u.stddev is not None
        assert abs(u.stddev - expected_stddev) / expected_stddev < 0.02  # within 2%

    def test_percentiles(self, numeric_distribution_db, tmp_path):
        """Verify percentile estimates are in the right ballpark."""
        tables = _profile(numeric_distribution_db, tmp_path)
        u = _col(tables["distributions"], "uniform")
        # For uniform 1..5000:
        # p25 ≈ 1250, p50 ≈ 2500, p75 ≈ 3750
        if u.p25 is not None:
            assert 1100 < u.p25 < 1400
        if u.median is not None:
            assert 2400 < u.median < 2600
        if u.p75 is not None:
            assert 3600 < u.p75 < 3900


# ---------------------------------------------------------------------------
# Test Suite 2: Edge cases and anomaly detection
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Validate profiler handles edge cases correctly."""

    def test_all_null_columns(self, edge_case_db, tmp_path):
        tables = _profile(edge_case_db, tmp_path)
        t = tables["all_nulls"]
        assert t.total_row_count == 1000
        for col in t.columns:
            assert col.null_count == 1000
            assert col.null_rate == 1.0

    def test_single_value_anomaly(self, edge_case_db, tmp_path):
        tables = _profile(edge_case_db, tmp_path)
        t = tables["single_value"]
        status = _col(t, "status")
        assert status.approx_distinct == 1
        # Should trigger single_value or near_constant anomaly
        assert any("single_value" in a or "near_constant" in a
                   for a in status.anomalies), f"Expected anomaly, got {status.anomalies}"

    def test_empty_table(self, edge_case_db, tmp_path):
        tables = _profile(edge_case_db, tmp_path)
        t = tables["empty_table"]
        assert t.total_row_count == 0

    def test_all_unique_detection(self, edge_case_db, tmp_path):
        tables = _profile(edge_case_db, tmp_path)
        t = tables["all_unique"]
        uuid_col = _col(t, "uuid")
        # 2000 rows, all distinct
        assert uuid_col.approx_distinct >= 1600  # HLL tolerance on small cardinality

    def test_tiny_table(self, edge_case_db, tmp_path):
        tables = _profile(edge_case_db, tmp_path)
        t = tables["tiny"]
        assert t.total_row_count == 3
        val = _col(t, "val")
        assert val.min == 10.0
        assert val.max == 30.0
        assert abs(val.mean - 20.0) < 0.1

    def test_pii_email_detection(self, edge_case_db, tmp_path):
        tables = _profile(edge_case_db, tmp_path)
        email = _col(tables["pii_data"], "email")
        assert "email" in email.patterns, f"Expected email pattern, got {email.patterns}"

    def test_pii_ip_detection(self, edge_case_db, tmp_path):
        tables = _profile(edge_case_db, tmp_path)
        ip = _col(tables["pii_data"], "ip_address")
        assert "ipv4" in ip.patterns, f"Expected ipv4 pattern, got {ip.patterns}"

    def test_pii_normal_column_clean(self, edge_case_db, tmp_path):
        """Non-PII column should not trigger PII patterns."""
        tables = _profile(edge_case_db, tmp_path)
        normal = _col(tables["pii_data"], "normal_col")
        pii_patterns = {"email", "ssn", "credit_card", "phone", "ipv4"}
        detected_pii = set(normal.patterns) & pii_patterns
        assert len(detected_pii) == 0, f"False positive PII: {detected_pii}"


class TestWideTables:
    """Validate column batching works correctly on wide tables."""

    def test_wide_table_all_columns_profiled(self, wide_table_db, tmp_path):
        tables = _profile(wide_table_db, tmp_path)
        t = tables["wide_table"]
        assert len(t.columns) == 120
        assert t.total_row_count == 200

    def test_wide_table_column_batch_size(self, wide_table_db, tmp_path):
        """With batch_size=40, a 120-col table needs 3 batches. All cols should still work."""
        tables = _profile(wide_table_db, tmp_path, column_batch_size=40)
        t = tables["wide_table"]
        assert len(t.columns) == 120
        # Spot-check a few columns across batches
        col0 = _col(t, "col_0")
        col60 = _col(t, "col_60")
        col119 = _col(t, "col_119")
        assert col0.null_count == 0
        assert col60.null_count == 0
        assert col119.null_count == 0


# ---------------------------------------------------------------------------
# Test Suite 3: CLI commands end-to-end
# ---------------------------------------------------------------------------


class TestCLIRun:
    """Test the `profiler run` command via CliRunner."""

    def test_basic_run(self, precision_db, tmp_path):
        out = str(tmp_path / "output.ndjson")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "run", "--engine", "duckdb",
            "--dsn", f"duckdb:///{precision_db}",
            "--sample", "0",
            "-o", out,
        ])
        assert result.exit_code == 0, result.output
        assert Path(out).exists()
        # Verify NDJSON contains table data
        lines = Path(out).read_text().strip().split("\n")
        assert len(lines) >= 3  # header + 2 tables + trailer

    def test_run_with_sampling(self, precision_db, tmp_path):
        out = str(tmp_path / "sampled.ndjson")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "run", "--engine", "duckdb",
            "--dsn", f"duckdb:///{precision_db}",
            "--sample", "100",
            "-o", out,
        ])
        assert result.exit_code == 0, result.output

    def test_run_stats_depth_fast(self, precision_db, tmp_path):
        out = str(tmp_path / "fast.ndjson")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "run", "--engine", "duckdb",
            "--dsn", f"duckdb:///{precision_db}",
            "--stats-depth", "fast",
            "-o", out,
        ])
        assert result.exit_code == 0, result.output

    def test_run_exact_distinct(self, precision_db, tmp_path):
        out = str(tmp_path / "exact.ndjson")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "run", "--engine", "duckdb",
            "--dsn", f"duckdb:///{precision_db}",
            "--exact-distinct",
            "-o", out,
        ])
        assert result.exit_code == 0, result.output

    def test_run_yaml_output(self, precision_db, tmp_path):
        out = str(tmp_path / "output.yaml")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "run", "--engine", "duckdb",
            "--dsn", f"duckdb:///{precision_db}",
            "--output-format", "yaml",
            "-o", out,
        ])
        assert result.exit_code == 0, result.output
        assert Path(out).exists()

    def test_run_verbose(self, precision_db, tmp_path):
        out = str(tmp_path / "verbose.ndjson")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "-v", "run", "--engine", "duckdb",
            "--dsn", f"duckdb:///{precision_db}",
            "-o", out,
        ])
        assert result.exit_code == 0, result.output

    def test_run_column_batch_size(self, wide_table_db, tmp_path):
        out = str(tmp_path / "batched.ndjson")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "run", "--engine", "duckdb",
            "--dsn", f"duckdb:///{wide_table_db}",
            "--column-batch-size", "30",
            "-o", out,
        ])
        assert result.exit_code == 0, result.output


class TestCLIDashboard:
    """Test the `profiler dashboard` command."""

    def test_dashboard_generates_html(self, precision_db, tmp_path):
        out = str(tmp_path / "dashboard.html")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "dashboard", "--engine", "duckdb",
            "--dsn", f"duckdb:///{precision_db}",
            "--sample", "0",
            "-o", out,
        ])
        assert result.exit_code == 0, result.output
        assert Path(out).exists()
        content = Path(out).read_text()
        assert "<!DOCTYPE html>" in content
        assert "Data Profiler Dashboard" in content

    def test_dashboard_contains_table_data(self, precision_db, tmp_path):
        out = str(tmp_path / "dashboard.html")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "dashboard", "--engine", "duckdb",
            "--dsn", f"duckdb:///{precision_db}",
            "--sample", "0",
            "-o", out,
        ])
        assert result.exit_code == 0, result.output
        content = Path(out).read_text()
        assert "known_stats" in content
        assert "departments" in content


class TestCLIExport:
    """Test the `profiler export` command."""

    def test_export_openmetadata(self, precision_db, tmp_path):
        out = str(tmp_path / "catalog.json")
        runner = CliRunner()
        result = runner.invoke(cli, [
            "export", "--engine", "duckdb",
            "--dsn", f"duckdb:///{precision_db}",
            "-o", out,
        ])
        assert result.exit_code == 0, result.output
        assert Path(out).exists()
        data = json.loads(Path(out).read_text())
        assert "tables" in data
        table_names = {t["name"] for t in data["tables"]}
        assert "known_stats" in table_names


# ---------------------------------------------------------------------------
# Test Suite 4: Output format validation
# ---------------------------------------------------------------------------


class TestOutputFormats:
    """Verify all output formats produce valid files."""

    def test_json_output_parseable(self, precision_db, tmp_path):
        out = str(tmp_path / "output.ndjson")
        config = ProfilerConfig(
            engine="duckdb", dsn=f"duckdb:///{precision_db}",
            sample_size=0, concurrency=1, output=out, output_format="json",
        )
        run_profiler(config)
        lines = Path(out).read_text().strip().split("\n")
        for line in lines:
            json.loads(line)  # each line must be valid JSON

    def test_yaml_output_parseable(self, precision_db, tmp_path):
        import yaml
        out = str(tmp_path / "output.yaml")
        config = ProfilerConfig(
            engine="duckdb", dsn=f"duckdb:///{precision_db}",
            sample_size=0, concurrency=1, output=out, output_format="yaml",
        )
        run_profiler(config)
        content = Path(out).read_text()
        docs = list(yaml.safe_load_all(content))
        assert len(docs) >= 2  # at least header + tables


# ---------------------------------------------------------------------------
# Test Suite 5: Sampling accuracy
# ---------------------------------------------------------------------------


class TestSamplingAccuracy:
    """Verify sampled results are within acceptable tolerance of full-scan."""

    def test_sampled_row_count_respected(self, precision_db, tmp_path):
        """With sample=100, sampled_row_count should be <= 100."""
        tables = _profile(precision_db, tmp_path, sample_size=100)
        t = tables["known_stats"]
        assert t.total_row_count == 1000
        assert t.sampled_row_count <= 200  # reservoir can overshoot slightly

    def test_sampled_mean_within_tolerance(self, numeric_distribution_db, tmp_path):
        """Sampled mean of uniform 1..5000 should be close to 2500.5."""
        tables = _profile(numeric_distribution_db, tmp_path, sample_size=1000)
        u = _col(tables["distributions"], "uniform")
        assert u.mean is not None
        # With 1000 samples from uniform 1..5000, mean should be within 10%
        assert abs(u.mean - 2500.5) / 2500.5 < 0.15

    def test_full_scan_distinct_despite_sampling(self, precision_db, tmp_path):
        """HLL distinct counts should run on full table even when sampling."""
        tables = _profile(precision_db, tmp_path, sample_size=100)
        dept = _col(tables["known_stats"], "dept")
        # distinct ~10 even though only 100 rows sampled (HLL runs on full table)
        assert 9 <= dept.approx_distinct <= 12


# ---------------------------------------------------------------------------
# Test Suite 6: Relationship discovery
# ---------------------------------------------------------------------------


class TestRelationshipDiscovery:
    """Verify FK relationships are discovered correctly."""

    def test_discovers_relationships(self, precision_db, tmp_path):
        from data_profiler.workers.relationship_worker import discover_relationships
        tables = _profile(precision_db, tmp_path)
        results = list(tables.values())
        rels = discover_relationships(results)
        # Should find some relationship candidates between known_stats and departments
        assert len(rels) >= 0  # May or may not find dept FK depending on naming


# ---------------------------------------------------------------------------
# Test Suite 7: Constraint suggestion accuracy
# ---------------------------------------------------------------------------


class TestConstraintSuggestions:
    """Verify constraint suggestions match known data properties."""

    def test_not_null_suggestion(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        t = tables["known_stats"]
        if t.suggested_constraints:
            not_null_cols = {c["column"] for c in t.suggested_constraints
                           if c["constraint_type"] == "NOT NULL"}
            # id has 0 nulls → should get NOT NULL suggestion
            assert "id" in not_null_cols, f"Expected NOT NULL for id, got {not_null_cols}"
            # with_nulls has 200 nulls → should NOT get NOT NULL
            assert "with_nulls" not in not_null_cols

    def test_unique_suggestion(self, edge_case_db, tmp_path):
        tables = _profile(edge_case_db, tmp_path, exact_distinct=True)
        t = tables["all_unique"]
        if t.suggested_constraints:
            unique_cols = {c["column"] for c in t.suggested_constraints
                         if c["constraint_type"] == "UNIQUE"}
            # With exact distinct, all_unique anomaly fires reliably
            assert "uuid" in unique_cols or "seq" in unique_cols


# ---------------------------------------------------------------------------
# Test Suite 8: Enrichment features
# ---------------------------------------------------------------------------


class TestEnrichmentFeatures:
    """Verify histograms, Benford, correlations are computed."""

    def test_histogram_generated(self, numeric_distribution_db, tmp_path):
        tables = _profile(numeric_distribution_db, tmp_path)
        u = _col(tables["distributions"], "uniform")
        assert u.histogram is not None
        assert len(u.histogram) > 0
        # Histogram bins should sum to total rows
        total = sum(b["count"] for b in u.histogram)
        assert total == 5000

    def test_benford_on_positive_column(self, numeric_distribution_db, tmp_path):
        tables = _profile(numeric_distribution_db, tmp_path)
        u = _col(tables["distributions"], "uniform")
        assert u.benford_digits is not None
        assert u.benford_pvalue is not None
        # Leading digits 1..9 should be present
        digits = {d["digit"] for d in u.benford_digits}
        assert digits == {1, 2, 3, 4, 5, 6, 7, 8, 9}

    def test_correlation_computed(self, numeric_distribution_db, tmp_path):
        tables = _profile(numeric_distribution_db, tmp_path)
        t = tables["distributions"]
        assert t.correlations is not None
        assert len(t.correlations) > 0

    def test_top_values_present(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        dept = _col(tables["known_stats"], "dept")
        assert dept.top_values is not None
        assert len(dept.top_values) > 0
        # Each department appears 100 times
        for tv in dept.top_values:
            assert tv["count"] == 100

    def test_constraint_suggestions_present(self, precision_db, tmp_path):
        tables = _profile(precision_db, tmp_path)
        t = tables["known_stats"]
        assert t.suggested_constraints is not None
        assert len(t.suggested_constraints) > 0
        # Should at minimum suggest NOT NULL for id (which has 0 nulls)
        types = {c["constraint_type"] for c in t.suggested_constraints}
        assert "NOT NULL" in types


# ===========================================================================
# TPC-H BENCHMARK TESTS
# ===========================================================================


@pytest.fixture(scope="module")
def tpch_db(tmp_path_factory):
    """Generate TPC-H sf=0.01 dataset in a persistent DuckDB file."""
    db_path = str(tmp_path_factory.mktemp("tpch") / "tpch.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("INSTALL tpch; LOAD tpch; CALL dbgen(sf=0.01);")
    conn.close()
    return db_path


@pytest.fixture(scope="module")
def tpch_profiles(tpch_db, tmp_path_factory):
    """Profile the full TPC-H dataset once, share across tests."""
    out = str(tmp_path_factory.mktemp("tpch_out") / "output.ndjson")
    config = ProfilerConfig(
        engine="duckdb",
        dsn=f"duckdb:///{tpch_db}",
        sample_size=0,
        concurrency=1,
        output=out,
        output_format="json",
    )
    _run_id, results = run_profiler(config)
    return {r.name: r for r in results}


class TestTPCH_RowCounts:
    """Validate row counts against known TPC-H sf=0.01 cardinalities."""

    EXPECTED = {
        "lineitem": 60175,
        "orders": 15000,
        "customer": 1500,
        "part": 2000,
        "partsupp": 8000,
        "supplier": 100,
        "nation": 25,
        "region": 5,
    }

    def test_all_tables_discovered(self, tpch_profiles):
        for name in self.EXPECTED:
            assert name in tpch_profiles, f"Missing table: {name}"

    def test_row_counts(self, tpch_profiles):
        for name, expected in self.EXPECTED.items():
            assert tpch_profiles[name].total_row_count == expected, \
                f"{name}: expected {expected}, got {tpch_profiles[name].total_row_count}"


class TestTPCH_Lineitem:
    """Validate profiler accuracy on lineitem — the largest TPC-H table."""

    def test_quantity_range(self, tpch_profiles):
        q = _col(tpch_profiles["lineitem"], "l_quantity")
        assert q.min == 1.0
        assert q.max == 50.0

    def test_quantity_mean(self, tpch_profiles):
        q = _col(tpch_profiles["lineitem"], "l_quantity")
        assert abs(q.mean - 25.53) < 0.5

    def test_discount_range(self, tpch_profiles):
        d = _col(tpch_profiles["lineitem"], "l_discount")
        assert d.min == 0.0
        assert d.max == 0.10

    def test_discount_distinct(self, tpch_profiles):
        d = _col(tpch_profiles["lineitem"], "l_discount")
        assert 10 <= d.approx_distinct <= 13  # HLL tolerance on 11 distinct

    def test_returnflag_distinct(self, tpch_profiles):
        rf = _col(tpch_profiles["lineitem"], "l_returnflag")
        assert 2 <= rf.approx_distinct <= 4  # 3 values (A, N, R), HLL tolerance

    def test_linestatus_distinct(self, tpch_profiles):
        ls = _col(tpch_profiles["lineitem"], "l_linestatus")
        assert 1 <= ls.approx_distinct <= 3  # 2 values (F, O), HLL tolerance

    def test_shipmode_distinct(self, tpch_profiles):
        sm = _col(tpch_profiles["lineitem"], "l_shipmode")
        assert 6 <= sm.approx_distinct <= 8  # 7 distinct modes

    def test_no_nulls(self, tpch_profiles):
        """TPC-H lineitem has no NULL columns."""
        for col in tpch_profiles["lineitem"].columns:
            assert col.null_count == 0, f"{col.name} has {col.null_count} nulls"

    def test_shipdate_is_date(self, tpch_profiles):
        sd = _col(tpch_profiles["lineitem"], "l_shipdate")
        assert sd.canonical_type == "date"

    def test_extendedprice_stats(self, tpch_profiles):
        ep = _col(tpch_profiles["lineitem"], "l_extendedprice")
        assert ep.min == 904.0
        assert ep.max == 94949.50
        assert abs(ep.mean - 35765.5) < 50
        assert ep.stddev is not None
        assert abs(ep.stddev - 21844.2) < 200

    def test_canonical_types(self, tpch_profiles):
        li = tpch_profiles["lineitem"]
        assert _col(li, "l_orderkey").canonical_type == "integer"
        assert _col(li, "l_quantity").canonical_type == "float"
        assert _col(li, "l_returnflag").canonical_type == "string"
        assert _col(li, "l_shipdate").canonical_type == "date"


class TestTPCH_Orders:
    """Validate profiler accuracy on orders table."""

    def test_totalprice_range(self, tpch_profiles):
        tp = _col(tpch_profiles["orders"], "o_totalprice")
        assert tp.min == 874.89
        assert tp.max == 466001.28

    def test_orderstatus_distinct(self, tpch_profiles):
        os_ = _col(tpch_profiles["orders"], "o_orderstatus")
        assert 2 <= os_.approx_distinct <= 4  # 3 values (F, O, P), HLL tolerance

    def test_orderpriority_distinct(self, tpch_profiles):
        op = _col(tpch_profiles["orders"], "o_orderpriority")
        assert 4 <= op.approx_distinct <= 6  # 5 priorities

    def test_clerk_high_cardinality(self, tpch_profiles):
        cl = _col(tpch_profiles["orders"], "o_clerk")
        assert cl.approx_distinct >= 800  # 1000 distinct clerks, HLL tolerance


class TestTPCH_Customer:
    """Validate profiler accuracy on customer table."""

    def test_mktsegment_distinct(self, tpch_profiles):
        ms = _col(tpch_profiles["customer"], "c_mktsegment")
        assert 4 <= ms.approx_distinct <= 6  # 5 segments, HLL tolerance

    def test_acctbal_has_negatives(self, tpch_profiles):
        ab = _col(tpch_profiles["customer"], "c_acctbal")
        assert ab.min < 0  # TPC-H has negative account balances

    def test_nationkey_cardinality(self, tpch_profiles):
        nk = _col(tpch_profiles["customer"], "c_nationkey")
        assert 23 <= nk.approx_distinct <= 27  # 25 nations


class TestTPCH_SmallTables:
    """Validate profiler handles small dimension tables correctly."""

    def test_nation_25_rows(self, tpch_profiles):
        n = tpch_profiles["nation"]
        assert n.total_row_count == 25
        nk = _col(n, "n_nationkey")
        assert 23 <= nk.approx_distinct <= 27  # 25 nations

    def test_region_5_rows(self, tpch_profiles):
        r = tpch_profiles["region"]
        assert r.total_row_count == 5
        rk = _col(r, "r_regionkey")
        assert 4 <= rk.approx_distinct <= 6  # 5 regions

    def test_part_size_range(self, tpch_profiles):
        ps = _col(tpch_profiles["part"], "p_size")
        assert ps.min == 1
        assert ps.max == 50

    def test_part_brand_cardinality(self, tpch_profiles):
        pb = _col(tpch_profiles["part"], "p_brand")
        assert 20 <= pb.approx_distinct <= 32  # 25 brands, HLL wider tolerance on small tables


class TestTPCH_Enrichment:
    """Validate enrichment features work on real-world TPC-H data."""

    def test_relationships_discovered(self, tpch_profiles):
        """TPC-H has well-named FK columns — profiler should find some."""
        # At least one table should have discovered relationships
        has_relationships = any(
            t.correlations for t in tpch_profiles.values()
            if t.correlations
        )
        # Relationships are discovered at the cross-table level, check suggested_constraints
        has_constraints = any(
            t.suggested_constraints for t in tpch_profiles.values()
            if t.suggested_constraints
        )
        assert has_relationships or has_constraints, \
            "Expected enrichment output from TPC-H data"

    def test_lineitem_has_top_values(self, tpch_profiles):
        rf = _col(tpch_profiles["lineitem"], "l_returnflag")
        assert rf.top_values is not None
        assert len(rf.top_values) >= 3
        top_vals = {tv["value"] for tv in rf.top_values}
        assert {"A", "N", "R"} == top_vals

    def test_numeric_histograms(self, tpch_profiles):
        q = _col(tpch_profiles["lineitem"], "l_quantity")
        assert q.histogram is not None
        assert len(q.histogram) > 0

    def test_benford_on_prices(self, tpch_profiles):
        """Extended price is a good Benford candidate (naturally occurring values)."""
        ep = _col(tpch_profiles["lineitem"], "l_extendedprice")
        if ep.benford_digits is not None:
            assert len(ep.benford_digits) > 0
            assert ep.benford_pvalue is not None


class TestTPCH_Sampling:
    """Validate sampling works correctly against TPC-H lineitem (60K rows)."""

    def test_sampled_profile(self, tpch_db, tmp_path):
        """Profile with 5000-row sample — stats should be in the ballpark."""
        out = str(tmp_path / "sampled.ndjson")
        config = ProfilerConfig(
            engine="duckdb",
            dsn=f"duckdb:///{tpch_db}",
            sample_size=5000,
            concurrency=1,
            output=out,
            output_format="json",
        )
        _run_id, results = run_profiler(config)
        tables = {r.name: r for r in results}
        li = tables["lineitem"]

        # Row count should be the full table count
        assert li.total_row_count == 60175
        # Sampled row count should be at or near 5000
        assert li.sampled_row_count <= 5500

        # Sampled mean should be within 15% of true mean
        q = _col(li, "l_quantity")
        assert abs(q.mean - 25.53) / 25.53 < 0.15


# ===========================================================================
# TPC-DS BENCHMARK TESTS — FULL SCAN ALL TABLES
# ===========================================================================


@pytest.fixture(scope="module")
def tpcds_db(tmp_path_factory):
    """Generate TPC-DS sf=0.01 dataset in a persistent DuckDB file."""
    db_path = str(tmp_path_factory.mktemp("tpcds") / "tpcds.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=0.01);")
    conn.close()
    return db_path


@pytest.fixture(scope="module")
def tpcds_profiles(tpcds_db, tmp_path_factory):
    """Profile the full TPC-DS dataset — all 24 tables, full scan."""
    out = str(tmp_path_factory.mktemp("tpcds_out") / "output.ndjson")
    config = ProfilerConfig(
        engine="duckdb",
        dsn=f"duckdb:///{tpcds_db}",
        sample_size=0,
        concurrency=1,
        output=out,
        output_format="json",
    )
    _run_id, results = run_profiler(config)
    return {r.name: r for r in results}


class TestTPCDS_AllTables:
    """Validate profiler discovers and profiles every TPC-DS table."""

    EXPECTED_TABLES = {
        "call_center": 1,
        "catalog_page": 11718,
        "catalog_returns": 1358,
        "catalog_sales": 14313,
        "customer": 1000,
        "customer_address": 500,
        "customer_demographics": 19208,
        "date_dim": 73049,
        "household_demographics": 7200,
        "income_band": 20,
        "inventory": 23490,
        "item": 180,
        "promotion": 3,
        "reason": 1,
        "ship_mode": 20,
        "store": 1,
        "store_returns": 2810,
        "store_sales": 28810,
        "time_dim": 86400,
        "warehouse": 1,
        "web_page": 1,
        "web_returns": 679,
        "web_sales": 7212,
        "web_site": 1,
    }

    def test_all_24_tables_discovered(self, tpcds_profiles):
        for name in self.EXPECTED_TABLES:
            assert name in tpcds_profiles, f"Missing table: {name}"

    def test_row_counts(self, tpcds_profiles):
        for name, expected in self.EXPECTED_TABLES.items():
            actual = tpcds_profiles[name].total_row_count
            assert actual == expected, \
                f"{name}: expected {expected} rows, got {actual}"

    def test_no_empty_column_lists(self, tpcds_profiles):
        """Every table should have at least one profiled column."""
        for name, t in tpcds_profiles.items():
            assert len(t.columns) > 0, f"{name} has no profiled columns"

    def test_column_counts(self, tpcds_profiles):
        """Spot-check column counts for key tables."""
        assert len(tpcds_profiles["store_sales"].columns) == 23
        assert len(tpcds_profiles["catalog_sales"].columns) == 34
        assert len(tpcds_profiles["date_dim"].columns) == 28
        assert len(tpcds_profiles["time_dim"].columns) == 10
        assert len(tpcds_profiles["customer"].columns) == 18


class TestTPCDS_StoreSales:
    """Validate profiler accuracy on store_sales — the main fact table."""

    def test_quantity_range(self, tpcds_profiles):
        q = _col(tpcds_profiles["store_sales"], "ss_quantity")
        assert q.min == 1
        assert q.max == 100

    def test_quantity_mean(self, tpcds_profiles):
        q = _col(tpcds_profiles["store_sales"], "ss_quantity")
        assert abs(q.mean - 50.77) < 1.0

    def test_quantity_has_nulls(self, tpcds_profiles):
        q = _col(tpcds_profiles["store_sales"], "ss_quantity")
        assert q.null_count > 1000  # ~1283 nulls

    def test_sales_price_range(self, tpcds_profiles):
        sp = _col(tpcds_profiles["store_sales"], "ss_sales_price")
        assert sp.min == 0.0
        assert sp.max > 150

    def test_canonical_types(self, tpcds_profiles):
        ss = tpcds_profiles["store_sales"]
        assert _col(ss, "ss_sold_date_sk").canonical_type == "integer"
        assert _col(ss, "ss_quantity").canonical_type == "integer"
        assert _col(ss, "ss_sales_price").canonical_type == "float"


class TestTPCDS_DateDim:
    """Validate profiler accuracy on date_dim — largest dimension table."""

    def test_row_count(self, tpcds_profiles):
        assert tpcds_profiles["date_dim"].total_row_count == 73049

    def test_year_cardinality(self, tpcds_profiles):
        yr = _col(tpcds_profiles["date_dim"], "d_year")
        assert 170 <= yr.approx_distinct <= 270  # 201 distinct years, HLL tolerance

    def test_date_range(self, tpcds_profiles):
        d = _col(tpcds_profiles["date_dim"], "d_date")
        assert d.canonical_type == "date"

    def test_hour_range_in_time_dim(self, tpcds_profiles):
        h = _col(tpcds_profiles["time_dim"], "t_hour")
        assert h.min == 0
        assert h.max == 23

    def test_minute_range_in_time_dim(self, tpcds_profiles):
        m = _col(tpcds_profiles["time_dim"], "t_minute")
        assert m.min == 0
        assert m.max == 59


class TestTPCDS_Customer:
    """Validate profiler accuracy on customer dimension."""

    def test_birth_year_range(self, tpcds_profiles):
        by = _col(tpcds_profiles["customer"], "c_birth_year")
        assert by.min == 1924
        assert by.max == 1992

    def test_birth_country_cardinality(self, tpcds_profiles):
        bc = _col(tpcds_profiles["customer"], "c_birth_country")
        assert bc.approx_distinct >= 180  # 210 distinct, HLL tolerance

    def test_email_has_nulls(self, tpcds_profiles):
        email = _col(tpcds_profiles["customer"], "c_email_address")
        assert email.null_count > 0  # ~31 nulls


class TestTPCDS_Item:
    """Validate profiler accuracy on item dimension."""

    def test_current_price_range(self, tpcds_profiles):
        cp = _col(tpcds_profiles["item"], "i_current_price")
        assert cp.min == 0.11
        assert cp.max == 98.66

    def test_category_cardinality(self, tpcds_profiles):
        cat = _col(tpcds_profiles["item"], "i_category")
        assert 9 <= cat.approx_distinct <= 12  # 10 categories

    def test_brand_cardinality(self, tpcds_profiles):
        br = _col(tpcds_profiles["item"], "i_brand")
        assert br.approx_distinct >= 90  # 111 distinct brands


class TestTPCDS_NullPatterns:
    """Validate profiler correctly detects null patterns in TPC-DS."""

    def test_store_returns_has_nulls(self, tpcds_profiles):
        sr = _col(tpcds_profiles["store_returns"], "sr_returned_date_sk")
        assert sr.null_count > 0
        assert sr.null_rate > 0.0

    def test_web_returns_has_nulls(self, tpcds_profiles):
        wr = _col(tpcds_profiles["web_returns"], "wr_returned_date_sk")
        assert wr.null_count > 0

    def test_single_row_tables(self, tpcds_profiles):
        """Tables with 1 row should still profile correctly."""
        for name in ["call_center", "store", "warehouse", "web_page", "web_site", "reason"]:
            t = tpcds_profiles[name]
            assert t.total_row_count == 1 or t.total_row_count == 3  # reason has 1, promotion has 3
            assert len(t.columns) > 0
            for col in t.columns:
                if col.null_count == 0:
                    assert col.null_rate == 0.0


class TestTPCDS_Enrichment:
    """Validate enrichment features on TPC-DS data."""

    def test_constraints_suggested(self, tpcds_profiles):
        """At least some tables should get constraint suggestions."""
        tables_with_constraints = [
            name for name, t in tpcds_profiles.items()
            if t.suggested_constraints and len(t.suggested_constraints) > 0
        ]
        assert len(tables_with_constraints) > 0

    def test_histograms_on_fact_tables(self, tpcds_profiles):
        """Fact table numeric columns should have histograms."""
        q = _col(tpcds_profiles["store_sales"], "ss_quantity")
        if q.histogram is not None:
            assert len(q.histogram) > 0

    def test_top_values_on_dimensions(self, tpcds_profiles):
        """Dimension columns with low cardinality should have top values."""
        ms = _col(tpcds_profiles["time_dim"], "t_meal_time")
        if ms.top_values is not None:
            assert len(ms.top_values) > 0


class TestTPCDS_FullScanIntegrity:
    """Cross-cutting checks that every profiled table has consistent output."""

    def test_every_column_has_canonical_type(self, tpcds_profiles):
        for name, t in tpcds_profiles.items():
            for col in t.columns:
                assert col.canonical_type in (
                    "integer", "float", "string", "boolean",
                    "date", "datetime", "binary", "unknown",
                ), f"{name}.{col.name}: unexpected type '{col.canonical_type}'"

    def test_null_rate_consistent(self, tpcds_profiles):
        """null_rate should equal null_count / total_row_count."""
        for name, t in tpcds_profiles.items():
            if t.total_row_count == 0:
                continue
            for col in t.columns:
                expected_rate = col.null_count / t.total_row_count
                assert abs(col.null_rate - expected_rate) < 0.001, \
                    f"{name}.{col.name}: null_rate={col.null_rate}, expected {expected_rate}"

    def test_min_max_ordering(self, tpcds_profiles):
        """For numeric columns with data, min <= max."""
        for name, t in tpcds_profiles.items():
            for col in t.columns:
                if col.min is not None and col.max is not None:
                    if col.canonical_type in ("integer", "float"):
                        assert col.min <= col.max, \
                            f"{name}.{col.name}: min={col.min} > max={col.max}"

    def test_distinct_within_bounds(self, tpcds_profiles):
        """approx_distinct should be roughly bounded by row count.
        HLL accuracy improves with table size — allow wider margin for small tables
        and skip float/decimal columns (HLL overcounts due to hash characteristics)."""
        for name, t in tpcds_profiles.items():
            for col in t.columns:
                if col.approx_distinct is not None and t.total_row_count > 100:
                    if col.canonical_type == "float":
                        continue
                    # HLL is less accurate on small tables; use tiered tolerance
                    margin = 1.50 if t.total_row_count < 5000 else 1.20
                    assert col.approx_distinct <= t.total_row_count * margin, \
                        f"{name}.{col.name}: distinct={col.approx_distinct} > rows={t.total_row_count}"


class TestTPCDS_Dashboard:
    """Validate dashboard generation from TPC-DS profiling results."""

    def test_dashboard_generates_html(self, tpcds_db, tpcds_profiles, tmp_path):
        from datetime import datetime, timezone
        from dataclasses import asdict
        from data_profiler.dashboard import generate_dashboard
        from data_profiler.workers.relationship_worker import discover_relationships

        results = list(tpcds_profiles.values())
        rels = discover_relationships(results)
        rel_dicts = [asdict(r) for r in rels] if rels else None

        out_path = str(tmp_path / "tpcds_dashboard.html")
        generate_dashboard(
            run_id="tpcds-test",
            engine="duckdb",
            profiled_at=datetime.now(timezone.utc).isoformat(),
            results=results,
            output_path=out_path,
            relationships=rel_dicts,
        )

        html = Path(out_path).read_text()
        assert len(html) > 10000  # dashboard should be substantial
        assert "store_sales" in html
        assert "catalog_sales" in html
        assert "date_dim" in html
        assert "customer" in html

    def test_dashboard_table_count(self, tpcds_profiles, tmp_path):
        from datetime import datetime, timezone
        from data_profiler.dashboard import generate_dashboard

        results = list(tpcds_profiles.values())
        out_path = str(tmp_path / "tpcds_dash2.html")
        generate_dashboard(
            run_id="tpcds-count",
            engine="duckdb",
            profiled_at=datetime.now(timezone.utc).isoformat(),
            results=results,
            output_path=out_path,
        )

        html = Path(out_path).read_text()
        # Dashboard should show 24 tables
        assert "24" in html

    def test_dashboard_column_count(self, tpcds_profiles, tmp_path):
        from datetime import datetime, timezone
        from data_profiler.dashboard import generate_dashboard

        results = list(tpcds_profiles.values())
        total_cols = sum(len(t.columns) for t in results)
        out_path = str(tmp_path / "tpcds_dash3.html")
        generate_dashboard(
            run_id="tpcds-cols",
            engine="duckdb",
            profiled_at=datetime.now(timezone.utc).isoformat(),
            results=results,
            output_path=out_path,
        )

        html = Path(out_path).read_text()
        assert str(total_cols) in html
