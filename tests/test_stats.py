"""Unit tests for new statistical features: skewness/kurtosis, histogram, Benford, correlation."""

import math

import pytest

from data_profiler.adapters.duckdb import DuckDBAdapter
from data_profiler.adapters.sqlite import SQLiteAdapter
from data_profiler.workers.stats_worker import (
    ColumnProfile,
    ProfiledTable,
    compute_quality_score,
    _chi2_pvalue,
    _compute_histogram,
    _compute_benford,
    _compute_correlations,
    _compute_cramers_v,
    _compute_kde,
    _compute_mad,
    _compute_monotonicity,
    _compute_nmi,
)


# ---- Feature 1: Skewness + Kurtosis ----

class TestSkewnessKurtosisFields:
    def test_column_profile_has_skewness_kurtosis(self):
        """New fields exist on ColumnProfile with None defaults."""
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True,
        )
        assert cp.skewness is None
        assert cp.kurtosis is None

    def test_fields_accept_float_values(self):
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True, skewness=1.5, kurtosis=-0.3,
        )
        assert cp.skewness == 1.5
        assert cp.kurtosis == -0.3


class TestAdapterSkewnessKurtosis:
    def test_duckdb_returns_sql(self):
        adapter = DuckDBAdapter("duckdb:///test.db")
        assert adapter.skewness_sql("col1") == "skewness(col1)"
        assert adapter.kurtosis_sql("col1") == "kurtosis(col1)"

    def test_sqlite_returns_none(self):
        adapter = SQLiteAdapter("sqlite:///test.db")
        assert adapter.skewness_sql("col1") is None
        assert adapter.kurtosis_sql("col1") is None

    def test_duckdb_correlation_sql(self):
        adapter = DuckDBAdapter("duckdb:///test.db")
        assert adapter.correlation_sql("a", "b") == "corr(a, b)"

    def test_sqlite_correlation_returns_none(self):
        adapter = SQLiteAdapter("sqlite:///test.db")
        assert adapter.correlation_sql("a", "b") is None


# ---- Feature 2: Histograms ----

class TestHistogram:
    @pytest.fixture()
    def duckdb_engine(self, tmp_path):
        """Create a small in-memory DuckDB with test data."""
        import duckdb
        from sqlalchemy import create_engine, text
        db_path = str(tmp_path / "test.duckdb")
        # Create via duckdb first, then connect via sqlalchemy
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE nums (val DOUBLE)")
        conn.execute("INSERT INTO nums SELECT i FROM generate_series(1, 100) t(i)")
        conn.close()
        engine = create_engine(f"duckdb:///{db_path}")
        return engine

    def test_histogram_shape(self, duckdb_engine):
        """Histogram returns bins with correct structure."""
        bins = _compute_histogram(
            duckdb_engine, "nums", "val",
            min_val=1.0, max_val=100.0, num_bins=10,
            sample_clause="", schema=None,
        )
        assert len(bins) > 0
        for b in bins:
            assert "bin_start" in b
            assert "bin_end" in b
            assert "count" in b
            assert b["count"] > 0

    def test_histogram_total_count(self, duckdb_engine):
        """Sum of bin counts equals total non-null rows."""
        bins = _compute_histogram(
            duckdb_engine, "nums", "val",
            min_val=1.0, max_val=100.0, num_bins=10,
            sample_clause="", schema=None,
        )
        total = sum(b["count"] for b in bins)
        assert total == 100

    def test_histogram_min_equals_max(self, duckdb_engine):
        """Single-value column returns one bin."""
        from sqlalchemy import text
        with duckdb_engine.connect() as conn:
            conn.execute(text("CREATE TABLE constant (val DOUBLE)"))
            conn.execute(text("INSERT INTO constant VALUES (5.0), (5.0), (5.0)"))
            conn.commit()
        bins = _compute_histogram(
            duckdb_engine, "constant", "val",
            min_val=5.0, max_val=5.0, num_bins=10,
            sample_clause="", schema=None,
        )
        assert len(bins) == 1
        assert bins[0]["count"] == 3
        assert bins[0]["bin_start"] == 5.0

    def test_histogram_empty_table(self, duckdb_engine):
        """Empty table returns empty or single-zero bin."""
        from sqlalchemy import text
        with duckdb_engine.connect() as conn:
            conn.execute(text("CREATE TABLE empty_nums (val DOUBLE)"))
            conn.commit()
        bins = _compute_histogram(
            duckdb_engine, "empty_nums", "val",
            min_val=0.0, max_val=10.0, num_bins=10,
            sample_clause="", schema=None,
        )
        # Either empty list or bins summing to 0
        total = sum(b["count"] for b in bins) if bins else 0
        assert total == 0


# ---- Feature 3: Correlation ----

class TestCorrelation:
    @pytest.fixture()
    def corr_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "corr.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE corr_test (a DOUBLE, b DOUBLE, c DOUBLE)")
        # a and b perfectly correlated, c is random
        conn.execute("""
            INSERT INTO corr_test
            SELECT i, i * 2.0, i % 7
            FROM generate_series(1, 200) t(i)
        """)
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    def test_perfect_correlation(self, corr_engine):
        adapter = DuckDBAdapter("duckdb:///test.db")
        results = _compute_correlations(
            corr_engine, adapter, "corr_test",
            ["a", "b", "c"], "", None,
        )
        # Find a-b pair
        ab = next((r for r in results if set([r["col1"], r["col2"]]) == {"a", "b"}), None)
        assert ab is not None
        assert abs(ab["pearson"] - 1.0) < 0.01

    def test_low_correlation(self, corr_engine):
        adapter = DuckDBAdapter("duckdb:///test.db")
        results = _compute_correlations(
            corr_engine, adapter, "corr_test",
            ["a", "b", "c"], "", None,
        )
        # a-c or b-c should be low correlation (not perfect)
        ac = next((r for r in results if "c" in [r["col1"], r["col2"]] and "a" in [r["col1"], r["col2"]]), None)
        assert ac is not None
        assert abs(ac["pearson"]) < 0.5

    def test_column_cap(self, corr_engine):
        """Max columns parameter limits pairs."""
        adapter = DuckDBAdapter("duckdb:///test.db")
        # With max_columns=2, only first 2 columns used -> 1 pair
        results = _compute_correlations(
            corr_engine, adapter, "corr_test",
            ["a", "b", "c"], "", None, max_columns=2,
        )
        assert len(results) == 1

    def test_single_column_returns_empty(self, corr_engine):
        adapter = DuckDBAdapter("duckdb:///test.db")
        results = _compute_correlations(
            corr_engine, adapter, "corr_test",
            ["a"], "", None,
        )
        assert results == []


class TestCramersV:
    @pytest.fixture()
    def cat_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "cat.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE cats (x VARCHAR, y VARCHAR)")
        # Perfectly associated: x determines y
        for i in range(100):
            x = "A" if i < 50 else "B"
            y = "1" if i < 50 else "2"
            conn.execute(f"INSERT INTO cats VALUES ('{x}', '{y}')")
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    def test_cramers_v_perfect_association(self, cat_engine):
        v = _compute_cramers_v(cat_engine, "cats", "x", "y", "", None)
        assert v is not None
        assert v > 0.5  # 2x2 perfect association gives V = sqrt(chi2/(n*1)) ~ 0.707

    def test_cramers_v_single_value_guard(self, tmp_path):
        """Single-value column returns 0.0 (division guard)."""
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "single.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE single (x VARCHAR, y VARCHAR)")
        conn.execute("INSERT INTO single VALUES ('A', '1'), ('A', '2'), ('A', '3')")
        conn.close()
        engine = create_engine(f"duckdb:///{db_path}")
        v = _compute_cramers_v(engine, "single", "x", "y", "", None)
        assert v == 0.0


# ---- Feature 4: Benford's Law ----

class TestBenford:
    @pytest.fixture()
    def benford_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "benford.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE benford_test (val BIGINT)")
        # Generate Benford-like distribution (powers of integers)
        vals = []
        for i in range(1, 10):
            count = int(math.log10(1 + 1 / i) * 1000)
            for _ in range(count):
                vals.append(i * 100 + len(vals) % 99)
        conn.executemany("INSERT INTO benford_test VALUES (?)", [(v,) for v in vals])
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    @pytest.fixture()
    def uniform_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "uniform.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE uniform (val BIGINT)")
        # Uniform distribution of leading digits -> should fail Benford
        for d in range(1, 10):
            for j in range(200):
                conn.execute(f"INSERT INTO uniform VALUES ({d * 1000 + j})")
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    def test_benford_conforming_data(self, benford_engine):
        result = _compute_benford(benford_engine, "benford_test", "val", "", None)
        assert result is not None
        digits, pvalue = result
        assert len(digits) == 9
        # Should pass Benford test (p-value > 0.01)
        assert pvalue > 0.01

    def test_uniform_fails_benford(self, uniform_engine):
        result = _compute_benford(uniform_engine, "uniform", "val", "", None)
        assert result is not None
        digits, pvalue = result
        # Uniform distribution should fail Benford test
        assert pvalue < 0.01

    def test_small_column_skipped(self, tmp_path):
        """Column with < 100 values returns None."""
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "small.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE small (val INT)")
        for i in range(50):
            conn.execute(f"INSERT INTO small VALUES ({i + 1})")
        conn.close()
        engine = create_engine(f"duckdb:///{db_path}")
        result = _compute_benford(engine, "small", "val", "", None)
        assert result is None


class TestWilsonHilferty:
    def test_zero_chi2_returns_one(self):
        assert _chi2_pvalue(0, 8) == 1.0

    def test_large_chi2_returns_near_zero(self):
        p = _chi2_pvalue(100, 8)
        assert p < 0.001

    def test_moderate_chi2(self):
        # Chi2 = 15.51 with df=8 -> p ~ 0.05
        p = _chi2_pvalue(15.51, 8)
        assert 0.01 < p < 0.15  # Approximate


# ---- Feature 8: CV (coefficient of variation) ----

class TestCVField:
    def test_cv_field_exists_with_none_default(self):
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True,
        )
        assert cp.cv is None

    def test_cv_field_accepts_float(self):
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True, cv=0.25,
        )
        assert cp.cv == 0.25


# ---- Feature 10: MAD field ----

class TestMADField:
    def test_mad_field_exists_with_none_default(self):
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True,
        )
        assert cp.mad is None

    def test_mad_field_accepts_float(self):
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True, mad=3.5,
        )
        assert cp.mad == 3.5


# ---- Feature 11: Monotonicity fields ----

class TestMonotonicityFields:
    def test_fields_default_none(self):
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True,
        )
        assert cp.is_monotonic_increasing is None
        assert cp.is_monotonic_decreasing is None

    def test_fields_accept_bool(self):
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True,
            is_monotonic_increasing=True, is_monotonic_decreasing=False,
        )
        assert cp.is_monotonic_increasing is True
        assert cp.is_monotonic_decreasing is False


# ---- Feature 12: min_length for strings ----

class TestMinLengthField:
    def test_min_length_default_none(self):
        cp = ColumnProfile(
            name="x", engine_type="VARCHAR", canonical_type="string",
            comment=None, nullable=True,
        )
        assert cp.min_length is None

    def test_min_length_accepts_int(self):
        cp = ColumnProfile(
            name="x", engine_type="VARCHAR", canonical_type="string",
            comment=None, nullable=True, min_length=3,
        )
        assert cp.min_length == 3


# ---- Feature 13: bottom_values ----

class TestBottomValuesField:
    def test_bottom_values_default_none(self):
        cp = ColumnProfile(
            name="x", engine_type="VARCHAR", canonical_type="string",
            comment=None, nullable=True,
        )
        assert cp.bottom_values is None

    def test_bottom_values_accepts_list(self):
        cp = ColumnProfile(
            name="x", engine_type="VARCHAR", canonical_type="string",
            comment=None, nullable=True,
            bottom_values=[{"value": "rare", "count": 1}],
        )
        assert len(cp.bottom_values) == 1
        assert cp.bottom_values[0]["count"] == 1


# ---- Feature 15: infinite_count and whitespace_count fields ----

class TestNewCountFields:
    def test_infinite_count_default_none(self):
        cp = ColumnProfile(
            name="x", engine_type="DOUBLE", canonical_type="float",
            comment=None, nullable=True,
        )
        assert cp.infinite_count is None

    def test_whitespace_count_default_none(self):
        cp = ColumnProfile(
            name="x", engine_type="VARCHAR", canonical_type="string",
            comment=None, nullable=True,
        )
        assert cp.whitespace_count is None

    def test_freshness_days_default_none(self):
        cp = ColumnProfile(
            name="x", engine_type="DATE", canonical_type="date",
            comment=None, nullable=True,
        )
        assert cp.freshness_days is None

    def test_freshness_days_accepts_int(self):
        cp = ColumnProfile(
            name="x", engine_type="DATE", canonical_type="date",
            comment=None, nullable=True, freshness_days=15,
        )
        assert cp.freshness_days == 15


# ---- Feature 14: _compute_mad unit tests ----

class TestComputeMAD:
    def test_known_values(self):
        # For [1, 1, 2, 2, 4, 6, 9], median=2, deviations=[1,1,0,0,2,4,7], median=1
        values = [1, 1, 2, 2, 4, 6, 9]
        median = 2.0
        result = _compute_mad(median, values)
        assert result == 1.0

    def test_returns_none_when_median_none(self):
        assert _compute_mad(None, [1, 2, 3]) is None

    def test_returns_none_for_small_sample(self):
        assert _compute_mad(5.0, [5, 5]) is None  # len < 3


class TestComputeMonotonicity:
    @pytest.fixture()
    def inc_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "mono.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE mono_inc (val DOUBLE)")
        conn.execute("INSERT INTO mono_inc SELECT i FROM generate_series(1, 100) t(i)")
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    @pytest.fixture()
    def dec_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "mono_dec.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE mono_dec (val DOUBLE)")
        conn.execute("INSERT INTO mono_dec SELECT 100 - i FROM generate_series(0, 99) t(i)")
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    def test_increasing_detected(self, inc_engine):
        adapter = DuckDBAdapter("duckdb:///test.db")
        inc, dec = _compute_monotonicity(inc_engine, "mono_inc", "val", "", None, adapter)
        assert inc is True
        assert dec is False

    def test_decreasing_detected(self, dec_engine):
        adapter = DuckDBAdapter("duckdb:///test.db")
        inc, dec = _compute_monotonicity(dec_engine, "mono_dec", "val", "", None, adapter)
        assert inc is False
        assert dec is True


# ---- Feature 9: Spearman correlation ----

class TestSpearmanCorrelation:
    @pytest.fixture()
    def spearman_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "spearman.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE spearman_test (a DOUBLE, b DOUBLE)")
        # a and b are monotonically related (perfect Spearman, imperfect Pearson)
        # b = a^3 — rank-identical, value-nonlinear
        conn.execute("""
            INSERT INTO spearman_test
            SELECT i, i * i * i
            FROM generate_series(1, 100) t(i)
        """)
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    def test_spearman_key_present(self, spearman_engine):
        """Result dict includes 'spearman' key when DuckDB supports CORR."""
        adapter = DuckDBAdapter("duckdb:///test.db")
        results = _compute_correlations(
            spearman_engine, adapter, "spearman_test",
            ["a", "b"], "", None,
        )
        assert len(results) == 1
        assert "spearman" in results[0]

    def test_spearman_perfect_monotone(self, spearman_engine):
        """Perfectly monotone relationship gives Spearman ≈ 1.0."""
        adapter = DuckDBAdapter("duckdb:///test.db")
        results = _compute_correlations(
            spearman_engine, adapter, "spearman_test",
            ["a", "b"], "", None,
        )
        assert len(results) == 1
        assert abs(results[0]["spearman"] - 1.0) < 0.01

    def test_pearson_and_spearman_both_present(self, spearman_engine):
        """Both pearson and spearman keys appear for a DuckDB table."""
        adapter = DuckDBAdapter("duckdb:///test.db")
        results = _compute_correlations(
            spearman_engine, adapter, "spearman_test",
            ["a", "b"], "", None,
        )
        assert len(results) == 1
        assert "pearson" in results[0]
        assert "spearman" in results[0]


# ---- Feature 16: KDE (kernel density estimation) ----

class TestKDEField:
    def test_kde_field_exists_with_none_default(self):
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True,
        )
        assert cp.kde is None

    def test_kde_field_accepts_list(self):
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True,
            kde=[{"x": 1.0, "y": 0.01}],
        )
        assert len(cp.kde) == 1


class TestComputeKDE:
    def test_returns_correct_length(self):
        values = list(range(1, 101))  # 100 uniform integers
        result = _compute_kde(values, n_points=50)
        assert result is not None
        assert len(result) == 50

    def test_each_point_has_x_y(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        result = _compute_kde(values)
        assert result is not None
        for pt in result:
            assert "x" in pt
            assert "y" in pt
            assert pt["y"] >= 0.0

    def test_returns_none_for_small_sample(self):
        assert _compute_kde([1.0, 2.0, 3.0]) is None  # len < 5

    def test_returns_none_for_constant_values(self):
        values = [5.0] * 20  # all same — sigma=0
        assert _compute_kde(values) is None

    def test_peak_near_center_for_normal_like_data(self):
        """KDE of symmetric data should peak near the middle."""
        import statistics
        values = [float(i) for i in range(1, 101)]
        result = _compute_kde(values, n_points=50)
        assert result is not None
        peak_x = max(result, key=lambda p: p["y"])["x"]
        data_mean = statistics.mean(values)
        # Peak should be within 20% of the data range from the mean
        data_range = max(values) - min(values)
        assert abs(peak_x - data_mean) < data_range * 0.2

    def test_integrates_to_approximately_one(self):
        """Trapezoidal integration of KDE over support should ≈ 1."""
        import math
        values = [float(i) for i in range(1, 201)]
        result = _compute_kde(values, n_points=100)
        assert result is not None
        # Trapezoidal rule
        total = 0.0
        for i in range(len(result) - 1):
            dx = result[i + 1]["x"] - result[i]["x"]
            total += 0.5 * (result[i]["y"] + result[i + 1]["y"]) * dx
        # Should integrate to ≈ 1, but KDE tails extend beyond [min, max]
        # so the integral over [min, max] is typically 0.85-0.99
        assert 0.7 < total < 1.1


# ---- Feature 17: Normalized Mutual Information (NMI) ----

class TestComputeNMI:
    @pytest.fixture()
    def nmi_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "nmi.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE nmi_test (x DOUBLE, y DOUBLE, z DOUBLE)")
        # x and y are perfectly correlated (y = x), z is independent random
        conn.execute("""
            INSERT INTO nmi_test
            SELECT i, i, (i * 7 + 3) % 13
            FROM generate_series(1, 200) t(i)
        """)
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    def test_perfectly_correlated_columns_have_high_nmi(self, nmi_engine):
        """NMI of x vs y (y=x) should be high (near 1)."""
        adapter = DuckDBAdapter("duckdb:///test.db")
        nmi = _compute_nmi(nmi_engine, "nmi_test", "x", 1.0, 200.0, "y", 1.0, 200.0, "", None, adapter)
        assert nmi is not None
        assert nmi > 0.8

    def test_returns_none_when_constant(self, nmi_engine):
        """NMI returns None when min == max."""
        adapter = DuckDBAdapter("duckdb:///test.db")
        nmi = _compute_nmi(nmi_engine, "nmi_test", "x", 5.0, 5.0, "y", 1.0, 200.0, "", None, adapter)
        assert nmi is None

    def test_result_in_zero_one_range(self, nmi_engine):
        """NMI is bounded to [0, 1]."""
        adapter = DuckDBAdapter("duckdb:///test.db")
        nmi = _compute_nmi(nmi_engine, "nmi_test", "x", 1.0, 200.0, "z", 3.0, 10.0, "", None, adapter)
        assert nmi is not None
        assert 0.0 <= nmi <= 1.0


# ---- Feature 18: Row-level completeness ----

class TestRowCompletenessFields:
    def test_profiled_table_has_completeness_fields(self):
        """ProfiledTable has row_completeness_min/max/mean with None defaults."""
        pt = ProfiledTable(name="t", comment=None)
        assert pt.row_completeness_min is None
        assert pt.row_completeness_max is None
        assert pt.row_completeness_mean is None

    def test_fields_accept_float_values(self):
        pt = ProfiledTable(
            name="t", comment=None,
            row_completeness_min=0.75,
            row_completeness_max=1.0,
            row_completeness_mean=0.92,
        )
        assert pt.row_completeness_min == 0.75
        assert pt.row_completeness_max == 1.0
        assert pt.row_completeness_mean == 0.92


class TestRowCompletenessIntegration:
    @pytest.fixture()
    def rc_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "rc.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE rc_test (a INTEGER, b INTEGER, c INTEGER)")
        # Row 1: all 3 non-null → completeness 1.0
        # Row 2: b is null → completeness 2/3 ≈ 0.667
        # Row 3: b and c are null → completeness 1/3 ≈ 0.333
        conn.execute("INSERT INTO rc_test VALUES (1, 2, 3), (4, NULL, 6), (7, NULL, NULL)")
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    def test_row_completeness_computed_end_to_end(self, rc_engine, tmp_path):
        from data_profiler.adapters.duckdb import DuckDBAdapter
        from data_profiler.config import ProfilerConfig
        from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
        from data_profiler.workers.stats_worker import profile_table

        adapter = DuckDBAdapter.__new__(DuckDBAdapter)
        adapter._engine = rc_engine
        adapter._dsn = "duckdb:///test.db"

        config = ProfilerConfig(
            engine="duckdb", dsn="duckdb:///:memory:",
            sample_size=0, concurrency=1, stats_depth="full",
            enable_histogram=False, enable_benford=False,
            enable_correlation=False, enable_patterns=False,
            detect_duplicates=False,
        )
        table_schema = TableSchema(
            name="rc_test",
            columns=[
                ColumnSchema("a", "INTEGER", "integer", True),
                ColumnSchema("b", "INTEGER", "integer", True),
                ColumnSchema("c", "INTEGER", "integer", True),
            ],
        )
        result = profile_table(adapter, table_schema, config)

        assert result.row_completeness_min is not None
        assert result.row_completeness_max is not None
        assert result.row_completeness_mean is not None
        # min completeness should be 1/3 ≈ 0.333
        assert abs(result.row_completeness_min - (1 / 3)) < 0.01
        # max completeness should be 1.0
        assert abs(result.row_completeness_max - 1.0) < 0.01
        # mean completeness should be (1.0 + 2/3 + 1/3) / 3 = 2/3 ≈ 0.667
        assert abs(result.row_completeness_mean - (2 / 3)) < 0.01


# ---- Feature 19: Functional dependency detection ----

class TestFunctionalDependencyFields:
    def test_profiled_table_has_fd_field(self):
        """ProfiledTable has functional_dependencies with None default."""
        pt = ProfiledTable(name="t", comment=None)
        assert pt.functional_dependencies is None

    def test_field_accepts_list_of_dicts(self):
        pt = ProfiledTable(
            name="t", comment=None,
            functional_dependencies=[{"from": "dept_id", "to": "dept_name"}],
        )
        assert len(pt.functional_dependencies) == 1
        assert pt.functional_dependencies[0]["from"] == "dept_id"


class TestFunctionalDependencyIntegration:
    @pytest.fixture()
    def fd_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "fd.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("""
            CREATE TABLE fd_test (
                dept_id INTEGER,
                dept_name VARCHAR,
                emp_id INTEGER,
                salary DOUBLE
            )
        """)
        # dept_id → dept_name (each dept_id has exactly one dept_name)
        # dept_name → dept_id (bijective)
        # emp_id does NOT determine dept_id (many employees per dept)
        conn.execute("""
            INSERT INTO fd_test VALUES
            (1, 'Engineering', 101, 90000),
            (1, 'Engineering', 102, 85000),
            (2, 'Marketing',   201, 75000),
            (2, 'Marketing',   202, 80000),
            (3, 'Finance',     301, 95000)
        """)
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    def test_functional_dependency_detected(self, fd_engine, tmp_path):
        from data_profiler.adapters.duckdb import DuckDBAdapter
        from data_profiler.config import ProfilerConfig
        from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
        from data_profiler.workers.stats_worker import profile_table

        adapter = DuckDBAdapter.__new__(DuckDBAdapter)
        adapter._engine = fd_engine
        adapter._dsn = "duckdb:///test.db"

        config = ProfilerConfig(
            engine="duckdb", dsn="duckdb:///:memory:",
            sample_size=0, concurrency=1, stats_depth="full",
            enable_histogram=False, enable_benford=False,
            enable_correlation=False, enable_patterns=False,
            detect_duplicates=False,
        )
        table_schema = TableSchema(
            name="fd_test",
            columns=[
                ColumnSchema("dept_id", "INTEGER", "integer", True),
                ColumnSchema("dept_name", "VARCHAR", "string", True),
                ColumnSchema("emp_id", "INTEGER", "integer", True),
                ColumnSchema("salary", "DOUBLE", "float", True),
            ],
        )
        result = profile_table(adapter, table_schema, config)

        assert result.functional_dependencies is not None
        fd_pairs = {(fd["from"], fd["to"]) for fd in result.functional_dependencies}
        # dept_id → dept_name should be detected
        assert ("dept_id", "dept_name") in fd_pairs
        # dept_name → dept_id should also be detected (bijection)
        assert ("dept_name", "dept_id") in fd_pairs


# ---- Feature 20: Boolean rates and imbalance ----

class TestBooleanRates:
    def test_false_count_derived_from_true_count(self):
        """false_count == non_null - true_count."""
        cp = ColumnProfile(
            name="flag", engine_type="BOOLEAN", canonical_type="boolean",
            comment=None, nullable=True,
            true_count=70, false_count=30, true_rate=0.7, false_rate=0.3,
        )
        assert cp.false_count == 30
        assert cp.true_rate == 0.7
        assert cp.false_rate == 0.3

    def test_imbalance_ratio_balanced(self):
        """50/50 split → imbalance_ratio = 1.0."""
        cp = ColumnProfile(
            name="flag", engine_type="BOOLEAN", canonical_type="boolean",
            comment=None, nullable=True,
            true_count=50, false_count=50, true_rate=0.5, false_rate=0.5,
            imbalance_ratio=1.0,
        )
        assert cp.imbalance_ratio == 1.0

    def test_boolean_rates_integration(self, tmp_path):
        """Profile a boolean column end-to-end and verify rates."""
        import duckdb
        from sqlalchemy import create_engine
        from data_profiler.adapters.duckdb import DuckDBAdapter
        from data_profiler.config import ProfilerConfig
        from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
        from data_profiler.workers.stats_worker import profile_table

        db_path = str(tmp_path / "bool.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE bt (active BOOLEAN)")
        # 7 true, 3 false
        conn.execute("INSERT INTO bt SELECT true FROM generate_series(1,7)")
        conn.execute("INSERT INTO bt SELECT false FROM generate_series(1,3)")
        conn.close()

        engine = create_engine(f"duckdb:///{db_path}")
        adapter = DuckDBAdapter.__new__(DuckDBAdapter)
        adapter._engine = engine
        adapter._dsn = "duckdb:///test.db"
        config = ProfilerConfig(
            engine="duckdb", dsn="duckdb:///:memory:",
            sample_size=0, concurrency=1, stats_depth="full",
            enable_histogram=False, enable_benford=False,
            enable_correlation=False, enable_patterns=False,
            detect_duplicates=False,
        )
        table_schema = TableSchema(
            name="bt",
            columns=[ColumnSchema("active", "BOOLEAN", "boolean", True)],
        )
        result = profile_table(adapter, table_schema, config)
        cp = result.columns[0]
        assert cp.true_count == 7
        assert cp.false_count == 3
        assert abs(cp.true_rate - 0.7) < 1e-6
        assert abs(cp.false_rate - 0.3) < 1e-6
        assert cp.imbalance_ratio is not None
        assert abs(cp.imbalance_ratio - (7 / 3)) < 0.01


# ---- Feature 21: Distinct ratio and PK candidate ----

class TestDistinctRatioAndPKCandidate:
    def test_distinct_ratio_field_exists(self):
        """ColumnProfile has distinct_ratio with None default."""
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True,
        )
        assert cp.distinct_ratio is None

    def test_pk_candidate_default_false(self):
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True,
        )
        assert cp.pk_candidate is False

    def test_distinct_ratio_computed_in_profile(self, tmp_path):
        """distinct_ratio == approx_distinct / sampled_row_count."""
        import duckdb
        from sqlalchemy import create_engine
        from data_profiler.adapters.duckdb import DuckDBAdapter
        from data_profiler.config import ProfilerConfig
        from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
        from data_profiler.workers.stats_worker import profile_table

        db_path = str(tmp_path / "pk.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE pk_test (id INTEGER, cat VARCHAR)")
        # id: all unique (10 distinct / 10 rows → ratio 1.0)
        # cat: 2 distinct values / 10 rows → ratio 0.2
        conn.execute("""
            INSERT INTO pk_test SELECT i, CASE WHEN i <= 5 THEN 'A' ELSE 'B' END
            FROM generate_series(1, 10) t(i)
        """)
        conn.close()

        engine = create_engine(f"duckdb:///{db_path}")
        adapter = DuckDBAdapter.__new__(DuckDBAdapter)
        adapter._engine = engine
        adapter._dsn = "duckdb:///test.db"
        config = ProfilerConfig(
            engine="duckdb", dsn="duckdb:///:memory:",
            sample_size=0, concurrency=1, stats_depth="full",
            enable_histogram=False, enable_benford=False,
            enable_correlation=False, enable_patterns=False,
            detect_duplicates=False,
        )
        table_schema = TableSchema(
            name="pk_test",
            columns=[
                ColumnSchema("id", "INTEGER", "integer", True),
                ColumnSchema("cat", "VARCHAR", "string", True),
            ],
        )
        result = profile_table(adapter, table_schema, config)
        id_cp = next(c for c in result.columns if c.name == "id")
        cat_cp = next(c for c in result.columns if c.name == "cat")

        # id should be identified as PK candidate (HLL may overestimate on small tables)
        assert id_cp.pk_candidate is True
        assert id_cp.distinct_ratio >= 0.9  # close to 1.0, allow HLL error on small datasets
        # cat should not be PK
        assert cat_cp.pk_candidate is False
        assert cat_cp.distinct_ratio < 0.5


# ---- Feature 22: Box plot data ----

class TestBoxPlotData:
    def test_box_plot_field_exists(self):
        """ColumnProfile has box_plot field with None default."""
        cp = ColumnProfile(
            name="x", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True,
        )
        assert cp.box_plot is None

    def test_box_plot_computed_from_percentiles(self, tmp_path):
        """box_plot contains q1, q3, median, lower_fence, upper_fence."""
        import duckdb
        from sqlalchemy import create_engine
        from data_profiler.adapters.duckdb import DuckDBAdapter
        from data_profiler.config import ProfilerConfig
        from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
        from data_profiler.workers.stats_worker import profile_table

        db_path = str(tmp_path / "bp.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE bp_test (val DOUBLE)")
        conn.execute("INSERT INTO bp_test SELECT i FROM generate_series(1, 100) t(i)")
        conn.close()

        engine = create_engine(f"duckdb:///{db_path}")
        adapter = DuckDBAdapter.__new__(DuckDBAdapter)
        adapter._engine = engine
        adapter._dsn = "duckdb:///test.db"
        config = ProfilerConfig(
            engine="duckdb", dsn="duckdb:///:memory:",
            sample_size=0, concurrency=1, stats_depth="full",
            enable_histogram=False, enable_benford=False,
            enable_correlation=False, enable_patterns=False,
            detect_duplicates=False,
        )
        table_schema = TableSchema(
            name="bp_test",
            columns=[ColumnSchema("val", "DOUBLE", "float", True)],
        )
        result = profile_table(adapter, table_schema, config)
        cp = result.columns[0]

        assert cp.box_plot is not None
        bp = cp.box_plot
        assert "q1" in bp and "q3" in bp and "median" in bp
        assert "lower_fence" in bp and "upper_fence" in bp
        # Tukey fences: lower = Q1 - 1.5*IQR, upper = Q3 + 1.5*IQR
        assert bp["lower_fence"] < bp["q1"]
        assert bp["upper_fence"] > bp["q3"]
        assert bp["q1"] < bp["median"] < bp["q3"]


# ---- Feature 23: Sampling stability ----

class TestSamplingStability:
    """Verify that repeated sampling produces stable mean estimates (CV < 5%)."""

    def test_sampling_stability_cv_under_5pct(self, tmp_path):
        """Mean of a numeric column should be stable across repeated samples (CV < 5%)."""
        import duckdb
        from sqlalchemy import create_engine
        from data_profiler.adapters.duckdb import DuckDBAdapter
        from data_profiler.config import ProfilerConfig
        from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
        from data_profiler.workers.stats_worker import profile_table
        import statistics

        db_path = str(tmp_path / "stability.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE stable_test (val DOUBLE)")
        # Insert 100K rows with known mean = 50.5
        conn.execute("INSERT INTO stable_test SELECT i FROM generate_series(1, 100000) t(i)")
        conn.close()

        engine = create_engine(f"duckdb:///{db_path}")
        means = []
        for _ in range(5):
            adapter = DuckDBAdapter.__new__(DuckDBAdapter)
            adapter._engine = engine
            adapter._dsn = "duckdb:///test.db"
            config = ProfilerConfig(
                engine="duckdb", dsn="duckdb:///:memory:",
                sample_size=10000, concurrency=1, stats_depth="full",
                enable_histogram=False, enable_benford=False,
                enable_correlation=False, enable_patterns=False,
                detect_duplicates=False,
            )
            table_schema = TableSchema(
                name="stable_test",
                columns=[ColumnSchema("val", "DOUBLE", "float", True)],
            )
            result = profile_table(adapter, table_schema, config)
            cp = result.columns[0]
            if cp.mean is not None:
                means.append(cp.mean)

        assert len(means) == 5, "All 5 samples should return a mean"
        cv = statistics.stdev(means) / statistics.mean(means) if statistics.mean(means) != 0 else 0
        assert cv < 0.05, f"Sampling CV {cv:.4f} exceeds 5% threshold (means: {means})"


# ---- Feature 24: CDF, Q-Q plot, and string length histogram ----

class TestCDFAndQQPlot:
    @pytest.fixture()
    def numeric_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "dist.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE dist_test (val DOUBLE)")
        conn.execute("INSERT INTO dist_test SELECT i FROM generate_series(1, 200) t(i)")
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    def _profile(self, engine, tmp_path):
        from data_profiler.adapters.duckdb import DuckDBAdapter
        from data_profiler.config import ProfilerConfig
        from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
        from data_profiler.workers.stats_worker import profile_table

        adapter = DuckDBAdapter.__new__(DuckDBAdapter)
        adapter._engine = engine
        adapter._dsn = "duckdb:///test.db"
        config = ProfilerConfig(
            engine="duckdb", dsn="duckdb:///:memory:",
            sample_size=0, concurrency=1, stats_depth="full",
            enable_histogram=True, enable_benford=False,
            enable_correlation=False, enable_patterns=False,
            detect_duplicates=False,
        )
        table_schema = TableSchema(
            name="dist_test",
            columns=[ColumnSchema("val", "DOUBLE", "float", True)],
        )
        return profile_table(adapter, table_schema, config)

    def test_cdf_computed_from_histogram(self, numeric_engine, tmp_path):
        """CDF is derived from histogram; last entry should be 1.0."""
        result = self._profile(numeric_engine, tmp_path)
        cp = result.columns[0]
        assert cp.histogram is not None and len(cp.histogram) > 0
        assert cp.cdf is not None and len(cp.cdf) > 0
        # CDF is monotonically increasing and ends at 1.0
        pcts = [b["cumulative_pct"] for b in cp.cdf]
        for i in range(1, len(pcts)):
            assert pcts[i] >= pcts[i - 1]
        assert abs(pcts[-1] - 1.0) < 1e-6

    def test_cdf_x_matches_histogram_bin_ends(self, numeric_engine, tmp_path):
        """CDF x values correspond to histogram bin_end values."""
        result = self._profile(numeric_engine, tmp_path)
        cp = result.columns[0]
        hist_ends = [b["bin_end"] for b in cp.histogram]
        cdf_xs = [b["x"] for b in cp.cdf]
        assert hist_ends == cdf_xs

    def test_qq_plot_computed(self, numeric_engine, tmp_path):
        """Q-Q plot contains theoretical vs actual pairs."""
        result = self._profile(numeric_engine, tmp_path)
        cp = result.columns[0]
        assert cp.qq_plot is not None and len(cp.qq_plot) > 0
        for point in cp.qq_plot:
            assert "theoretical" in point
            assert "actual" in point

    def test_qq_plot_actual_sorted(self, numeric_engine, tmp_path):
        """Q-Q plot actual values should be monotonically increasing."""
        result = self._profile(numeric_engine, tmp_path)
        cp = result.columns[0]
        actuals = [p["actual"] for p in cp.qq_plot]
        for i in range(1, len(actuals)):
            assert actuals[i] >= actuals[i - 1]


class TestStringLengthHistogram:
    @pytest.fixture()
    def string_engine(self, tmp_path):
        import duckdb
        from sqlalchemy import create_engine
        db_path = str(tmp_path / "str.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE str_test (name VARCHAR)")
        conn.execute("""
            INSERT INTO str_test VALUES
            ('a'), ('bb'), ('ccc'), ('dddd'), ('eeeee'),
            ('ff'), ('ggg'), ('hhhh'), (NULL), ('')
        """)
        conn.close()
        return create_engine(f"duckdb:///{db_path}")

    def test_length_histogram_computed(self, string_engine, tmp_path):
        """String column gets a length_histogram with {length, count} entries."""
        from data_profiler.adapters.duckdb import DuckDBAdapter
        from data_profiler.config import ProfilerConfig
        from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
        from data_profiler.workers.stats_worker import profile_table

        engine = string_engine
        adapter = DuckDBAdapter.__new__(DuckDBAdapter)
        adapter._engine = engine
        adapter._dsn = "duckdb:///test.db"
        config = ProfilerConfig(
            engine="duckdb", dsn="duckdb:///:memory:",
            sample_size=0, concurrency=1, stats_depth="full",
            enable_histogram=True, enable_benford=False,
            enable_correlation=False, enable_patterns=False,
            detect_duplicates=False,
        )
        table_schema = TableSchema(
            name="str_test",
            columns=[ColumnSchema("name", "VARCHAR", "string", True)],
        )
        result = profile_table(adapter, table_schema, config)
        cp = result.columns[0]
        assert cp.length_histogram is not None
        assert len(cp.length_histogram) > 0
        for entry in cp.length_histogram:
            assert "length" in entry
            assert "count" in entry
            assert entry["count"] > 0

    def test_length_histogram_functions(self):
        """Unit test _compute_length_histogram internals via _compute_cdf."""
        from data_profiler.workers.stats_worker import _compute_cdf, _normal_ppf
        # _normal_ppf sanity
        assert abs(_normal_ppf(0.5)) < 1e-6   # median = 0
        assert abs(_normal_ppf(0.975) - 1.96) < 0.01  # 95th percentile z ≈ 1.96
        # _compute_cdf sanity
        hist = [
            {"bin_start": 0, "bin_end": 1, "count": 50},
            {"bin_start": 1, "bin_end": 2, "count": 50},
        ]
        cdf = _compute_cdf(hist)
        assert len(cdf) == 2
        assert abs(cdf[0]["cumulative_pct"] - 0.5) < 1e-6
        assert abs(cdf[1]["cumulative_pct"] - 1.0) < 1e-6


# ---- Feature: Date range + granularity inference ----

class TestDateRangeAndGranularity:
    """date_range_days and granularity_guess on date/datetime columns."""

    def _make_date_table(self, tmp_path, rows: list[str]) -> tuple:
        """Create a SQLite table with a single DATE column."""
        from sqlalchemy import create_engine, text
        from data_profiler.adapters.sqlite import SQLiteAdapter
        from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
        from data_profiler.config import ProfilerConfig
        from data_profiler.workers.stats_worker import profile_table

        engine = create_engine(f"sqlite:///{tmp_path}/dates.db")
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE t (d TEXT)"))
            for r in rows:
                conn.execute(text("INSERT INTO t VALUES (:v)"), {"v": r})
            conn.commit()

        adapter = SQLiteAdapter.__new__(SQLiteAdapter)
        adapter._engine = engine
        adapter._dsn = "sqlite:///dates.db"
        schema = TableSchema(name="t", columns=[ColumnSchema("d", "TEXT", "date", True)])
        config = ProfilerConfig(
            engine="sqlite", dsn="sqlite:///:memory:",
            sample_size=0, concurrency=1, stats_depth="full",
            enable_histogram=False, enable_benford=False,
            enable_correlation=False, enable_patterns=False,
            detect_duplicates=False,
        )
        result = profile_table(adapter, schema, config)
        return result.columns[0]

    def test_date_range_days_computed(self, tmp_path):
        """date_range_days = max - min in days for a date column."""
        # 30 daily rows: 2024-01-01 to 2024-01-30
        rows = [f"2024-01-{str(i).zfill(2)}" for i in range(1, 31)]
        cp = self._make_date_table(tmp_path, rows)
        assert cp.date_range_days is not None
        assert cp.date_range_days == 29  # 2024-01-30 - 2024-01-01 = 29 days

    def test_granularity_daily(self, tmp_path):
        """Daily data: ~1 distinct value per day → granularity_guess = 'daily'."""
        rows = [f"2024-01-{str(i).zfill(2)}" for i in range(1, 31)]
        cp = self._make_date_table(tmp_path, rows)
        assert cp.granularity_guess == "daily"

    def test_granularity_monthly(self, tmp_path):
        """Monthly data: 1 value per month over 2 years → granularity_guess = 'monthly'."""
        rows = []
        for year in [2022, 2023, 2024]:
            for month in range(1, 13):
                rows.append(f"{year}-{str(month).zfill(2)}-01")
        cp = self._make_date_table(tmp_path, rows)
        assert cp.granularity_guess == "monthly"

    def test_date_range_days_none_on_single_value(self, tmp_path):
        """Single date value: range = 0, granularity unknown."""
        cp = self._make_date_table(tmp_path, ["2024-06-15"])
        assert cp.date_range_days == 0
        assert cp.granularity_guess == "unknown"

    def test_new_fields_exist_on_column_profile(self):
        """ColumnProfile has date_range_days and granularity_guess fields."""
        cp = ColumnProfile(
            name="d", engine_type="DATE", canonical_type="date",
            comment=None, nullable=True,
        )
        assert cp.date_range_days is None
        assert cp.granularity_guess is None


# ---- Feature: Unique count and uniqueness ratio ----

class TestUniqueCountAndUniquenessRatio:
    """unique_count = singletons; uniqueness_ratio = unique_count / approx_distinct."""

    def _make_table(self, tmp_path, values: list):
        from sqlalchemy import create_engine, text
        from data_profiler.adapters.sqlite import SQLiteAdapter
        from data_profiler.workers.schema_worker import ColumnSchema, TableSchema
        from data_profiler.config import ProfilerConfig
        from data_profiler.workers.stats_worker import profile_table

        engine = create_engine(f"sqlite:///{tmp_path}/uc.db")
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE t (v INTEGER)"))
            for v in values:
                conn.execute(text("INSERT INTO t VALUES (:v)"), {"v": v})
            conn.commit()

        adapter = SQLiteAdapter.__new__(SQLiteAdapter)
        adapter._engine = engine
        adapter._dsn = "sqlite:///uc.db"
        schema = TableSchema(name="t", columns=[ColumnSchema("v", "INTEGER", "integer", True)])
        config = ProfilerConfig(
            engine="sqlite", dsn="sqlite:///:memory:",
            sample_size=0, concurrency=1, stats_depth="full",
            enable_histogram=False, enable_benford=False,
            enable_correlation=False, enable_patterns=False,
            detect_duplicates=False,
        )
        return profile_table(adapter, schema, config).columns[0]

    def test_unique_count_all_singletons(self, tmp_path):
        """All values are unique → unique_count == len(values)."""
        cp = self._make_table(tmp_path, [1, 2, 3, 4, 5])
        assert cp.unique_count == 5

    def test_unique_count_with_duplicates(self, tmp_path):
        """Values: [1,1,2,3,4] → 3 singletons (2,3,4); value 1 appears twice."""
        cp = self._make_table(tmp_path, [1, 1, 2, 3, 4])
        assert cp.unique_count == 3

    def test_unique_count_no_singletons(self, tmp_path):
        """Values: [1,1,2,2] → 0 singletons."""
        cp = self._make_table(tmp_path, [1, 1, 2, 2])
        assert cp.unique_count == 0

    def test_uniqueness_ratio_is_computed(self, tmp_path):
        """uniqueness_ratio = unique_count / approx_distinct."""
        cp = self._make_table(tmp_path, [1, 2, 3, 4, 5])
        assert cp.uniqueness_ratio is not None
        # All 5 are singletons, approx_distinct = 5 → ratio = 1.0
        assert abs(cp.uniqueness_ratio - 1.0) < 0.05

    def test_new_fields_default_to_none(self):
        cp = ColumnProfile(
            name="v", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True,
        )
        assert cp.unique_count is None
        assert cp.uniqueness_ratio is None


# ---- Feature: Data Quality Score ----

class TestQualityScore:
    def _make_col(self, null_rate=0.0, anomalies=None):
        return ColumnProfile(
            name="c", engine_type="INTEGER", canonical_type="integer",
            comment=None, nullable=True, null_rate=null_rate,
            anomalies=anomalies or [],
        )

    def test_perfect_score(self):
        """Table with no anomalies, no nulls, no dupes → 100."""
        t = ProfiledTable(name="t", comment=None, columns=[self._make_col()])
        assert compute_quality_score(t) == 100.0

    def test_error_table_returns_zero(self):
        t = ProfiledTable(name="t", comment=None, error="boom")
        assert compute_quality_score(t) == 0.0

    def test_empty_columns_returns_100(self):
        t = ProfiledTable(name="t", comment=None, columns=[])
        assert compute_quality_score(t) == 100.0

    def test_anomaly_penalty(self):
        """10 anomalies × 3 pts = 30 penalty → score 70."""
        col = self._make_col(anomalies=["a"] * 10)
        t = ProfiledTable(name="t", comment=None, columns=[col])
        assert compute_quality_score(t) == 70.0

    def test_anomaly_penalty_capped_at_30(self):
        """20 anomalies × 3 = 60 but capped at 30 → score 70."""
        col = self._make_col(anomalies=["a"] * 20)
        t = ProfiledTable(name="t", comment=None, columns=[col])
        assert compute_quality_score(t) == 70.0

    def test_null_penalty(self):
        """avg null rate 0.5 × 40 = 20 penalty → score 80."""
        col = self._make_col(null_rate=0.5)
        t = ProfiledTable(name="t", comment=None, columns=[col])
        assert compute_quality_score(t) == 80.0

    def test_duplicate_penalty(self):
        """duplicate_rate 0.1 × 100 = 10 penalty → score 90."""
        col = self._make_col()
        t = ProfiledTable(name="t", comment=None, columns=[col], duplicate_rate=0.1)
        assert compute_quality_score(t) == 90.0

    def test_combined_penalties(self):
        """Anomalies + nulls + dupes all contribute."""
        col = self._make_col(null_rate=0.25, anomalies=["a", "b"])
        t = ProfiledTable(name="t", comment=None, columns=[col], duplicate_rate=0.05)
        # 100 - 6 (2×3) - 10 (0.25×40) - 5 (0.05×100) = 79
        assert compute_quality_score(t) == 79.0

    def test_score_floors_at_minimum(self):
        """Max penalties: 30 (anomaly) + 20 (null) + 15 (dupe) = 65 → score 35."""
        col = self._make_col(null_rate=1.0, anomalies=["a"] * 20)
        t = ProfiledTable(name="t", comment=None, columns=[col], duplicate_rate=1.0)
        assert compute_quality_score(t) == 35.0

    def test_field_default(self):
        """ProfiledTable.quality_score defaults to 0.0."""
        t = ProfiledTable(name="t", comment=None)
        assert t.quality_score == 0.0
