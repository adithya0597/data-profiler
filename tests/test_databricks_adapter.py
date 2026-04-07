"""Unit tests for DatabricksAdapter SQL generation (no live connection)."""

from unittest.mock import MagicMock

import pytest

from data_profiler.adapters.databricks import DatabricksAdapter

DUMMY_DSN = "databricks://token:xyz@host/db"


class TestDatabricksAdapter:

    @pytest.fixture()
    def adapter(self):
        return DatabricksAdapter(DUMMY_DSN)

    # ---- sample_clause ----

    def test_sample_clause_no_repeatable(self, adapter):
        """Old broken syntax used REPEATABLE; confirm it is gone."""
        result = adapter.sample_clause("orders", 1000, 100_000)
        assert "REPEATABLE" not in result

    def test_sample_clause_uses_tablesample(self, adapter):
        result = adapter.sample_clause("orders", 1000, 100_000)
        assert "TABLESAMPLE" in result

    def test_sample_clause_full_scan_zero(self, adapter):
        assert adapter.sample_clause("orders", 0, 100_000) == ""

    def test_sample_clause_full_scan_exceeds(self, adapter):
        assert adapter.sample_clause("orders", 100_000, 100_000) == ""
        assert adapter.sample_clause("orders", 200_000, 100_000) == ""

    def test_sample_clause_percentage_capped(self, adapter):
        """Even with absurd sample_size > total_rows, pct should not exceed 100."""
        assert adapter.sample_clause("t", 500, 500) == ""
        assert adapter.sample_clause("t", 501, 500) == ""

    def test_sample_clause_percentage_calculation(self, adapter):
        result = adapter.sample_clause("orders", 10_000, 1_000_000)
        assert "1.0000" in result

    def test_sample_clause_no_zero_percent_on_large_tables(self, adapter):
        """Very large tables should not round sampling pct to 0.0000."""
        result = adapter.sample_clause("huge", 10_000, 10_000_000_000)
        assert result != ""
        assert "0.0000" not in result

    # ---- SQL generation methods ----

    def test_approx_distinct_sql(self, adapter):
        result = adapter.approx_distinct_sql("col1", "distinct_count")
        assert "APPROX_COUNT_DISTINCT" in result
        assert "col1" in result
        assert "distinct_count" in result

    def test_stddev_sql(self, adapter):
        result = adapter.stddev_sql("col1", "sd")
        assert result is not None
        assert "STDDEV" in result
        assert "col1" in result
        assert "sd" in result

    def test_percentile_sql(self, adapter):
        result = adapter.percentile_sql(
            "price", [0.25, 0.75], ["p25", "p75"],
        )
        assert len(result) == 2
        assert "PERCENTILE_APPROX" in result[0]
        assert "PERCENTILE_APPROX" in result[1]
        assert "0.25" in result[0]
        assert "0.75" in result[1]
        assert "p25" in result[0]
        assert "p75" in result[1]

    def test_skewness_sql(self, adapter):
        result = adapter.skewness_sql("col1")
        assert result is not None
        assert "SKEWNESS" in result
        assert "col1" in result

    def test_kurtosis_sql(self, adapter):
        result = adapter.kurtosis_sql("col1")
        assert result is not None
        assert "KURTOSIS" in result
        assert "col1" in result

    def test_correlation_sql(self, adapter):
        result = adapter.correlation_sql("col1", "col2")
        assert result is not None
        assert "CORR(col1, col2)" in result

    # ---- quote_identifier ----

    def test_quote_identifier_backticks(self, adapter):
        assert adapter.quote_identifier("my_col") == "`my_col`"

    def test_quote_identifier_escapes_backtick(self, adapter):
        assert adapter.quote_identifier("my`col") == "`my``col`"

    # ---- set_session_params ----

    def test_set_session_params(self, adapter):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_config = MagicMock()
        mock_config.query_timeout = 300

        adapter.set_session_params(mock_engine, mock_config)

        executed_stmts = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
        assert any("spark.sql.session.timeZone" in s for s in executed_stmts)
        mock_conn.commit.assert_called_once()
