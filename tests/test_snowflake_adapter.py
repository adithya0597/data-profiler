"""Unit tests for SnowflakeAdapter SQL generation methods (no live connection)."""

from unittest.mock import MagicMock, patch, call

import pytest

from data_profiler.adapters.snowflake import SnowflakeAdapter


DUMMY_DSN = "snowflake://user:pass@account/db"


class TestSnowflakeAdapter:

    @pytest.fixture()
    def adapter(self):
        return SnowflakeAdapter(DUMMY_DSN)

    # ---- sample_clause ----

    def test_sample_clause_uses_bernoulli(self, adapter):
        result = adapter.sample_clause("orders", 1000, 100_000)
        assert "SAMPLE BERNOULLI" in result
        assert "SEED" in result

    def test_sample_clause_full_scan_zero(self, adapter):
        assert adapter.sample_clause("orders", 0, 100_000) == ""

    def test_sample_clause_full_scan_exceeds(self, adapter):
        assert adapter.sample_clause("orders", 100_000, 100_000) == ""
        assert adapter.sample_clause("orders", 200_000, 100_000) == ""

    def test_sample_clause_percentage_calculation(self, adapter):
        # 10,000 / 1,000,000 = 1.0%
        result = adapter.sample_clause("orders", 10_000, 1_000_000)
        assert "1.0000" in result

    def test_sample_clause_no_row_based_seed(self, adapter):
        """Snowflake SEED only works with BERNOULLI/SYSTEM, not row-based ROWS."""
        result = adapter.sample_clause("orders", 1000, 100_000)
        assert "ROWS" not in result

    # ---- SQL generation methods ----

    def test_approx_distinct_sql(self, adapter):
        result = adapter.approx_distinct_sql("col1", "approx_distinct")
        assert "APPROX_COUNT_DISTINCT" in result
        assert "col1" in result
        assert "approx_distinct" in result

    def test_stddev_sql(self, adapter):
        result = adapter.stddev_sql("col1", "stddev_col1")
        assert "STDDEV" in result
        assert "col1" in result
        assert "stddev_col1" in result

    def test_percentile_sql(self, adapter):
        result = adapter.percentile_sql("price", [0.25, 0.75], ["p25", "p75"])
        assert len(result) == 2
        assert "APPROX_PERCENTILE" in result[0]
        assert "APPROX_PERCENTILE" in result[1]
        assert "0.25" in result[0]
        assert "0.75" in result[1]
        assert "p25" in result[0]
        assert "p75" in result[1]

    def test_skewness_sql(self, adapter):
        result = adapter.skewness_sql("col1")
        assert result == "SKEW(col1)"

    def test_kurtosis_sql(self, adapter):
        result = adapter.kurtosis_sql("col1")
        assert result == "KURTOSIS(col1)"

    def test_correlation_sql(self, adapter):
        result = adapter.correlation_sql("col1", "col2")
        assert result == "CORR(col1, col2)"

    # ---- inherited from BaseAdapter ----

    def test_quote_identifier(self, adapter):
        assert adapter.quote_identifier("my_col") == '"my_col"'

    def test_quote_identifier_escapes_double_quote(self, adapter):
        assert adapter.quote_identifier('my"col') == '"my""col"'

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
        assert any("STATEMENT_TIMEOUT_IN_SECONDS = 300" in s for s in executed_stmts)
        assert any("QUERY_TAG" in s for s in executed_stmts)
        mock_conn.commit.assert_called_once()
