"""Unit tests for constraint discovery."""

import pytest
from unittest.mock import MagicMock, patch
from data_profiler.enrichment.constraints import discover_constraints, TableConstraints


def _mock_inspector(
    pk=None, fks=None, uniques=None, checks=None,
    pk_error=False, fk_error=False, unique_error=False, check_error=False,
):
    """Create a mock Inspector with configurable constraint returns."""
    insp = MagicMock()

    if pk_error:
        insp.get_pk_constraint.side_effect = NotImplementedError("PK not supported")
    else:
        insp.get_pk_constraint.return_value = {"constrained_columns": pk or []}

    if fk_error:
        insp.get_foreign_keys.side_effect = NotImplementedError("FK not supported")
    else:
        insp.get_foreign_keys.return_value = fks or []

    if unique_error:
        insp.get_unique_constraints.side_effect = NotImplementedError("UNIQUE not supported")
    else:
        insp.get_unique_constraints.return_value = uniques or []

    if check_error:
        insp.get_check_constraints.side_effect = NotImplementedError("CHECK not supported")
    else:
        insp.get_check_constraints.return_value = checks or []

    return insp


class TestPrimaryKey:
    @patch("data_profiler.enrichment.constraints.inspect")
    def test_discovers_pk(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(pk=["id", "version"])
        result = discover_constraints(MagicMock(), "test_table")
        assert result.primary_key == ["id", "version"]

    @patch("data_profiler.enrichment.constraints.inspect")
    def test_handles_missing_pk(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(pk=[])
        result = discover_constraints(MagicMock(), "test_table")
        assert result.primary_key == []

    @patch("data_profiler.enrichment.constraints.inspect")
    def test_handles_pk_not_supported(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(pk_error=True)
        result = discover_constraints(MagicMock(), "test_table")
        assert result.primary_key == []


class TestForeignKeys:
    @patch("data_profiler.enrichment.constraints.inspect")
    def test_discovers_fk(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(fks=[{
            "constrained_columns": ["customer_id"],
            "referred_table": "customer",
            "referred_columns": ["c_customer_sk"],
            "referred_schema": None,
            "name": "fk_customer",
        }])
        result = discover_constraints(MagicMock(), "orders")
        assert len(result.foreign_keys) == 1
        assert result.foreign_keys[0]["referred_table"] == "customer"
        assert result.foreign_keys[0]["constrained_columns"] == ["customer_id"]

    @patch("data_profiler.enrichment.constraints.inspect")
    def test_multiple_fks(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(fks=[
            {"constrained_columns": ["a"], "referred_table": "t1", "referred_columns": ["id"], "referred_schema": None, "name": "fk1"},
            {"constrained_columns": ["b"], "referred_table": "t2", "referred_columns": ["id"], "referred_schema": None, "name": "fk2"},
        ])
        result = discover_constraints(MagicMock(), "test_table")
        assert len(result.foreign_keys) == 2

    @patch("data_profiler.enrichment.constraints.inspect")
    def test_handles_fk_not_supported(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(fk_error=True)
        result = discover_constraints(MagicMock(), "test_table")
        assert result.foreign_keys == []


class TestUniqueConstraints:
    @patch("data_profiler.enrichment.constraints.inspect")
    def test_discovers_unique(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(uniques=[
            {"name": "uq_email", "column_names": ["email"]},
        ])
        result = discover_constraints(MagicMock(), "users")
        assert len(result.unique_constraints) == 1
        assert result.unique_constraints[0]["columns"] == ["email"]

    @patch("data_profiler.enrichment.constraints.inspect")
    def test_handles_unique_not_supported(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(unique_error=True)
        result = discover_constraints(MagicMock(), "test_table")
        assert result.unique_constraints == []


class TestCheckConstraints:
    @patch("data_profiler.enrichment.constraints.inspect")
    def test_discovers_check(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(checks=[
            {"name": "ck_positive", "sqltext": "amount > 0"},
        ])
        result = discover_constraints(MagicMock(), "payments")
        assert len(result.check_constraints) == 1
        assert "amount > 0" in result.check_constraints[0]["sqltext"]

    @patch("data_profiler.enrichment.constraints.inspect")
    def test_handles_check_not_supported(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(check_error=True)
        result = discover_constraints(MagicMock(), "test_table")
        assert result.check_constraints == []


class TestGracefulDegradation:
    @patch("data_profiler.enrichment.constraints.inspect")
    def test_all_methods_fail_returns_empty(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(
            pk_error=True, fk_error=True, unique_error=True, check_error=True,
        )
        result = discover_constraints(MagicMock(), "test_table")
        assert result.primary_key == []
        assert result.foreign_keys == []
        assert result.unique_constraints == []
        assert result.check_constraints == []

    @patch("data_profiler.enrichment.constraints.inspect")
    def test_partial_support(self, mock_inspect):
        mock_inspect.return_value = _mock_inspector(
            pk=["id"], fk_error=True, check_error=True,
        )
        result = discover_constraints(MagicMock(), "test_table")
        assert result.primary_key == ["id"]
        assert result.foreign_keys == []
        assert result.check_constraints == []
