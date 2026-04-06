"""Unit tests for OpenMetadata JSON export."""

import json
import tempfile
from pathlib import Path

import pytest

from data_profiler.enrichment.constraints import TableConstraints
from data_profiler.persistence.openmetadata import export_openmetadata
from data_profiler.workers.stats_worker import ColumnProfile, ProfiledTable


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


class TestOpenMetadataExport:
    def test_basic_export_structure(self, tmp_path):
        col = _make_col("id", approx_distinct=500, null_count=10, null_rate=0.01, min=1, max=1000, mean=500.0)
        table = _make_table("users", columns=[col], profiled_at="2026-04-03T00:00:00Z")
        out = str(tmp_path / "export.json")
        export_openmetadata([table], out, run_id="test-1", engine="duckdb")

        data = json.loads(Path(out).read_text())
        assert data["openMetadataExport"] is True
        assert data["version"] == "1.0"
        assert data["runId"] == "test-1"
        assert len(data["tables"]) == 1

        t = data["tables"][0]
        assert t["name"] == "users"
        assert t["tableProfile"]["rowCount"] == 1000
        assert len(t["columns"]) == 1

        cp = t["columns"][0]
        assert cp["name"] == "id"
        assert cp["profile"]["uniqueCount"] == 500
        assert cp["profile"]["min"] == "1"

    def test_excludes_errored_tables(self, tmp_path):
        good = _make_table("good", columns=[_make_col("a")])
        bad = _make_table("bad", columns=[], error="connection failed")
        out = str(tmp_path / "export.json")
        export_openmetadata([good, bad], out)

        data = json.loads(Path(out).read_text())
        assert len(data["tables"]) == 1
        assert data["tables"][0]["name"] == "good"

    def test_includes_constraints(self, tmp_path):
        constraints = TableConstraints(
            primary_key=["id"],
            foreign_keys=[{
                "constrained_columns": ["user_id"],
                "referred_table": "users",
                "referred_columns": ["id"],
                "name": "fk_user",
            }],
        )
        table = _make_table("orders", columns=[_make_col("id")], constraints=constraints)
        out = str(tmp_path / "export.json")
        export_openmetadata([table], out)

        data = json.loads(Path(out).read_text())
        tc = data["tables"][0]["tableConstraints"]
        assert len(tc) == 2
        assert tc[0]["constraintType"] == "PRIMARY_KEY"
        assert tc[0]["columns"] == ["id"]
        assert tc[1]["constraintType"] == "FOREIGN_KEY"

    def test_includes_patterns(self, tmp_path):
        col = _make_col("email", canonical_type="string", engine_type="VARCHAR",
                        patterns=["email"], pattern_scores={"email": 0.92})
        table = _make_table("contacts", columns=[col])
        out = str(tmp_path / "export.json")
        export_openmetadata([table], out)

        data = json.loads(Path(out).read_text())
        profile = data["tables"][0]["columns"][0]["profile"]
        assert "customMetrics" in profile
        assert profile["customMetrics"][0]["name"] == "pattern_email"
        assert profile["customMetrics"][0]["value"] == pytest.approx(0.92)

    def test_includes_relationships(self, tmp_path):
        table = _make_table("orders", columns=[_make_col("id")])
        rels = [{"source_table": "orders", "source_columns": ["user_id"],
                 "target_table": "users", "target_columns": ["id"],
                 "relationship_type": "declared_fk"}]
        out = str(tmp_path / "export.json")
        export_openmetadata([table], out, relationships=rels)

        data = json.loads(Path(out).read_text())
        assert len(data["relationships"]) == 1
        assert data["relationships"][0]["source_table"] == "orders"

    def test_includes_duplicates(self, tmp_path):
        table = _make_table("orders", columns=[_make_col("id")],
                           duplicate_row_count=50, duplicate_rate=0.05)
        out = str(tmp_path / "export.json")
        export_openmetadata([table], out)

        data = json.loads(Path(out).read_text())
        tp = data["tables"][0]["tableProfile"]
        assert tp["duplicateCount"] == 50
        assert tp["duplicateProportion"] == pytest.approx(0.05)

    def test_empty_results(self, tmp_path):
        out = str(tmp_path / "export.json")
        export_openmetadata([], out)

        data = json.loads(Path(out).read_text())
        assert data["tables"] == []
