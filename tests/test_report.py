"""Unit tests for HTML report generation, focusing on security (A4)."""

from pathlib import Path

import pytest

from data_profiler.report import generate_html_report
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


class TestReportHTMLEscaping:
    """A4: All DB-sourced values must be HTML-escaped in report.py."""

    def test_table_name_html_injection(self, tmp_path):
        table = _make_table('<script>alert("xss")</script>',
                           columns=[_make_col("id", approx_distinct=1,
                                              null_count=0, null_rate=0.0)])
        out = str(tmp_path / "report.html")

        generate_html_report(
            run_id="r-1", engine="duckdb",
            profiled_at="2026-04-04", results=[table],
            output_path=out,
        )

        content = Path(out).read_text()
        assert "<script>alert" not in content
        assert "&lt;script&gt;" in content

    def test_column_name_html_injection(self, tmp_path):
        col = _make_col('<img onerror=alert(1)>', approx_distinct=1,
                        null_count=0, null_rate=0.0)
        table = _make_table("safe_table", columns=[col])
        out = str(tmp_path / "report.html")

        generate_html_report(
            run_id="r-2", engine="duckdb",
            profiled_at="2026-04-04", results=[table],
            output_path=out,
        )

        content = Path(out).read_text()
        assert "<img onerror" not in content
        assert "&lt;img" in content

    def test_run_id_html_injection(self, tmp_path):
        table = _make_table("t", columns=[_make_col("id", approx_distinct=1,
                                                     null_count=0, null_rate=0.0)])
        out = str(tmp_path / "report.html")

        generate_html_report(
            run_id='"><script>xss</script>',
            engine="duckdb",
            profiled_at="2026-04-04",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "<script>xss</script>" not in content

    def test_generates_valid_html(self, tmp_path):
        col = _make_col("amount", canonical_type="float", engine_type="DECIMAL",
                        approx_distinct=100, null_count=5, null_rate=0.005,
                        min=0.0, max=9999.99, mean=500.0)
        table = _make_table("orders", columns=[col], total_row_count=1000)
        out = str(tmp_path / "report.html")

        generate_html_report(
            run_id="r-3", engine="duckdb",
            profiled_at="2026-04-04",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "<!DOCTYPE html>" in content
        assert "orders" in content
