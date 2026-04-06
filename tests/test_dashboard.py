"""Unit tests for interactive dashboard generation."""

from pathlib import Path

import pytest

from data_profiler.dashboard import generate_dashboard, _serialize_results
from data_profiler.enrichment.constraints import TableConstraints
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


class TestDashboardGeneration:
    def test_generates_html_file(self, tmp_path):
        col = _make_col("id", approx_distinct=500, null_count=10, null_rate=0.01,
                        min=1, max=1000, mean=500.0)
        table = _make_table("users", columns=[col])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="test-1",
            engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table],
            output_path=out,
        )

        assert Path(out).exists()
        content = Path(out).read_text()
        assert "<!DOCTYPE html>" in content
        assert "Data Profiler Dashboard" in content

    def test_embeds_table_data(self, tmp_path):
        col = _make_col("name", canonical_type="string", engine_type="VARCHAR",
                        approx_distinct=100, null_count=0, null_rate=0.0)
        table = _make_table("customers", columns=[col], total_row_count=5000)
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="test-2",
            engine="sqlite",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table],
            output_path=out,
        )

        content = Path(out).read_text()
        assert "customers" in content
        assert "5000" in content

    def test_handles_empty_results(self, tmp_path):
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="test-3",
            engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[],
            output_path=out,
        )

        assert Path(out).exists()
        content = Path(out).read_text()
        assert "<!DOCTYPE html>" in content

    def test_includes_relationships(self, tmp_path):
        table = _make_table("orders", columns=[_make_col("id")])
        rels = [{"source_table": "orders", "source_columns": ["user_id"],
                 "target_table": "users", "target_columns": ["id"],
                 "relationship_type": "declared_fk"}]
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="test-4",
            engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table],
            output_path=out,
            relationships=rels,
        )

        content = Path(out).read_text()
        assert "orders" in content

    def test_includes_anomalies(self, tmp_path):
        col = _make_col("status", canonical_type="string", engine_type="VARCHAR",
                        approx_distinct=1, null_count=0, null_rate=0.0,
                        anomalies=["single_value"])
        table = _make_table("events", columns=[col])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="test-5",
            engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table],
            output_path=out,
        )

        content = Path(out).read_text()
        assert "single_value" in content

    def test_includes_constraints(self, tmp_path):
        constraints = TableConstraints(
            primary_key=["id"],
            foreign_keys=[],
        )
        table = _make_table("users", columns=[_make_col("id")],
                           constraints=constraints)
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="test-6",
            engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table],
            output_path=out,
        )

        content = Path(out).read_text()
        assert "users" in content

    def test_file_size_reasonable(self, tmp_path):
        """Dashboard with a few tables should be under 500KB."""
        tables = []
        for i in range(5):
            cols = [_make_col(f"col_{j}", approx_distinct=j * 10,
                             null_count=j, null_rate=j * 0.01)
                    for j in range(10)]
            tables.append(_make_table(f"table_{i}", columns=cols,
                                     total_row_count=10000 * (i + 1)))
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="test-7",
            engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=tables,
            output_path=out,
        )

        size_kb = Path(out).stat().st_size / 1024
        assert size_kb < 500, f"Dashboard is {size_kb:.0f} KB, expected < 500 KB"


class TestSecurityHardening:
    """Tests for Phase A security fixes."""

    def test_script_tag_injection_in_data(self, tmp_path):
        """A1a: </script> in column data must not break dashboard HTML."""
        col = _make_col("x</script><img onerror=alert(1)>", canonical_type="string",
                        engine_type="VARCHAR", approx_distinct=5, null_count=0, null_rate=0.0)
        table = _make_table("injection_test", columns=[col])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="sec-1", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        # Extract the main script block containing the JSON data
        script_blocks = content.split("<script>")
        # Find the block containing var DATA (the main app script)
        data_block = [b for b in script_blocks if "var DATA" in b]
        assert len(data_block) > 0, "No script block with profiler data found"
        # The JSON data section must not contain raw </script>
        json_section = data_block[0].split("</script>")[0]
        assert "</script>" not in json_section

    def test_html_comment_injection(self, tmp_path):
        """A1a: <!-- in column data must not break out of script context."""
        col = _make_col("test<!--injected-->", canonical_type="string",
                        engine_type="VARCHAR", approx_distinct=5, null_count=0, null_rate=0.0)
        table = _make_table("comment_test", columns=[col])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="sec-2", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        # Raw <!-- must not appear in the JSON data section
        script_section = content.split("<script>")[1].split("</script>")[0]
        assert "<!--" not in script_section

    def test_serialize_escapes_script_tags(self):
        """A1a: _serialize_results escapes dangerous sequences."""
        col = _make_col("test", approx_distinct=1, null_count=0, null_rate=0.0)
        col_with_script = _make_col("</script><img>", canonical_type="string",
                                     engine_type="VARCHAR", approx_distinct=1,
                                     null_count=0, null_rate=0.0)
        table = _make_table("t", columns=[col_with_script])
        json_str = _serialize_results([table])
        assert "</" not in json_str
        assert "<!--" not in json_str

    def test_metadata_html_escaping(self, tmp_path):
        """A1b: run_id, engine, profiled_at are HTML-escaped."""
        col = _make_col("id", approx_distinct=1, null_count=0, null_rate=0.0)
        table = _make_table("t", columns=[col])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id='<script>alert("xss")</script>',
            engine='<img onerror=alert(1)>',
            profiled_at='2026&"<>',
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "<script>alert" not in content
        assert "<img onerror" not in content
        assert "&amp;" in content  # HTML-escaped ampersand

    def test_csp_meta_tag_present(self, tmp_path):
        """A2: CSP meta tag restricts what the HTML file can do."""
        table = _make_table("t", columns=[_make_col("id", approx_distinct=1,
                                                     null_count=0, null_rate=0.0)])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="csp-1", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "Content-Security-Policy" in content
        assert "default-src 'none'" in content


class TestUXFeatures:
    """Tests for Phase C UX polish."""

    def test_theme_toggle_present(self, tmp_path):
        """C1: Dashboard has a theme toggle button."""
        table = _make_table("t", columns=[_make_col("id", approx_distinct=1,
                                                     null_count=0, null_rate=0.0)])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="ux-1", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "theme-toggle" in content
        assert "data-theme" in content

    def test_light_theme_css_vars(self, tmp_path):
        """C1: Light theme CSS variables are defined."""
        table = _make_table("t", columns=[_make_col("id", approx_distinct=1,
                                                     null_count=0, null_rate=0.0)])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="ux-2", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert '[data-theme="light"]' in content
        assert "--bg: #f8f9fa" in content

    def test_print_css(self, tmp_path):
        """C1: Print stylesheet hides sidebar and shows all pages."""
        table = _make_table("t", columns=[_make_col("id", approx_distinct=1,
                                                     null_count=0, null_rate=0.0)])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="ux-3", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "@media print" in content
        assert ".sidebar { display: none" in content

    def test_hash_navigation_support(self, tmp_path):
        """C2: Hash-based deep linking code is present."""
        table = _make_table("t", columns=[_make_col("id", approx_distinct=1,
                                                     null_count=0, null_rate=0.0)])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="ux-4", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "hashchange" in content
        assert "handleHash" in content

    def test_donut_circle_guard(self, tmp_path):
        """B5: SVG donut uses <circle> guard for 360-degree case."""
        table = _make_table("t", columns=[_make_col("id", approx_distinct=1,
                                                     null_count=0, null_rate=0.0)])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="ux-5", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "359.9" in content  # The angle guard threshold


class TestNewFeaturePages:
    """Tests for Feature 2-6 dashboard pages and sections."""

    def test_correlation_page_present(self, tmp_path):
        """Correlations page renders with correlation data."""
        col1 = _make_col("a", approx_distinct=50, null_count=0, null_rate=0.0,
                         min=1, max=100, mean=50.0)
        col2 = _make_col("b", approx_distinct=50, null_count=0, null_rate=0.0,
                         min=1, max=100, mean=50.0)
        table = _make_table("corr_test", columns=[col1, col2],
                            correlations=[{"col1": "a", "col2": "b", "pearson": 0.95}])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="feat-1", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "page-correlations" in content
        assert "Correlations" in content

    def test_missing_page_present(self, tmp_path):
        """Missing values page renders."""
        col = _make_col("nullable_col", approx_distinct=50,
                        null_count=100, null_rate=0.1, min=1, max=100, mean=50.0)
        table = _make_table("missing_test", columns=[col])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="feat-2", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "page-missing" in content
        assert "Missing" in content

    def test_histogram_section_in_detail(self, tmp_path):
        """Histogram section renders when histogram data exists."""
        col = _make_col("amount", approx_distinct=50, null_count=0, null_rate=0.0,
                        min=1, max=100, mean=50.0)
        col.histogram = [
            {"bin_start": 0, "bin_end": 10, "count": 20},
            {"bin_start": 10, "bin_end": 20, "count": 30},
        ]
        table = _make_table("hist_test", columns=[col])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="feat-3", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "histogramSection" in content

    def test_constraint_suggestion_section(self, tmp_path):
        """Constraint suggestion section renders."""
        col = _make_col("id", approx_distinct=1000, null_count=0, null_rate=0.0,
                        min=1, max=1000, mean=500.0)
        table = _make_table("suggest_test", columns=[col],
                            suggested_constraints=[{
                                "table": "suggest_test", "column": "id",
                                "constraint_type": "NOT NULL",
                                "expression": "ALTER TABLE suggest_test ALTER COLUMN id SET NOT NULL",
                                "confidence": 0.95, "evidence": "0 nulls across 1000 rows",
                            }])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="feat-4", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "constraintSuggestSection" in content

    def test_skewness_kurtosis_columns(self, tmp_path):
        """Skewness and kurtosis columns appear in table detail."""
        col = _make_col("val", approx_distinct=50, null_count=0, null_rate=0.0,
                        min=1, max=100, mean=50.0)
        col.skewness = 0.5
        col.kurtosis = -1.2
        table = _make_table("skew_test", columns=[col])
        out = str(tmp_path / "dashboard.html")

        generate_dashboard(
            run_id="feat-5", engine="duckdb",
            profiled_at="2026-04-04T00:00:00Z",
            results=[table], output_path=out,
        )

        content = Path(out).read_text()
        assert "Skew" in content
        assert "Kurt" in content
