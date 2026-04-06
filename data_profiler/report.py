"""HTML report generator: self-contained, inline CSS, one file per run."""

from __future__ import annotations

import html as html_lib
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from data_profiler.workers.stats_worker import ProfiledTable

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Data Profile Report — {run_id}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: #f8f9fa; color: #212529; padding: 2rem; line-height: 1.5; }}
  .container {{ max-width: 1200px; margin: 0 auto; }}
  h1 {{ font-size: 1.5rem; margin-bottom: 0.5rem; }}
  .meta {{ color: #6c757d; font-size: 0.875rem; margin-bottom: 2rem; }}
  .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
              gap: 1rem; margin-bottom: 2rem; }}
  .stat-card {{ background: #fff; border-radius: 8px; padding: 1rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .stat-card .label {{ font-size: 0.75rem; color: #6c757d; text-transform: uppercase; letter-spacing: 0.05em; }}
  .stat-card .value {{ font-size: 1.5rem; font-weight: 600; }}
  .table-section {{ background: #fff; border-radius: 8px; padding: 1.5rem; margin-bottom: 1.5rem;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .table-section h2 {{ font-size: 1.1rem; margin-bottom: 0.25rem; }}
  .table-meta {{ color: #6c757d; font-size: 0.8rem; margin-bottom: 1rem; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
  th {{ background: #f1f3f5; text-align: left; padding: 0.5rem; border-bottom: 2px solid #dee2e6;
       font-weight: 600; white-space: nowrap; }}
  td {{ padding: 0.5rem; border-bottom: 1px solid #f1f3f5; }}
  tr:hover td {{ background: #f8f9fa; }}
  .anomaly {{ display: inline-block; background: #fff3cd; color: #856404; padding: 0.1rem 0.4rem;
              border-radius: 4px; font-size: 0.75rem; margin-right: 0.25rem; }}
  .error {{ color: #dc3545; }}
  .type-badge {{ display: inline-block; background: #e9ecef; padding: 0.1rem 0.4rem;
                 border-radius: 4px; font-size: 0.75rem; font-family: monospace; }}
  .null-bar {{ display: inline-block; width: 60px; height: 8px; background: #e9ecef;
               border-radius: 4px; overflow: hidden; vertical-align: middle; }}
  .null-bar-fill {{ height: 100%; border-radius: 4px; }}
  .null-low {{ background: #28a745; }}
  .null-med {{ background: #ffc107; }}
  .null-high {{ background: #dc3545; }}
  .pattern-badge {{ display: inline-block; background: #d4edda; color: #155724; padding: 0.1rem 0.4rem;
                    border-radius: 4px; font-size: 0.75rem; margin-right: 0.25rem; margin-bottom: 0.15rem; }}
  .constraint-info {{ margin-top: 0.75rem; font-size: 0.8rem; color: #495057; }}
  .constraint-info span {{ margin-right: 1rem; }}
  .constraint-tag {{ display: inline-block; background: #cce5ff; color: #004085; padding: 0.1rem 0.4rem;
                     border-radius: 4px; font-size: 0.75rem; margin-right: 0.25rem; }}
  .fk-tag {{ background: #e2d9f3; color: #4a235a; }}
  .rel-section {{ background: #fff; border-radius: 8px; padding: 1.5rem; margin-bottom: 1.5rem;
                  box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .rel-section h2 {{ font-size: 1.1rem; margin-bottom: 1rem; }}
  .rel-badge {{ display: inline-block; padding: 0.15rem 0.5rem; border-radius: 4px;
                font-size: 0.75rem; font-weight: 600; }}
  .rel-declared {{ background: #cce5ff; color: #004085; }}
  .rel-inferred {{ background: #d4edda; color: #155724; }}
</style>
</head>
<body>
<div class="container">
  <h1>Data Profile Report</h1>
  <div class="meta">
    Run ID: {run_id} | Engine: {engine} | {profiled_at}
  </div>

  <div class="summary">
    <div class="stat-card">
      <div class="label">Tables</div>
      <div class="value">{table_count}</div>
    </div>
    <div class="stat-card">
      <div class="label">Columns</div>
      <div class="value">{column_count}</div>
    </div>
    <div class="stat-card">
      <div class="label">Total Rows</div>
      <div class="value">{total_rows}</div>
    </div>
    <div class="stat-card">
      <div class="label">Anomalies</div>
      <div class="value">{anomaly_count}</div>
    </div>
    <div class="stat-card">
      <div class="label">Patterns</div>
      <div class="value">{pattern_count}</div>
    </div>
    <div class="stat-card">
      <div class="label">Relationships</div>
      <div class="value">{relationship_count}</div>
    </div>
    <div class="stat-card">
      <div class="label">Duration</div>
      <div class="value">{duration}s</div>
    </div>
  </div>

  {table_sections}
  {relationship_section}
</div>
</body>
</html>"""

TABLE_SECTION_TEMPLATE = """
  <div class="table-section">
    <h2>{name}</h2>
    <div class="table-meta">
      {comment_html}
      {rows} rows | {col_count} columns | sampled {sampled} | {duration}s{duplicates_html}
    </div>
    {error_html}
    {constraints_html}
    <table>
      <thead>
        <tr>
          <th>Column</th>
          <th>Type</th>
          <th>Canonical</th>
          <th>Nulls</th>
          <th>Distinct</th>
          <th>Min</th>
          <th>Max</th>
          <th>Mean</th>
          <th>Stddev</th>
          <th>Median</th>
          <th>p25 / p75</th>
          <th>IQR</th>
          <th>Skew</th>
          <th>Kurt</th>
          <th>Extra</th>
          <th>Benford</th>
          <th>Patterns</th>
          <th>Anomalies</th>
        </tr>
      </thead>
      <tbody>
        {column_rows}
      </tbody>
    </table>
  </div>
"""


def _null_bar(rate: float) -> str:
    pct = int(rate * 100)
    cls = "null-low" if rate < 0.1 else ("null-med" if rate < 0.5 else "null-high")
    return (
        f'<span class="null-bar"><span class="null-bar-fill {cls}" '
        f'style="width:{pct}%"></span></span> {rate:.0%}'
    )


def _fmt(val: Any) -> str:
    if val is None:
        return '<span style="color:#adb5bd">—</span>'
    if isinstance(val, float):
        return f"{val:,.2f}"
    if isinstance(val, int):
        return f"{val:,}"
    return html_lib.escape(str(val))


def _pattern_badges(col: dict) -> str:
    patterns = col.get("patterns", [])
    scores = col.get("pattern_scores", {})
    if not patterns:
        return "—"
    badges = []
    for p in patterns:
        pct = scores.get(p, 0)
        badges.append(f'<span class="pattern-badge">{html_lib.escape(str(p))} {pct:.0%}</span>')
    return " ".join(badges)


def _benford_badge(col: dict) -> str:
    pval = col.get("benford_pvalue")
    if pval is None:
        return '<span style="color:#adb5bd">—</span>'
    if pval >= 0.01:
        return '<span style="color:#28a745;font-weight:600">Pass</span>'
    return '<span style="color:#dc3545;font-weight:600">Anomaly</span>'


def _column_extra(col: dict) -> str:
    """Type-specific supplementary stats for the Extra column."""
    ct = col.get("canonical_type", "")
    parts = []
    if ct in ("integer", "float"):
        if col.get("sum") is not None:
            parts.append(f"sum={_fmt(col['sum'])}")
        if col.get("zero_count"):
            parts.append(f"zeros={col['zero_count']:,}")
        if col.get("negative_count"):
            parts.append(f"neg={col['negative_count']:,}")
        if col.get("unique_count") is not None:
            ratio = col.get("uniqueness_ratio")
            ratio_str = f" ({ratio:.1%})" if ratio is not None else ""
            parts.append(f"singletons={col['unique_count']:,}{ratio_str}")
    elif ct == "boolean":
        if col.get("true_count") is not None:
            rate = col.get("true_rate")
            rate_str = f" ({rate:.1%})" if rate is not None else ""
            parts.append(f"true={col['true_count']:,}{rate_str}")
    elif ct == "string":
        if col.get("empty_count"):
            parts.append(f"empty={col['empty_count']:,}")
        if col.get("leading_trailing_whitespace_count"):
            parts.append(f"ltws={col['leading_trailing_whitespace_count']:,}")
        if col.get("min_length") is not None:
            parts.append(f"len={col['min_length']}–{col.get('max_length', '?')}")
    elif ct in ("date", "datetime"):
        if col.get("date_range_days") is not None:
            parts.append(f"span={col['date_range_days']}d")
        if col.get("granularity_guess") and col["granularity_guess"] != "unknown":
            parts.append(f"gran={col['granularity_guess']}")
    return html_lib.escape(", ".join(parts)) if parts else "—"


def _column_row(col: dict) -> str:
    anomalies_html = " ".join(f'<span class="anomaly">{html_lib.escape(str(a))}</span>' for a in col.get("anomalies", []))
    p25 = col.get("p25")
    p75 = col.get("p75")
    p25p75 = f"{_fmt(p25)} / {_fmt(p75)}" if p25 is not None or p75 is not None else "—"
    return (
        f"<tr>"
        f"<td><strong>{html_lib.escape(str(col['name']))}</strong></td>"
        f'<td><span class="type-badge">{html_lib.escape(str(col["engine_type"]))}</span></td>'
        f'<td><span class="type-badge">{html_lib.escape(str(col["canonical_type"]))}</span></td>'
        f"<td>{_null_bar(col.get('null_rate', 0))}</td>"
        f"<td>{_fmt(col.get('approx_distinct'))}</td>"
        f"<td>{_fmt(col.get('min'))}</td>"
        f"<td>{_fmt(col.get('max'))}</td>"
        f"<td>{_fmt(col.get('mean'))}</td>"
        f"<td>{_fmt(col.get('stddev'))}</td>"
        f"<td>{_fmt(col.get('median'))}</td>"
        f"<td>{p25p75}</td>"
        f"<td>{_fmt(col.get('iqr'))}</td>"
        f"<td>{_fmt(col.get('skewness'))}</td>"
        f"<td>{_fmt(col.get('kurtosis'))}</td>"
        f"<td>{_column_extra(col)}</td>"
        f"<td>{_benford_badge(col)}</td>"
        f"<td>{_pattern_badges(col)}</td>"
        f"<td>{anomalies_html or '—'}</td>"
        f"</tr>"
    )


def _constraints_html(constraints: Any) -> str:
    """Render constraint badges for a table section."""
    if constraints is None:
        return ""
    # Handle both TableConstraints dataclass and dict form
    if isinstance(constraints, dict):
        pk = constraints.get("primary_key", [])
        fks = constraints.get("foreign_keys", [])
        uqs = constraints.get("unique_constraints", [])
        cks = constraints.get("check_constraints", [])
    else:
        pk = getattr(constraints, "primary_key", [])
        fks = getattr(constraints, "foreign_keys", [])
        uqs = getattr(constraints, "unique_constraints", [])
        cks = getattr(constraints, "check_constraints", [])

    if not (pk or fks or uqs or cks):
        return ""

    parts = []
    if pk:
        parts.append(f'<span class="constraint-tag">PK: {html_lib.escape(", ".join(pk))}</span>')
    for fk in fks:
        cols = html_lib.escape(", ".join(fk.get("constrained_columns", [])))
        ref = html_lib.escape(fk.get("referred_table", "?"))
        parts.append(f'<span class="constraint-tag fk-tag">FK: {cols} → {ref}</span>')
    for uq in uqs:
        cols = html_lib.escape(", ".join(uq.get("columns", uq.get("column_names", []))))
        parts.append(f'<span class="constraint-tag">UNIQUE: {cols}</span>')
    for ck in cks:
        name = html_lib.escape(ck.get("name", "check"))
        parts.append(f'<span class="constraint-tag">{name}</span>')

    return f'<div class="constraint-info">{"  ".join(parts)}</div>'


def _relationship_section(relationships: list[dict[str, Any]]) -> str:
    """Render the cross-table relationships section."""
    if not relationships:
        return ""
    rows = []
    for rel in relationships:
        rtype = rel.get("relationship_type", "unknown")
        cls = "rel-declared" if rtype == "declared_fk" else "rel-inferred"
        label = "Declared FK" if rtype == "declared_fk" else "Inferred"
        src_cols = html_lib.escape(", ".join(rel.get("source_columns", [])))
        tgt_cols = html_lib.escape(", ".join(rel.get("target_columns", [])))
        name = html_lib.escape(rel.get("constraint_name") or "")
        name_cell = f"<td>{name}</td>" if name else "<td>—</td>"
        rows.append(
            f"<tr>"
            f"<td><strong>{html_lib.escape(rel['source_table'])}</strong></td>"
            f"<td>{src_cols}</td>"
            f"<td>→</td>"
            f"<td><strong>{html_lib.escape(rel['target_table'])}</strong></td>"
            f"<td>{tgt_cols}</td>"
            f'<td><span class="rel-badge {cls}">{label}</span></td>'
            f"{name_cell}"
            f"</tr>"
        )
    return (
        '<div class="rel-section">'
        "<h2>Cross-Table Relationships</h2>"
        '<table><thead><tr>'
        "<th>Source Table</th><th>Source Columns</th><th></th>"
        "<th>Target Table</th><th>Target Columns</th><th>Type</th><th>Name</th>"
        "</tr></thead><tbody>"
        + "\n".join(rows)
        + "</tbody></table></div>"
    )


def generate_html_report(
    run_id: str,
    engine: str,
    profiled_at: str,
    results: list[ProfiledTable],
    output_path: str,
    relationships: list[dict[str, Any]] | None = None,
) -> None:
    """Generate a self-contained HTML report from profiling results."""
    table_sections = []
    total_columns = 0
    total_rows = 0
    total_anomalies = 0
    total_patterns = 0
    total_duration = 0.0

    for r in results:
        d = asdict(r)
        total_columns += len(d["columns"])
        total_rows += d["total_row_count"]
        total_anomalies += sum(len(c.get("anomalies", [])) for c in d["columns"])
        total_patterns += sum(1 for c in d["columns"] if c.get("patterns"))
        total_duration += d.get("duration_seconds", 0)

        column_rows = "\n        ".join(_column_row(c) for c in d["columns"])
        comment_html = f"<em>{html_lib.escape(str(d['comment']))}</em> | " if d.get("comment") else ""
        error_html = f'<p class="error">Error: {html_lib.escape(str(d["error"]))}</p>' if d.get("error") else ""
        cstr_html = _constraints_html(d.get("constraints"))
        dup_count = d.get("duplicate_row_count", 0)
        dup_html = f' | <span style="color:#dc3545">{dup_count:,} duplicate rows ({d.get("duplicate_rate", 0):.1%})</span>' if dup_count > 0 else ""

        section = TABLE_SECTION_TEMPLATE.format(
            name=html_lib.escape(d["name"]),
            comment_html=comment_html,
            rows=f"{d['total_row_count']:,}",
            col_count=len(d["columns"]),
            sampled=f"{d['sampled_row_count']:,}",
            duration=f"{d.get('duration_seconds', 0):.1f}",
            error_html=error_html,
            constraints_html=cstr_html,
            duplicates_html=dup_html,
            column_rows=column_rows,
        )
        table_sections.append(section)

    rel_list = relationships or []
    rel_section_html = _relationship_section(rel_list)

    html = HTML_TEMPLATE.format(
        run_id=html_lib.escape(run_id),
        engine=html_lib.escape(engine),
        profiled_at=html_lib.escape(profiled_at),
        table_count=len(results),
        column_count=f"{total_columns:,}",
        total_rows=f"{total_rows:,}",
        anomaly_count=total_anomalies,
        pattern_count=total_patterns,
        relationship_count=len(rel_list),
        duration=f"{total_duration:.1f}",
        table_sections="\n".join(table_sections),
        relationship_section=rel_section_html,
    )

    Path(output_path).write_text(html)
