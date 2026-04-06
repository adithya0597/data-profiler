"""Interactive dashboard generator: self-contained HTML with charts and drill-down."""

from __future__ import annotations

import html as html_lib
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from data_profiler.workers.stats_worker import ProfiledTable


def _serialize_results(results: list[ProfiledTable], relationships: list[dict] | None = None) -> str:
    """Serialize profiling results to JSON for embedding in the dashboard."""
    tables = []
    for r in results:
        d = asdict(r)
        # Convert constraints to dict if it's a dataclass
        if d.get("constraints") and not isinstance(d["constraints"], dict):
            d["constraints"] = asdict(d["constraints"])
        tables.append(d)
    data = {"tables": tables, "relationships": relationships or []}
    raw = json.dumps(data, default=str)
    # Prevent </script> tag breakout and HTML comment injection in JSON context
    return raw.replace("</", "<\\/").replace("<!--", "<\\!--")


def generate_dashboard(
    run_id: str,
    engine: str,
    profiled_at: str,
    results: list[ProfiledTable],
    output_path: str,
    relationships: list[dict[str, Any]] | None = None,
) -> None:
    """Generate an interactive HTML dashboard from profiling results."""
    data_json = _serialize_results(results, relationships)

    html = _DASHBOARD_HTML.replace("__DATA_JSON__", data_json)
    html = html.replace("__RUN_ID__", html_lib.escape(run_id))
    html = html.replace("__ENGINE__", html_lib.escape(engine))
    html = html.replace("__PROFILED_AT__", html_lib.escape(profiled_at))

    # Structural guard: the final HTML must have exactly 2 </script> tags
    # (FOUC prevention + main app). If someone adds a literal </script> in
    # a JS comment or string, this catches it at generation time instead of
    # silently breaking the dashboard in the browser.
    script_close_count = html.count("</script>")
    if script_close_count != 2:
        raise ValueError(
            f"Dashboard HTML has {script_close_count} </script> tags, expected 2. "
            f"A literal </script> may have been added in a JS comment or string."
        )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(html)


_DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:;">
<title>Data Profiler Dashboard — __RUN_ID__</title>
<style>
:root {
  --bg: #0f1117;
  --bg-card: #1a1d27;
  --bg-card-hover: #222536;
  --bg-sidebar: #141620;
  --border: #2a2d3a;
  --text: #e4e4e7;
  --text-dim: #71717a;
  --text-bright: #fafafa;
  --accent: #6366f1;
  --accent-light: #818cf8;
  --green: #22c55e;
  --green-bg: rgba(34,197,94,0.1);
  --yellow: #eab308;
  --yellow-bg: rgba(234,179,8,0.1);
  --red: #ef4444;
  --red-bg: rgba(239,68,68,0.1);
  --blue: #3b82f6;
  --blue-bg: rgba(59,130,246,0.1);
  --purple: #a855f7;
  --purple-bg: rgba(168,85,247,0.1);
  --orange: #f97316;
  --cyan: #06b6d4;
  --bg-hover: rgba(255,255,255,0.03);
  --bg-active: rgba(99,102,241,0.08);
  --border-subtle: rgba(42,45,58,0.5);
  --bg-subtle: rgba(255,255,255,0.02);
  --bg-track: rgba(255,255,255,0.06);
  --bg-track-alt: rgba(255,255,255,0.04);
}

[data-theme="light"] {
  --bg: #f8f9fa;
  --bg-card: #ffffff;
  --bg-card-hover: #f1f3f5;
  --bg-sidebar: #f1f3f5;
  --border: #dee2e6;
  --text: #212529;
  --text-dim: #6c757d;
  --text-bright: #0d0d0f;
  --accent: #4f46e5;
  --accent-light: #6366f1;
  --green: #16a34a;
  --green-bg: rgba(22,163,74,0.1);
  --yellow: #ca8a04;
  --yellow-bg: rgba(202,138,4,0.1);
  --red: #dc2626;
  --red-bg: rgba(220,38,38,0.1);
  --blue: #2563eb;
  --blue-bg: rgba(37,99,235,0.1);
  --purple: #9333ea;
  --purple-bg: rgba(147,51,234,0.1);
  --orange: #ea580c;
  --cyan: #0891b2;
  --bg-hover: rgba(0,0,0,0.04);
  --bg-active: rgba(79,70,229,0.08);
  --border-subtle: rgba(0,0,0,0.1);
  --bg-subtle: rgba(0,0,0,0.03);
  --bg-track: rgba(0,0,0,0.07);
  --bg-track-alt: rgba(0,0,0,0.05);
}

* { margin: 0; padding: 0; box-sizing: border-box; }

body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, Roboto, sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.5;
  display: flex;
  min-height: 100vh;
  overflow-x: hidden;
}

/* Sidebar */
.sidebar {
  width: 240px;
  background: var(--bg-sidebar);
  border-right: 1px solid var(--border);
  padding: 1.5rem 0;
  position: fixed;
  top: 0;
  left: 0;
  height: 100vh;
  z-index: 100;
  display: flex;
  flex-direction: column;
}

.sidebar-brand {
  padding: 0 1.25rem 1.5rem;
  border-bottom: 1px solid var(--border);
}

.sidebar-brand h1 {
  font-size: 1rem;
  font-weight: 700;
  color: var(--text-bright);
  letter-spacing: -0.02em;
}

.sidebar-brand .sub {
  font-size: 0.75rem;
  color: var(--text-dim);
  margin-top: 0.25rem;
}

.sidebar-nav {
  padding: 1rem 0;
  flex: 1;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.6rem 1.25rem;
  cursor: pointer;
  color: var(--text-dim);
  font-size: 0.875rem;
  font-weight: 500;
  transition: all 0.15s;
  border-left: 3px solid transparent;
}

.nav-item:hover { color: var(--text); background: var(--bg-hover); }
.nav-item.active {
  color: var(--accent-light);
  background: var(--bg-active);
  border-left-color: var(--accent);
}

.nav-icon { font-size: 1rem; width: 20px; text-align: center; }

.sidebar-footer {
  padding: 1rem 1.25rem;
  border-top: 1px solid var(--border);
  font-size: 0.7rem;
  color: var(--text-dim);
}

/* Main content */
.main {
  margin-left: 240px;
  flex: 1;
  padding: 2rem;
  max-width: calc(100vw - 240px);
}

.page { display: none; }
.page.active { display: block; }

/* Header bar */
.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1.5rem;
}

.header h2 {
  font-size: 1.25rem;
  font-weight: 700;
  color: var(--text-bright);
}

.search-box {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 0.5rem 0.75rem;
  width: 300px;
}

.search-box input {
  background: transparent;
  border: none;
  color: var(--text);
  font-size: 0.875rem;
  width: 100%;
  outline: none;
}

.search-box input::placeholder { color: var(--text-dim); }

/* KPI Cards */
.kpi-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 1rem;
  margin-bottom: 2rem;
}

.kpi-card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 1.25rem;
  transition: transform 0.15s, border-color 0.15s;
}

.kpi-card:hover { transform: translateY(-2px); border-color: var(--accent); }

.kpi-label {
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--text-dim);
  margin-bottom: 0.5rem;
}

.kpi-value {
  font-size: 1.75rem;
  font-weight: 700;
  color: var(--text-bright);
  line-height: 1;
}

.kpi-sub {
  font-size: 0.75rem;
  color: var(--text-dim);
  margin-top: 0.35rem;
}

/* Charts area */
.charts-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.5rem;
  margin-bottom: 2rem;
}

.chart-card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 1.25rem;
}

.chart-card h3 {
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--text-bright);
  margin-bottom: 1rem;
}

/* Data table */
.data-table-wrap {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
  margin-bottom: 1.5rem;
}

.data-table-wrap h3 {
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--text-bright);
  padding: 1rem 1.25rem 0;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.8rem;
}

th {
  text-align: left;
  padding: 0.65rem 1rem;
  border-bottom: 1px solid var(--border);
  font-weight: 600;
  color: var(--text-dim);
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
}

th:hover { color: var(--text); }
th.sorted-asc::after { content: " \25B2"; color: var(--accent-light); }
th.sorted-desc::after { content: " \25BC"; color: var(--accent-light); }

td {
  padding: 0.6rem 1rem;
  border-bottom: 1px solid var(--border-subtle);
  white-space: nowrap;
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
}

tr:hover td { background: var(--bg-subtle); }

/* Badges */
.badge {
  display: inline-block;
  padding: 0.15rem 0.5rem;
  border-radius: 9999px;
  font-size: 0.7rem;
  font-weight: 600;
}

.badge-green { background: var(--green-bg); color: var(--green); }
.badge-yellow { background: var(--yellow-bg); color: var(--yellow); }
.badge-red { background: var(--red-bg); color: var(--red); }
.badge-blue { background: var(--blue-bg); color: var(--blue); }
.badge-purple { background: var(--purple-bg); color: var(--purple); }

.type-chip {
  display: inline-block;
  padding: 0.1rem 0.45rem;
  border-radius: 4px;
  font-size: 0.7rem;
  font-family: "SF Mono", Monaco, Consolas, monospace;
  background: var(--bg-track);
  color: var(--text-dim);
}

/* Null bar */
.null-bar-container {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
}

.null-bar {
  width: 50px;
  height: 6px;
  background: var(--bg-track);
  border-radius: 3px;
  overflow: hidden;
}

.null-bar-fill { height: 100%; border-radius: 3px; }
.null-fill-low { background: var(--green); }
.null-fill-med { background: var(--yellow); }
.null-fill-high { background: var(--red); }

/* Quality score */
.quality-ring {
  width: 56px;
  height: 56px;
  position: relative;
}

.quality-ring svg { transform: rotate(-90deg); }

.quality-ring .score-text {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  font-size: 0.8rem;
  font-weight: 700;
}

/* Anomaly pill */
.anomaly-pill {
  display: inline-block;
  background: var(--yellow-bg);
  color: var(--yellow);
  padding: 0.1rem 0.4rem;
  border-radius: 4px;
  font-size: 0.68rem;
  margin-right: 0.2rem;
  margin-bottom: 0.15rem;
}

/* Pattern pill */
.pattern-pill {
  display: inline-block;
  background: var(--purple-bg);
  color: var(--purple);
  padding: 0.1rem 0.4rem;
  border-radius: 4px;
  font-size: 0.68rem;
  margin-right: 0.2rem;
}

/* Detail panel */
.detail-panel {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 1.5rem;
  margin-bottom: 1.5rem;
}

.detail-panel h3 {
  font-size: 1rem;
  font-weight: 700;
  color: var(--text-bright);
  margin-bottom: 1rem;
}

.detail-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
}

.detail-stat {
  padding: 0.75rem;
  background: var(--bg-subtle);
  border-radius: 8px;
}

.detail-stat .label { font-size: 0.7rem; color: var(--text-dim); text-transform: uppercase; }
.detail-stat .val { font-size: 1.1rem; font-weight: 600; color: var(--text-bright); margin-top: 0.15rem; }

/* Constraint badges */
.cstr-badge {
  display: inline-block;
  padding: 0.1rem 0.5rem;
  border-radius: 4px;
  font-size: 0.7rem;
  font-weight: 600;
  margin-right: 0.3rem;
}

.cstr-pk { background: var(--blue-bg); color: var(--blue); }
.cstr-fk { background: var(--purple-bg); color: var(--purple); }
.cstr-uq { background: var(--green-bg); color: var(--green); }

/* Relationship cards */
.rel-card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 1rem;
  margin-bottom: 0.75rem;
  display: flex;
  align-items: center;
  gap: 1rem;
}

.rel-arrow {
  color: var(--accent-light);
  font-size: 1.2rem;
  flex-shrink: 0;
}

.rel-table { font-weight: 600; color: var(--text-bright); }
.rel-cols { font-size: 0.8rem; color: var(--text-dim); font-family: monospace; }

/* Bar chart */
.bar-chart { display: flex; flex-direction: column; gap: 0.4rem; }

.bar-row {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.bar-label {
  width: 80px;
  font-size: 0.75rem;
  color: var(--text-dim);
  text-align: right;
  flex-shrink: 0;
}

.bar-track {
  flex: 1;
  height: 22px;
  background: var(--bg-track-alt);
  border-radius: 4px;
  overflow: hidden;
  position: relative;
}

.bar-fill {
  height: 100%;
  border-radius: 4px;
  transition: width 0.4s ease;
}

.bar-value {
  position: absolute;
  right: 8px;
  top: 50%;
  transform: translateY(-50%);
  font-size: 0.7rem;
  font-weight: 600;
  color: var(--text-bright);
}

/* Empty state */
.empty-state {
  text-align: center;
  padding: 3rem;
  color: var(--text-dim);
}

/* Scrollable */
.scroll-x { overflow-x: auto; }

/* Responsive */
@media (max-width: 1024px) {
  .charts-row { grid-template-columns: 1fr; }
}

@media (max-width: 768px) {
  .sidebar { display: none; }
  .main { margin-left: 0; }
  .kpi-grid { grid-template-columns: repeat(2, 1fr); }
}

/* Theme toggle button */
.theme-toggle {
  cursor: pointer;
  padding: 0.5rem 1rem;
  font-size: 0.8rem;
  color: var(--text-dim);
  display: flex;
  align-items: center;
  gap: 0.5rem;
  border: none;
  background: none;
  width: 100%;
  text-align: left;
}
.theme-toggle:hover { color: var(--text); }

/* Print styles */
@media print {
  body { background: #fff; color: #000; }
  .sidebar { display: none !important; }
  .main { margin-left: 0 !important; max-width: 100% !important; }
  .page { display: block !important; page-break-inside: avoid; }
  .kpi-card, .chart-card, .data-table-wrap, .detail-panel, .rel-card {
    background: #fff !important; border: 1px solid #ccc !important; color: #000 !important;
  }
  .kpi-value, .detail-stat .val, .rel-table, .chart-card h3, .header h2 {
    color: #000 !important;
  }
  .kpi-label, .detail-stat .label, .bar-label, .rel-cols {
    color: #555 !important;
  }
  th { color: #333 !important; }
  td { border-bottom-color: #ccc !important; color: #000 !important; }
  .theme-toggle { display: none !important; }
  .search-box { display: none !important; }
}
</style>
<script>
// FOUC prevention: apply saved theme before first paint
(function() {
  try {
    var saved = localStorage.getItem("dp-theme");
    if (saved === "light") document.documentElement.setAttribute("data-theme", "light");
  } catch(e) { /* localStorage unavailable in file:// context */ }
})();
</script>
</head>
<body>

<nav class="sidebar">
  <div class="sidebar-brand">
    <h1>Data Profiler</h1>
    <div class="sub">__ENGINE__ / __RUN_ID__</div>
  </div>
  <div class="sidebar-nav">
    <div class="nav-item active" data-page="overview">
      <span class="nav-icon">&#9636;</span> Overview
    </div>
    <div class="nav-item" data-page="tables">
      <span class="nav-icon">&#9638;</span> Tables
    </div>
    <div class="nav-item" data-page="columns">
      <span class="nav-icon">&#9783;</span> Column Explorer
    </div>
    <div class="nav-item" data-page="quality">
      <span class="nav-icon">&#9733;</span> Data Quality
    </div>
    <div class="nav-item" data-page="correlations">
      <span class="nav-icon">&#10227;</span> Correlations
    </div>
    <div class="nav-item" data-page="missing">
      <span class="nav-icon">&#9744;</span> Missing Values
    </div>
    <div class="nav-item" data-page="patterns">
      <span class="nav-icon">&#10070;</span> Patterns & PII
    </div>
    <div class="nav-item" data-page="relationships">
      <span class="nav-icon">&#10132;</span> Relationships
    </div>
  </div>
  <button class="theme-toggle" id="theme-toggle" title="Toggle light/dark theme">
    <span id="theme-icon">&#9789;</span> <span id="theme-label">Light mode</span>
  </button>
  <div class="sidebar-footer">
    Profiled __PROFILED_AT__
  </div>
</nav>

<div class="main">
  <!-- OVERVIEW -->
  <div id="page-overview" class="page active"></div>

  <!-- TABLES -->
  <div id="page-tables" class="page"></div>

  <!-- COLUMNS -->
  <div id="page-columns" class="page"></div>

  <!-- QUALITY -->
  <div id="page-quality" class="page"></div>

  <!-- CORRELATIONS -->
  <div id="page-correlations" class="page"></div>

  <!-- MISSING VALUES -->
  <div id="page-missing" class="page"></div>

  <!-- PATTERNS -->
  <div id="page-patterns" class="page"></div>

  <!-- RELATIONSHIPS -->
  <div id="page-relationships" class="page"></div>
</div>

<script>
// ============================================================
// DATA — injected by Python at generation time.
// All values originate from our own profiler output (trusted).
// ============================================================
var DATA = __DATA_JSON__;
var TABLES = DATA.tables;
var RELS = DATA.relationships;

// ============================================================
// HELPERS
// ============================================================
function esc(s) {
  // Escape HTML entities for safe text rendering
  if (s == null) return "";
  var d = document.createElement("div");
  d.textContent = String(s);
  return d.innerHTML;
}

function fmt(v) {
  if (v == null) return "\u2014";
  if (typeof v === "number") return v.toLocaleString(undefined, {maximumFractionDigits: 2});
  return esc(String(v));
}

function pct(v) {
  if (v == null) return "\u2014";
  return (v * 100).toFixed(1) + "%";
}

// Quality score formula constants
var QS_ANOMALY_PENALTY_PER = 3;       // points deducted per anomaly
var QS_ANOMALY_PENALTY_MAX = 30;      // max anomaly penalty
var QS_NULL_PENALTY_WEIGHT = 40;      // multiplier for avg null rate
var QS_NULL_PENALTY_MAX = 20;         // max null penalty
var QS_DUPE_PENALTY_WEIGHT = 100;     // multiplier for duplicate rate
var QS_DUPE_PENALTY_MAX = 15;         // max duplicate penalty
// Buckets: 90+ excellent, 75-89 good, 50-74 fair, <50 poor
var QS_EXCELLENT = 90;
var QS_GOOD = 75;
var QS_FAIR = 50;

function qualityScore(t) {
  if (t.error) return 0;
  var cols = t.columns || [];
  if (cols.length === 0) return 100;
  var score = 100;
  var totalAnomalies = cols.reduce(function(s, c) { return s + (c.anomalies || []).length; }, 0);
  var anomalyPenalty = Math.min(QS_ANOMALY_PENALTY_MAX, totalAnomalies * QS_ANOMALY_PENALTY_PER);
  score -= anomalyPenalty;
  var avgNull = cols.reduce(function(s, c) { return s + (c.null_rate || 0); }, 0) / cols.length;
  var nullPenalty = Math.min(QS_NULL_PENALTY_MAX, avgNull * QS_NULL_PENALTY_WEIGHT);
  score -= nullPenalty;
  var dupePenalty = 0;
  if (t.duplicate_rate > 0) { dupePenalty = Math.min(QS_DUPE_PENALTY_MAX, t.duplicate_rate * QS_DUPE_PENALTY_WEIGHT); score -= dupePenalty; }
  var result = Math.max(0, Math.round(score));
  if (isNaN(result)) result = 0;
  // Store breakdown for tooltip
  t._qBreakdown = {anomalyPenalty: Math.round(anomalyPenalty), nullPenalty: Math.round(nullPenalty), dupePenalty: Math.round(dupePenalty)};
  return result;
}

function qualityTooltip(t) {
  var b = t._qBreakdown || {anomalyPenalty:0, nullPenalty:0, dupePenalty:0};
  return "Quality: 100 - " + b.anomalyPenalty + " (anomalies) - " + b.nullPenalty + " (nulls) - " + b.dupePenalty + " (dupes)";
}

function scoreColor(s) {
  if (s >= QS_GOOD) return "var(--green)";
  if (s >= QS_FAIR) return "var(--yellow)";
  return "var(--red)";
}

function scoreBadgeHTML(s) {
  var cls = s >= 80 ? "badge-green" : s >= 60 ? "badge-yellow" : "badge-red";
  var span = document.createElement("span");
  span.className = "badge " + cls;
  span.textContent = s;
  return span.outerHTML;
}

function nullBarHTML(rate) {
  var w = Math.round((rate || 0) * 100);
  var cls = rate < 0.1 ? "null-fill-low" : rate < 0.5 ? "null-fill-med" : "null-fill-high";
  var container = document.createElement("span");
  container.className = "null-bar-container";

  var bar = document.createElement("span");
  bar.className = "null-bar";
  var fill = document.createElement("span");
  fill.className = "null-bar-fill " + cls;
  fill.style.width = w + "%";
  bar.appendChild(fill);

  var label = document.createElement("span");
  label.style.fontSize = "0.72rem";
  label.textContent = pct(rate);

  container.appendChild(bar);
  container.appendChild(label);
  return container.outerHTML;
}

function qualityRingSVG(score, size) {
  size = size || 56;
  var r = (size / 2) - 4;
  var circ = 2 * Math.PI * r;
  var offset = circ * (1 - score / 100);
  var color = scoreColor(score);
  return '<div class="quality-ring" style="width:' + size + 'px;height:' + size + 'px">'
    + '<svg width="' + size + '" height="' + size + '">'
    + '<circle cx="' + (size/2) + '" cy="' + (size/2) + '" r="' + r + '" fill="none" stroke="' + getComputedStyle(document.documentElement).getPropertyValue("--bg-track").trim() + '" stroke-width="4"/>'
    + '<circle cx="' + (size/2) + '" cy="' + (size/2) + '" r="' + r + '" fill="none" stroke="' + color + '" stroke-width="4"'
    + ' stroke-dasharray="' + circ + '" stroke-dashoffset="' + offset + '" stroke-linecap="round"/>'
    + '</svg>'
    + '<span class="score-text" style="color:' + color + '">' + score + '</span>'
    + '</div>';
}

function typeColor(t) {
  var map = {
    integer: "var(--blue)", float: "var(--cyan)", string: "var(--green)",
    date: "var(--orange)", datetime: "var(--yellow)", boolean: "var(--purple)",
    binary: "var(--red)", unknown: "var(--text-dim)"
  };
  return map[t] || "var(--text-dim)";
}

// ============================================================
// COMPUTED DATA
// ============================================================
var totalTables = TABLES.length;
var totalCols = TABLES.reduce(function(s, t) { return s + (t.columns || []).length; }, 0);
var totalRows = TABLES.reduce(function(s, t) { return s + (t.total_row_count || 0); }, 0);
var totalAnomalies = TABLES.reduce(function(s, t) {
  return s + (t.columns || []).reduce(function(a, c) { return a + (c.anomalies || []).length; }, 0);
}, 0);
var totalPatterns = TABLES.reduce(function(s, t) {
  return s + (t.columns || []).filter(function(c) { return c.patterns && c.patterns.length > 0; }).length;
}, 0);
var totalDuration = TABLES.reduce(function(s, t) { return s + (t.duration_seconds || 0); }, 0);
var errorTables = TABLES.filter(function(t) { return t.error; }).length;
var avgQuality = TABLES.length > 0
  ? Math.round(TABLES.reduce(function(s, t) { return s + qualityScore(t); }, 0) / TABLES.length)
  : 100;

var typeCounts = {};
TABLES.forEach(function(t) {
  (t.columns || []).forEach(function(c) {
    typeCounts[c.canonical_type] = (typeCounts[c.canonical_type] || 0) + 1;
  });
});

var anomalyCounts = {};
TABLES.forEach(function(t) {
  (t.columns || []).forEach(function(c) {
    (c.anomalies || []).forEach(function(a) { anomalyCounts[a] = (anomalyCounts[a] || 0) + 1; });
  });
});

var allColumns = [];
TABLES.forEach(function(t) {
  (t.columns || []).forEach(function(c) {
    var col = {};
    for (var k in c) col[k] = c[k];
    col._table = t.name;
    col._tableRows = t.total_row_count;
    allColumns.push(col);
  });
});

// ============================================================
// NAVIGATION
// ============================================================
function navigateTo(pageName) {
  document.querySelectorAll(".nav-item").forEach(function(n) { n.classList.remove("active"); });
  document.querySelectorAll(".page").forEach(function(p) { p.classList.remove("active"); });
  var navItem = document.querySelector('.nav-item[data-page="' + pageName + '"]');
  var pageEl = document.getElementById("page-" + pageName);
  if (navItem && pageEl) {
    navItem.classList.add("active");
    pageEl.classList.add("active");
    document.querySelector(".main").scrollTop = 0;
  }
}

document.querySelectorAll(".nav-item").forEach(function(item) {
  item.addEventListener("click", function() {
    var page = item.dataset.page;
    navigateTo(page);
    try { window.location.hash = "#" + page; } catch(e) {}
  });
});

// Hash-based deep linking
function handleHash() {
  var hash = (window.location.hash || "").replace("#", "").toLowerCase();
  var valid = ["overview", "tables", "columns", "quality", "correlations", "missing", "patterns", "relationships"];
  if (valid.indexOf(hash) !== -1) navigateTo(hash);
}
window.addEventListener("hashchange", handleHash);

// Theme toggle
(function() {
  var btn = document.getElementById("theme-toggle");
  var icon = document.getElementById("theme-icon");
  var label = document.getElementById("theme-label");
  function isLight() { return document.documentElement.getAttribute("data-theme") === "light"; }
  function updateUI() {
    if (isLight()) { icon.textContent = "\u2600"; label.textContent = "Dark mode"; }
    else { icon.textContent = "\u263D"; label.textContent = "Light mode"; }
  }
  updateUI();
  btn.addEventListener("click", function() {
    if (isLight()) { document.documentElement.removeAttribute("data-theme"); }
    else { document.documentElement.setAttribute("data-theme", "light"); }
    try { localStorage.setItem("dp-theme", isLight() ? "light" : "dark"); } catch(e) {}
    updateUI();
  });
})();

// ============================================================
// SAFE DOM BUILDER
// ============================================================
function setContent(el, html) {
  // Data originates from profiled databases, which IS user content.
  // All dynamic values pass through esc() (DOM textContent round-trip) before insertion.
  // JSON payload is escaped server-side to prevent script-tag breakout.
  el.innerHTML = html;
}

// ============================================================
// RENDER: OVERVIEW
// ============================================================
function renderOverview() {
  var el = document.getElementById("page-overview");

  var typeEntries = Object.entries(typeCounts).sort(function(a, b) { return b[1] - a[1]; });
  var donutSlices = "";
  var donutLegend = "";
  var cumAngle = 0;
  typeEntries.forEach(function(entry) {
    var type = entry[0], count = entry[1];
    var frac = count / totalCols;
    var angle = frac * 360;
    var startRad = (cumAngle - 90) * Math.PI / 180;
    var endRad = (cumAngle + angle - 90) * Math.PI / 180;
    var large = angle > 180 ? 1 : 0;
    var r = 80, cx = 100, cy = 100;
    var x1 = cx + r * Math.cos(startRad), y1 = cy + r * Math.sin(startRad);
    var x2 = cx + r * Math.cos(endRad), y2 = cy + r * Math.sin(endRad);
    if (angle >= 359.9) {
      donutSlices += '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="' + typeColor(type) + '" opacity="0.85"/>';
    } else {
      donutSlices += '<path d="M' + cx + ',' + cy + ' L' + x1 + ',' + y1 + ' A' + r + ',' + r + ' 0 ' + large + ',1 ' + x2 + ',' + y2 + ' Z" fill="' + typeColor(type) + '" opacity="0.85"/>';
    }
    donutLegend += '<div class="bar-row"><span class="bar-label" style="color:' + typeColor(type) + '">' + esc(type) + '</span><span class="bar-track"><span class="bar-fill" style="width:' + (frac*100) + '%;background:' + typeColor(type) + '"></span><span class="bar-value">' + count + '</span></span></div>';
    cumAngle += angle;
  });

  var anomalyEntries = Object.entries(anomalyCounts).sort(function(a, b) { return b[1] - a[1]; });
  var maxAnomaly = anomalyEntries.length > 0 ? anomalyEntries[0][1] : 1;
  var anomalyBars = "";
  anomalyEntries.forEach(function(entry) {
    var name = entry[0], count = entry[1];
    var w = (count / maxAnomaly) * 100;
    anomalyBars += '<div class="bar-row"><span class="bar-label">' + esc(name.replace(/_/g, " ")) + '</span><span class="bar-track"><span class="bar-fill" style="width:' + w + '%;background:var(--yellow)"></span><span class="bar-value">' + count + '</span></span></div>';
  });
  if (anomalyEntries.length === 0) anomalyBars = '<div class="empty-state">No anomalies detected</div>';

  var topTables = TABLES.slice().sort(function(a, b) { return b.total_row_count - a.total_row_count; }).slice(0, 8);
  var maxRows = topTables.length > 0 ? topTables[0].total_row_count : 1;
  var topTableBars = "";
  topTables.forEach(function(t) {
    var w = (t.total_row_count / maxRows) * 100;
    var label = t.name.length > 12 ? t.name.slice(0, 12) + "\u2026" : t.name;
    topTableBars += '<div class="bar-row"><span class="bar-label">' + esc(label) + '</span><span class="bar-track"><span class="bar-fill" style="width:' + w + '%;background:var(--accent)"></span><span class="bar-value">' + t.total_row_count.toLocaleString() + '</span></span></div>';
  });

  var qualityRings = TABLES.slice(0, 12).map(function(t) {
    var s = qualityScore(t);
    return '<div style="text-align:center">' + qualityRingSVG(s, 52) + '<div style="font-size:0.65rem;color:var(--text-dim);margin-top:0.2rem;max-width:60px;overflow:hidden;text-overflow:ellipsis">' + esc(t.name) + '</div></div>';
  }).join("");
  if (TABLES.length > 12) qualityRings += '<div style="color:var(--text-dim);font-size:0.75rem;align-self:center">+' + (TABLES.length - 12) + ' more</div>';

  setContent(el,
    '<div class="header"><h2>Overview</h2></div>'
    + '<div class="kpi-grid">'
    + '<div class="kpi-card"><div class="kpi-label">Tables</div><div class="kpi-value">' + totalTables + '</div><div class="kpi-sub">' + (errorTables ? '<span style="color:var(--red)">' + errorTables + ' errors</span>' : 'All profiled') + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Columns</div><div class="kpi-value">' + totalCols.toLocaleString() + '</div><div class="kpi-sub">' + Object.keys(typeCounts).length + ' canonical types</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Total Rows</div><div class="kpi-value">' + totalRows.toLocaleString() + '</div><div class="kpi-sub">Across all tables</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Avg Quality</div><div class="kpi-value" style="color:' + scoreColor(avgQuality) + '">' + avgQuality + '</div><div class="kpi-sub">Out of 100</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Anomalies</div><div class="kpi-value" style="color:' + (totalAnomalies > 0 ? 'var(--yellow)' : 'var(--green)') + '">' + totalAnomalies + '</div><div class="kpi-sub">' + Object.keys(anomalyCounts).length + ' unique types</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Patterns</div><div class="kpi-value">' + totalPatterns + '</div><div class="kpi-sub">Columns with patterns</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Relationships</div><div class="kpi-value">' + RELS.length + '</div><div class="kpi-sub">' + RELS.filter(function(r){return r.relationship_type==="declared_fk";}).length + ' declared, ' + RELS.filter(function(r){return r.relationship_type!=="declared_fk";}).length + ' inferred</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Duration</div><div class="kpi-value">' + totalDuration.toFixed(1) + 's</div><div class="kpi-sub">' + (totalDuration / Math.max(1, totalTables)).toFixed(2) + 's avg/table</div></div>'
    + '</div>'
    + '<div class="charts-row">'
    + '<div class="chart-card"><h3>Type Distribution</h3><div style="display:flex;align-items:center;gap:2rem"><svg width="200" height="200" viewBox="0 0 200 200">' + donutSlices + '<circle cx="100" cy="100" r="50" fill="var(--bg-card)"/><text x="100" y="96" text-anchor="middle" fill="var(--text-bright)" font-size="20" font-weight="700">' + totalCols + '</text><text x="100" y="114" text-anchor="middle" fill="var(--text-dim)" font-size="10">columns</text></svg><div class="bar-chart" style="flex:1">' + donutLegend + '</div></div></div>'
    + '<div class="chart-card"><h3>Anomaly Distribution</h3><div class="bar-chart">' + anomalyBars + '</div></div>'
    + '</div>'
    + '<div class="charts-row">'
    + '<div class="chart-card"><h3>Largest Tables</h3><div class="bar-chart">' + topTableBars + '</div></div>'
    + '<div class="chart-card"><h3>Quality Scores</h3><div style="display:flex;flex-wrap:wrap;gap:1rem;justify-content:center;padding:0.5rem">' + qualityRings + '</div></div>'
    + '</div>'
  );
}

// ============================================================
// RENDER: TABLES
// ============================================================
var tableSortCol = "name";
var tableSortAsc = true;

function renderTables(filter) {
  var el = document.getElementById("page-tables");
  filter = (filter || "").toLowerCase();

  var filtered = TABLES;
  if (filter) {
    filtered = TABLES.filter(function(t) { return t.name.toLowerCase().indexOf(filter) >= 0; });
  }

  var sorted = filtered.slice().sort(function(a, b) {
    var va, vb;
    switch(tableSortCol) {
      case "name": va = a.name; vb = b.name; break;
      case "rows": va = a.total_row_count; vb = b.total_row_count; break;
      case "cols": va = (a.columns||[]).length; vb = (b.columns||[]).length; break;
      case "quality": va = qualityScore(a); vb = qualityScore(b); break;
      case "anomalies": va = (a.columns||[]).reduce(function(s,c){return s+(c.anomalies||[]).length;},0); vb = (b.columns||[]).reduce(function(s,c){return s+(c.anomalies||[]).length;},0); break;
      case "nulls": va = (a.columns||[]).reduce(function(s,c){return s+(c.null_rate||0);},0)/Math.max(1,(a.columns||[]).length); vb = (b.columns||[]).reduce(function(s,c){return s+(c.null_rate||0);},0)/Math.max(1,(b.columns||[]).length); break;
      case "dupes": va = a.duplicate_row_count; vb = b.duplicate_row_count; break;
      default: va = a.name; vb = b.name;
    }
    if (typeof va === "string") return tableSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
    return tableSortAsc ? va - vb : vb - va;
  });

  var thClass = function(col) { return tableSortCol === col ? (tableSortAsc ? "sorted-asc" : "sorted-desc") : ""; };

  var rows = sorted.map(function(t) {
    var score = qualityScore(t);
    var anomalies = (t.columns || []).reduce(function(s,c){return s+(c.anomalies||[]).length;},0);
    var avgNull = (t.columns||[]).length > 0 ? (t.columns.reduce(function(s,c){return s+(c.null_rate||0);},0)/t.columns.length) : 0;
    return '<tr class="table-row-click" data-table="' + esc(t.name) + '" style="cursor:pointer">'
      + '<td><strong>' + esc(t.name) + '</strong></td>'
      + '<td>' + t.total_row_count.toLocaleString() + '</td>'
      + '<td>' + (t.columns||[]).length + '</td>'
      + '<td>' + scoreBadgeHTML(score) + '</td>'
      + '<td>' + (anomalies > 0 ? '<span class="badge badge-yellow">' + anomalies + '</span>' : '<span style="color:var(--text-dim)">0</span>') + '</td>'
      + '<td>' + nullBarHTML(avgNull) + '</td>'
      + '<td>' + (t.duplicate_row_count > 0 ? '<span class="badge badge-red">' + t.duplicate_row_count.toLocaleString() + '</span>' : '<span style="color:var(--text-dim)">0</span>') + '</td>'
      + '<td>' + (t.full_scan ? '<span class="badge badge-green">full scan</span>' : '<span class="badge badge-yellow">' + (t.sampled_row_count||0).toLocaleString() + ' of ' + t.total_row_count.toLocaleString() + '</span>') + '</td>'
      + '<td>' + (t.duration_seconds || 0).toFixed(2) + 's</td>'
      + '</tr>';
  }).join("");

  setContent(el,
    '<div class="header"><h2>Tables (' + filtered.length + ')</h2><div class="search-box"><span style="color:var(--text-dim)">\uD83D\uDD0D</span><input type="text" id="table-search" placeholder="Filter tables..." value="' + esc(filter) + '"></div></div>'
    + '<div class="data-table-wrap"><div class="scroll-x"><table><thead><tr>'
    + '<th class="' + thClass("name") + '" data-sort="name">Table</th>'
    + '<th class="' + thClass("rows") + '" data-sort="rows">Rows</th>'
    + '<th class="' + thClass("cols") + '" data-sort="cols">Columns</th>'
    + '<th class="' + thClass("quality") + '" data-sort="quality">Quality</th>'
    + '<th class="' + thClass("anomalies") + '" data-sort="anomalies">Anomalies</th>'
    + '<th class="' + thClass("nulls") + '" data-sort="nulls">Avg Null%</th>'
    + '<th class="' + thClass("dupes") + '" data-sort="dupes">Duplicates</th>'
    + '<th>Scan</th><th>Time</th>'
    + '</tr></thead><tbody>' + rows + '</tbody></table></div></div>'
    + '<div id="table-detail"></div>'
  );

  document.getElementById("table-search").addEventListener("input", function(e) { renderTables(e.target.value); });

  el.querySelectorAll("th[data-sort]").forEach(function(th) {
    th.addEventListener("click", function() {
      var col = th.dataset.sort;
      if (tableSortCol === col) tableSortAsc = !tableSortAsc;
      else { tableSortCol = col; tableSortAsc = true; }
      renderTables(filter);
    });
  });

  el.querySelectorAll(".table-row-click").forEach(function(row) {
    row.addEventListener("click", function() { renderTableDetail(row.dataset.table); });
  });
}

function renderTableDetail(tableName) {
  var t = TABLES.find(function(t) { return t.name === tableName; });
  if (!t) return;
  var el = document.getElementById("table-detail");
  var score = qualityScore(t);

  var constraintHtml = "";
  if (t.constraints) {
    var c = t.constraints;
    var badges = [];
    if (c.primary_key && c.primary_key.length > 0) badges.push('<span class="cstr-badge cstr-pk">PK: ' + esc(c.primary_key.join(", ")) + '</span>');
    (c.foreign_keys || []).forEach(function(fk) {
      badges.push('<span class="cstr-badge cstr-fk">FK: ' + esc((fk.constrained_columns||[]).join(", ")) + ' \u2192 ' + esc(fk.referred_table) + '</span>');
    });
    (c.unique_constraints || []).forEach(function(uq) {
      var cols = uq.columns || uq.column_names || [];
      badges.push('<span class="cstr-badge cstr-uq">UNIQUE: ' + esc(cols.join(", ")) + '</span>');
    });
    if (badges.length > 0) constraintHtml = '<div style="margin-bottom:1rem">' + badges.join(" ") + '</div>';
  }

  var colRows = (t.columns || []).map(function(c) {
    var anomalies = (c.anomalies || []).map(function(a) { return '<span class="anomaly-pill">' + esc(a) + '</span>'; }).join(" ");
    var patterns = (c.patterns || []).map(function(p) {
      var s = (c.pattern_scores || {})[p] || 0;
      return '<span class="pattern-pill">' + esc(p) + ' ' + (s * 100).toFixed(0) + '%</span>';
    }).join(" ");
    var topVals = (c.top_values || []).slice(0, 3).map(function(tv) {
      return '<span style="font-size:0.7rem;color:var(--text-dim)">' + esc(String(tv.value)) + ' (' + tv.count + ')</span>';
    }).join(", ");
    var bottomVals = (c.bottom_values || []).slice(0, 3).map(function(tv) {
      return '<span style="font-size:0.7rem;color:var(--text-dim)">' + esc(String(tv.value)) + ' (' + tv.count + ')</span>';
    }).join(", ");
    var madCell = c.mad != null ? c.mad.toFixed(4) : '\u2014';
    var cvCell = c.cv != null ? (c.cv * 100).toFixed(1) + '%' : '\u2014';
    var extras = [];
    if (c.canonical_type === 'string') {
      if (c.min_length != null) extras.push('minLen: ' + c.min_length);
      if (c.whitespace_count != null && c.whitespace_count > 0) extras.push('wsp: ' + c.whitespace_count);
    } else if (c.canonical_type === 'float') {
      if (c.infinite_count != null && c.infinite_count > 0) extras.push('\u221e: ' + c.infinite_count);
    }
    if (c.canonical_type === 'date' || c.canonical_type === 'datetime') {
      if (c.freshness_days != null) extras.push('fresh: ' + Math.round(c.freshness_days) + 'd');
    }
    if (c.is_monotonic_increasing) extras.push('mono:\u2191');
    else if (c.is_monotonic_decreasing) extras.push('mono:\u2193');
    var extrasCell = extras.map(function(e) {
      return '<span style="font-size:0.65rem;color:var(--text-dim);display:block;white-space:nowrap">' + esc(e) + '</span>';
    }).join('');

    return '<tr>'
      + '<td><strong>' + esc(c.name) + '</strong></td>'
      + '<td><span class="type-chip">' + esc(c.engine_type) + '</span></td>'
      + '<td><span class="type-chip" style="color:' + typeColor(c.canonical_type) + '">' + esc(c.canonical_type) + '</span></td>'
      + '<td>' + nullBarHTML(c.null_rate || 0) + '</td>'
      + '<td>' + fmt(c.approx_distinct) + '</td>'
      + '<td>' + fmt(c.min) + '</td>'
      + '<td>' + fmt(c.max) + '</td>'
      + '<td>' + fmt(c.mean) + '</td>'
      + '<td>' + fmt(c.median) + '</td>'
      + '<td>' + fmt(c.stddev) + '</td>'
      + '<td>' + madCell + '</td>'
      + '<td>' + cvCell + '</td>'
      + '<td>' + fmt(c.skewness) + '</td>'
      + '<td>' + fmt(c.kurtosis) + '</td>'
      + '<td>' + (extrasCell || '\u2014') + '</td>'
      + '<td>' + (patterns || '\u2014') + '</td>'
      + '<td>' + (anomalies || '\u2014') + '</td>'
      + '<td style="max-width:180px;overflow:hidden;text-overflow:ellipsis">' + (topVals || '\u2014') + '</td>'
      + '<td style="max-width:180px;overflow:hidden;text-overflow:ellipsis">' + (bottomVals || '\u2014') + '</td>'
      + '</tr>';
  }).join("");

  setContent(el,
    '<div class="detail-panel">'
    + '<div style="display:flex;align-items:center;gap:1.5rem;margin-bottom:1.5rem">'
    + qualityRingSVG(score, 64)
    + '<div><h3 style="margin-bottom:0.25rem">' + esc(t.name) + '</h3><span style="font-size:0.8rem;color:var(--text-dim)">' + esc(t.comment || '') + '</span></div>'
    + '<button id="close-detail-btn" style="margin-left:auto;background:var(--bg);border:1px solid var(--border);color:var(--text-dim);padding:0.3rem 0.75rem;border-radius:6px;cursor:pointer;font-size:0.8rem">Close</button>'
    + '</div>'
    + '<div class="detail-grid">'
    + '<div class="detail-stat"><div class="label">Total Rows</div><div class="val">' + t.total_row_count.toLocaleString() + '</div></div>'
    + '<div class="detail-stat"><div class="label">Sampled</div><div class="val">' + (t.sampled_row_count || 0).toLocaleString() + '</div></div>'
    + '<div class="detail-stat"><div class="label">Columns</div><div class="val">' + (t.columns || []).length + '</div></div>'
    + '<div class="detail-stat"><div class="label">Full Scan</div><div class="val">' + (t.full_scan ? "Yes" : "No") + '</div></div>'
    + '<div class="detail-stat"><div class="label">Duplicates</div><div class="val">' + (t.duplicate_row_count > 0 ? t.duplicate_row_count.toLocaleString() + " (" + pct(t.duplicate_rate) + ")" : "None") + '</div></div>'
    + '<div class="detail-stat"><div class="label">Duration</div><div class="val">' + (t.duration_seconds || 0).toFixed(2) + 's</div></div>'
    + (t.row_completeness_mean != null ? '<div class="detail-stat"><div class="label">Row Completeness</div><div class="val">' + pct(t.row_completeness_mean) + ' avg (min ' + pct(t.row_completeness_min) + ')</div></div>' : '')
    + '</div>'
    + (t.functional_dependencies && t.functional_dependencies.length > 0 ? '<div style="margin:0.75rem 0;padding:0.75rem;background:var(--bg);border:1px solid var(--border);border-radius:6px"><div style="font-size:0.75rem;font-weight:600;color:var(--text-dim);margin-bottom:0.5rem">FUNCTIONAL DEPENDENCIES</div><div style="display:flex;flex-wrap:wrap;gap:0.4rem">' + t.functional_dependencies.map(function(fd) { return '<span style="font-size:0.75rem;padding:0.2rem 0.5rem;background:rgba(100,200,255,0.1);border:1px solid rgba(100,200,255,0.3);border-radius:4px;color:var(--text)">' + esc(fd.from) + ' \u2192 ' + esc(fd.to) + '</span>'; }).join('') + '</div></div>' : '')
    + constraintHtml
    + '<div class="scroll-x" style="margin-top:1rem"><table><thead><tr>'
    + '<th>Column</th><th>Engine Type</th><th>Canonical</th><th>Nulls</th><th>Distinct</th>'
    + '<th>Min</th><th>Max</th><th>Mean</th><th>Median</th><th>Stddev</th>'
    + '<th>MAD</th><th>CV</th><th>Skew</th><th>Kurt</th>'
    + '<th>Extras</th><th>Patterns</th><th>Anomalies</th><th>Top Values</th><th>Bottom Values</th>'
    + '</tr></thead><tbody>' + colRows + '</tbody></table></div>'
    + histogramSection(t)
    + benfordSection(t)
    + constraintSuggestSection(t)
    + '</div>'
  );

  document.getElementById("close-detail-btn").addEventListener("click", function() { setContent(el, ""); });
  el.scrollIntoView({behavior: "smooth", block: "start"});
}

function histogramSection(t) {
  var cols = (t.columns || []).filter(function(c) { return c.histogram && c.histogram.length > 0; });
  if (cols.length === 0) return '';
  var charts = cols.map(function(c) {
    var maxCount = Math.max.apply(null, c.histogram.map(function(b) { return b.count; }));
    var bars = c.histogram.map(function(b) {
      var pctH = maxCount > 0 ? (b.count / maxCount * 100) : 0;
      var label = (typeof b.bin_start === 'number' ? b.bin_start.toFixed(1) : b.bin_start);
      return '<div style="display:flex;align-items:end;flex:1;flex-direction:column;gap:2px" title="' + esc(b.bin_start + ' - ' + b.bin_end + ': ' + b.count) + '">'
        + '<div style="width:100%;background:var(--accent);border-radius:2px 2px 0 0;height:' + Math.max(pctH, 2) + '%;min-height:2px"></div>'
        + '<div style="font-size:0.55rem;color:var(--text-dim);text-align:center;overflow:hidden;text-overflow:ellipsis;width:100%">' + label + '</div>'
        + '</div>';
    }).join('');
    var kdeOverlay = '';
    if (c.kde && c.kde.length >= 2) {
      var kdeMax = Math.max.apply(null, c.kde.map(function(p) { return p.y; }));
      var kdeXMin = c.kde[0].x;
      var kdeXMax = c.kde[c.kde.length - 1].x;
      var kdeXRange = kdeXMax - kdeXMin;
      var W = 300, H = 80;
      if (kdeMax > 0 && kdeXRange > 0) {
        var pts = c.kde.map(function(p) {
          var sx = ((p.x - kdeXMin) / kdeXRange * W).toFixed(1);
          var sy = (H - p.y / kdeMax * (H - 4)).toFixed(1);
          return sx + ',' + sy;
        }).join(' ');
        kdeOverlay = '<svg style="position:absolute;top:0;left:0;width:100%;height:80px;pointer-events:none" viewBox="0 0 ' + W + ' ' + H + '" preserveAspectRatio="none">'
          + '<polyline points="' + pts + '" fill="none" stroke="rgba(255,180,50,0.85)" stroke-width="1.5" stroke-linejoin="round"/>'
          + '</svg>';
      }
    }
    return '<div style="flex:1;min-width:200px;max-width:320px">'
      + '<div style="font-size:0.75rem;font-weight:600;margin-bottom:0.5rem">' + esc(c.name) + '</div>'
      + '<div style="position:relative;display:flex;align-items:end;height:80px;gap:2px;border-bottom:1px solid var(--border)">' + bars + kdeOverlay + '</div>'
      + '</div>';
  }).join('');
  return '<div style="margin-top:1.5rem"><h4 style="margin-bottom:0.75rem">Histograms <span style="font-size:0.7rem;color:var(--text-dim);font-weight:400">(orange = KDE density curve)</span></h4>'
    + '<div style="display:flex;flex-wrap:wrap;gap:1.5rem">' + charts + '</div></div>';
}

function benfordSection(t) {
  var cols = (t.columns || []).filter(function(c) { return c.benford_digits && c.benford_digits.length > 0; });
  if (cols.length === 0) return '';
  var EXPECTED = [0.301, 0.176, 0.125, 0.097, 0.079, 0.067, 0.058, 0.051, 0.046];
  var charts = cols.map(function(c) {
    var pass = c.benford_pvalue == null || c.benford_pvalue >= 0.01;
    var badge = pass
      ? '<span class="badge badge-green" style="font-size:0.65rem">PASS</span>'
      : '<span class="badge" style="font-size:0.65rem;background:var(--red);color:#fff">ANOMALY p=' + (c.benford_pvalue != null ? c.benford_pvalue.toFixed(4) : '?') + '</span>';
    var total = c.benford_digits.reduce(function(s, d) { return s + d.count; }, 0);
    var maxH = 0.35;
    var bars = c.benford_digits.map(function(d, i) {
      var obs = total > 0 ? d.count / total : 0;
      var exp = EXPECTED[i] || 0;
      var obsH = (obs / maxH * 100);
      var expH = (exp / maxH * 100);
      return '<div style="display:flex;flex-direction:column;align-items:center;flex:1;gap:1px">'
        + '<div style="display:flex;align-items:end;gap:1px;height:60px;width:100%">'
        + '<div style="flex:1;background:var(--accent);border-radius:2px 2px 0 0;height:' + Math.max(obsH, 1) + '%" title="Observed: ' + (obs * 100).toFixed(1) + '%"></div>'
        + '<div style="flex:1;background:var(--text-dim);opacity:0.3;border-radius:2px 2px 0 0;height:' + Math.max(expH, 1) + '%" title="Expected: ' + (exp * 100).toFixed(1) + '%"></div>'
        + '</div>'
        + '<div style="font-size:0.6rem;color:var(--text-dim)">' + d.digit + '</div>'
        + '</div>';
    }).join('');
    return '<div style="flex:1;min-width:200px;max-width:320px">'
      + '<div style="font-size:0.75rem;font-weight:600;margin-bottom:0.25rem">' + esc(c.name) + ' ' + badge + '</div>'
      + '<div style="display:flex;align-items:end;gap:2px;border-bottom:1px solid var(--border)">' + bars + '</div>'
      + '<div style="font-size:0.6rem;color:var(--text-dim);margin-top:2px"><span style="display:inline-block;width:8px;height:8px;background:var(--accent);border-radius:1px"></span> observed <span style="display:inline-block;width:8px;height:8px;background:var(--text-dim);opacity:0.3;border-radius:1px;margin-left:6px"></span> expected</div>'
      + '</div>';
  }).join('');
  return '<div style="margin-top:1.5rem"><h4 style="margin-bottom:0.75rem">Benford\'s Law Analysis</h4>'
    + '<div style="display:flex;flex-wrap:wrap;gap:1.5rem">' + charts + '</div></div>';
}

function constraintSuggestSection(t) {
  var sc = t.suggested_constraints;
  if (!sc || sc.length === 0) return '';
  var rows = sc.map(function(s) {
    var confColor = s.confidence >= 0.9 ? 'var(--green)' : (s.confidence >= 0.8 ? 'var(--blue)' : 'var(--text-dim)');
    return '<tr>'
      + '<td>' + esc(s.column) + '</td>'
      + '<td><span class="badge" style="background:' + confColor + ';color:#fff;font-size:0.65rem">' + esc(s.constraint_type) + '</span></td>'
      + '<td style="font-family:monospace;font-size:0.7rem">' + esc(s.expression) + '</td>'
      + '<td>' + (s.confidence * 100).toFixed(0) + '%</td>'
      + '<td style="font-size:0.75rem;color:var(--text-dim)">' + esc(s.evidence) + '</td>'
      + '</tr>';
  }).join('');
  return '<div style="margin-top:1.5rem"><h4 style="margin-bottom:0.75rem">Suggested Constraints</h4>'
    + '<div class="scroll-x"><table><thead><tr><th>Column</th><th>Type</th><th>Expression</th><th>Confidence</th><th>Evidence</th></tr></thead>'
    + '<tbody>' + rows + '</tbody></table></div></div>';
}

// ============================================================
// RENDER: COLUMN EXPLORER
// ============================================================
function renderColumns(filter) {
  var el = document.getElementById("page-columns");
  filter = (filter || "").toLowerCase();

  var filtered = allColumns;
  if (filter) {
    filtered = allColumns.filter(function(c) {
      return c.name.toLowerCase().indexOf(filter) >= 0
        || c._table.toLowerCase().indexOf(filter) >= 0
        || c.canonical_type.toLowerCase().indexOf(filter) >= 0
        || (c.patterns || []).some(function(p) { return p.toLowerCase().indexOf(filter) >= 0; })
        || (c.anomalies || []).some(function(a) { return a.toLowerCase().indexOf(filter) >= 0; });
    });
  }

  var shown = filtered.slice(0, 200);
  var rows = shown.map(function(c) {
    var anomalies = (c.anomalies || []).map(function(a) { return '<span class="anomaly-pill">' + esc(a) + '</span>'; }).join(" ");
    var patterns = (c.patterns || []).map(function(p) { return '<span class="pattern-pill">' + esc(p) + '</span>'; }).join(" ");
    return '<tr>'
      + '<td><strong>' + esc(c._table) + '</strong></td>'
      + '<td><strong>' + esc(c.name) + '</strong></td>'
      + '<td><span class="type-chip" style="color:' + typeColor(c.canonical_type) + '">' + esc(c.canonical_type) + '</span></td>'
      + '<td><span class="type-chip">' + esc(c.engine_type) + '</span></td>'
      + '<td>' + nullBarHTML(c.null_rate || 0) + '</td>'
      + '<td>' + fmt(c.approx_distinct) + '</td>'
      + '<td>' + fmt(c.min) + '</td>'
      + '<td>' + fmt(c.max) + '</td>'
      + '<td>' + fmt(c.mean) + '</td>'
      + '<td>' + (patterns || '\u2014') + '</td>'
      + '<td>' + (anomalies || '\u2014') + '</td>'
      + '</tr>';
  }).join("");

  setContent(el,
    '<div class="header"><h2>Column Explorer (' + filtered.length + ' columns)</h2><div class="search-box"><span style="color:var(--text-dim)">\uD83D\uDD0D</span><input type="text" id="col-search" placeholder="Search columns, types, patterns..." value="' + esc(filter) + '"></div></div>'
    + (filtered.length > 200 ? '<div style="color:var(--text-dim);font-size:0.8rem;margin-bottom:1rem">Showing first 200 of ' + filtered.length + ' results</div>' : '')
    + '<div class="data-table-wrap"><div class="scroll-x"><table><thead><tr>'
    + '<th>Table</th><th>Column</th><th>Canonical</th><th>Engine Type</th>'
    + '<th>Nulls</th><th>Distinct</th><th>Min</th><th>Max</th><th>Mean</th>'
    + '<th>Patterns</th><th>Anomalies</th>'
    + '</tr></thead><tbody>' + rows + '</tbody></table></div></div>'
  );

  document.getElementById("col-search").addEventListener("input", function(e) { renderColumns(e.target.value); });
}

// ============================================================
// RENDER: DATA QUALITY
// ============================================================
function renderQuality() {
  var el = document.getElementById("page-quality");
  var sorted = TABLES.slice().sort(function(a, b) { return qualityScore(a) - qualityScore(b); });

  var tableCards = sorted.map(function(t) {
    var score = qualityScore(t);
    var cols = t.columns || [];
    var avgNull = cols.length > 0 ? cols.reduce(function(s, c) { return s + (c.null_rate || 0); }, 0) / cols.length : 0;
    var tAnomalies = {};
    cols.forEach(function(c) { (c.anomalies || []).forEach(function(a) { tAnomalies[a] = (tAnomalies[a] || 0) + 1; }); });
    var anomalyPills = Object.entries(tAnomalies).map(function(e) { return '<span class="anomaly-pill">' + esc(e[0]) + ' (' + e[1] + ')</span>'; }).join(" ");

    return '<div style="display:flex;align-items:center;gap:1.25rem;padding:0.75rem 1.25rem;border-bottom:1px solid var(--border-subtle)">'
      + '<span title="' + esc(qualityTooltip(t)) + '">' + qualityRingSVG(score, 48) + '</span>'
      + '<div style="flex:1;min-width:0"><div style="font-weight:600;color:var(--text-bright)">' + esc(t.name) + '</div>'
      + '<div style="font-size:0.75rem;color:var(--text-dim)">' + t.total_row_count.toLocaleString() + ' rows, ' + cols.length + ' cols, avg null ' + pct(avgNull) + (t.row_completeness_mean != null ? ', row compl ' + pct(t.row_completeness_mean) : '') + (t.duplicate_row_count > 0 ? ', <span style="color:var(--red)">' + t.duplicate_row_count + ' dupes</span>' : '') + '</div>'
      + '<div style="margin-top:0.25rem">' + (anomalyPills || '<span style="font-size:0.75rem;color:var(--text-dim)">No anomalies</span>') + '</div>'
      + '</div></div>';
  }).join("");

  var buckets = {excellent: 0, good: 0, fair: 0, poor: 0};
  TABLES.forEach(function(t) {
    var s = qualityScore(t);
    if (s >= QS_EXCELLENT) buckets.excellent++;
    else if (s >= QS_GOOD) buckets.good++;
    else if (s >= QS_FAIR) buckets.fair++;
    else buckets.poor++;
  });

  var totalFDs = TABLES.reduce(function(s, t) { return s + (t.functional_dependencies ? t.functional_dependencies.length : 0); }, 0);
  var tablesWithRC = TABLES.filter(function(t) { return t.row_completeness_mean != null; });
  var avgRC = tablesWithRC.length > 0 ? (tablesWithRC.reduce(function(s, t) { return s + t.row_completeness_mean; }, 0) / tablesWithRC.length) : null;

  setContent(el,
    '<div class="header"><h2>Data Quality</h2></div>'
    + '<div class="kpi-grid" style="margin-bottom:2rem">'
    + '<div class="kpi-card"><div class="kpi-label">Avg Score</div><div class="kpi-value" style="color:' + scoreColor(avgQuality) + '">' + avgQuality + '/100</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Excellent (' + QS_EXCELLENT + '+)</div><div class="kpi-value" style="color:var(--green)">' + buckets.excellent + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Good (' + QS_GOOD + '-' + (QS_EXCELLENT-1) + ')</div><div class="kpi-value" style="color:var(--blue)">' + buckets.good + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Fair (' + QS_FAIR + '-' + (QS_GOOD-1) + ')</div><div class="kpi-value" style="color:var(--yellow)">' + buckets.fair + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Poor (&lt;' + QS_FAIR + ')</div><div class="kpi-value" style="color:var(--red)">' + buckets.poor + '</div></div>'
    + (avgRC != null ? '<div class="kpi-card"><div class="kpi-label">Avg Row Completeness</div><div class="kpi-value">' + pct(avgRC) + '</div></div>' : '')
    + (totalFDs > 0 ? '<div class="kpi-card"><div class="kpi-label">Functional Dependencies</div><div class="kpi-value">' + totalFDs + '</div></div>' : '')
    + '</div>'
    + '<div class="data-table-wrap"><h3 style="padding:1rem 1.25rem">All Tables by Quality</h3>' + tableCards + '</div>'
  );
}

// ============================================================
// RENDER: PATTERNS
// ============================================================
function renderPatterns() {
  var el = document.getElementById("page-patterns");
  var patternCols = allColumns.filter(function(c) { return c.patterns && c.patterns.length > 0; });

  var patternTypes = {};
  patternCols.forEach(function(c) { c.patterns.forEach(function(p) { patternTypes[p] = (patternTypes[p] || 0) + 1; }); });
  var patternSummary = Object.entries(patternTypes).sort(function(a, b) { return b[1] - a[1]; });
  var maxPat = patternSummary.length > 0 ? patternSummary[0][1] : 1;

  var summaryBars = patternSummary.map(function(e) {
    var w = (e[1] / maxPat) * 100;
    return '<div class="bar-row"><span class="bar-label">' + esc(e[0]) + '</span><span class="bar-track"><span class="bar-fill" style="width:' + w + '%;background:var(--purple)"></span><span class="bar-value">' + e[1] + '</span></span></div>';
  }).join("");

  var rows = patternCols.map(function(c) {
    var patterns = c.patterns.map(function(p) {
      var s = (c.pattern_scores || {})[p] || 0;
      return '<span class="pattern-pill">' + esc(p) + ' ' + (s * 100).toFixed(0) + '%</span>';
    }).join(" ");
    return '<tr><td><strong>' + esc(c._table) + '</strong></td><td><strong>' + esc(c.name) + '</strong></td><td><span class="type-chip">' + esc(c.engine_type) + '</span></td><td>' + patterns + '</td><td>' + fmt(c.approx_distinct) + '</td><td>' + nullBarHTML(c.null_rate || 0) + '</td></tr>';
  }).join("");

  setContent(el,
    '<div class="header"><h2>Patterns &amp; PII Detection</h2></div>'
    + '<div class="charts-row">'
    + '<div class="chart-card"><h3>Pattern Type Distribution</h3>' + (patternSummary.length > 0 ? '<div class="bar-chart">' + summaryBars + '</div>' : '<div class="empty-state">No patterns detected</div>') + '</div>'
    + '<div class="chart-card"><h3>Summary</h3><div class="detail-grid"><div class="detail-stat"><div class="label">Columns with Patterns</div><div class="val">' + patternCols.length + '</div></div><div class="detail-stat"><div class="label">Unique Pattern Types</div><div class="val">' + Object.keys(patternTypes).length + '</div></div><div class="detail-stat"><div class="label">Tables Affected</div><div class="val">' + new Set(patternCols.map(function(c){return c._table;})).size + '</div></div></div></div>'
    + '</div>'
    + '<div class="data-table-wrap"><h3 style="padding:1rem 1.25rem">Detected Patterns</h3>'
    + (patternCols.length > 0 ? '<div class="scroll-x"><table><thead><tr><th>Table</th><th>Column</th><th>Type</th><th>Patterns</th><th>Distinct</th><th>Nulls</th></tr></thead><tbody>' + rows + '</tbody></table></div>' : '<div class="empty-state">No patterns detected in this dataset</div>')
    + '</div>'
  );
}

// ============================================================
// RENDER: CORRELATIONS
// ============================================================
function renderCorrelations() {
  var el = document.getElementById("page-correlations");

  // Gather all correlations from all tables
  var allCorrs = [];
  TABLES.forEach(function(t) {
    (t.correlations || []).forEach(function(c) {
      c._table = t.name;
      allCorrs.push(c);
    });
  });

  if (allCorrs.length === 0) {
    setContent(el,
      '<div class="header"><h2>Correlations</h2></div>'
      + '<div class="empty-state" style="padding:3rem">No numeric column pairs found for correlation analysis.<br>'
      + '<span style="font-size:0.8rem">Correlation requires at least 2 numeric columns per table.</span></div>'
    );
    return;
  }

  // Separate Pearson/Spearman pairs from Cramer's V
  var numericCorrs = allCorrs.filter(function(c) { return c.type !== "cramers_v"; });
  var pearsons = numericCorrs.filter(function(c) { return c.pearson != null; });
  var spearmans = numericCorrs.filter(function(c) { return c.spearman != null; });
  var cramers = allCorrs.filter(function(c) { return c.type === "cramers_v"; });

  // Build heatmap per table (Pearson)
  var tableGroups = {};
  pearsons.forEach(function(c) {
    if (!tableGroups[c._table]) tableGroups[c._table] = [];
    tableGroups[c._table].push(c);
  });

  function corrColor(r) {
    if (r == null) return 'var(--bg-alt)';
    var abs = Math.abs(r);
    if (r > 0) return 'rgba(239,68,68,' + (abs * 0.8) + ')';
    return 'rgba(59,130,246,' + (abs * 0.8) + ')';
  }

  var heatmaps = Object.keys(tableGroups).map(function(tName) {
    var pairs = tableGroups[tName];
    var colSet = {};
    pairs.forEach(function(p) { colSet[p.col1] = 1; colSet[p.col2] = 1; });
    var cols = Object.keys(colSet).sort();
    if (cols.length < 2) return '';

    var lookup = {};
    pairs.forEach(function(p) { lookup[p.col1 + '|' + p.col2] = p.pearson; lookup[p.col2 + '|' + p.col1] = p.pearson; });

    var cellSize = Math.min(40, Math.floor(500 / cols.length));
    var labelW = 100;
    var svgW = labelW + cols.length * cellSize;
    var svgH = 20 + cols.length * cellSize;

    var cells = '';
    cols.forEach(function(row, ri) {
      cols.forEach(function(col, ci) {
        var r = (row === col) ? 1.0 : (lookup[row + '|' + col] != null ? lookup[row + '|' + col] : null);
        var rStr = r != null ? r.toFixed(2) : 'N/A';
        cells += '<rect x="' + (labelW + ci * cellSize) + '" y="' + (20 + ri * cellSize) + '" width="' + cellSize + '" height="' + cellSize + '" fill="' + corrColor(r) + '" stroke="var(--border)" stroke-width="0.5"><title>' + esc(row) + ' vs ' + esc(col) + ': ' + rStr + '</title></rect>';
      });
    });

    var colLabels = cols.map(function(c, i) {
      return '<text x="' + (labelW + i * cellSize + cellSize / 2) + '" y="14" text-anchor="middle" font-size="9" fill="var(--text-dim)" transform="rotate(-45,' + (labelW + i * cellSize + cellSize / 2) + ',14)">' + esc(c.substring(0, 12)) + '</text>';
    }).join('');
    var rowLabels = cols.map(function(c, i) {
      return '<text x="' + (labelW - 4) + '" y="' + (20 + i * cellSize + cellSize / 2 + 3) + '" text-anchor="end" font-size="9" fill="var(--text-dim)">' + esc(c.substring(0, 15)) + '</text>';
    }).join('');

    return '<div style="margin-bottom:2rem"><h4>' + esc(tName) + '</h4>'
      + '<svg viewBox="0 0 ' + svgW + ' ' + svgH + '" width="100%" style="max-width:' + svgW + 'px">'
      + colLabels + rowLabels + cells + '</svg></div>';
  }).join('');

  // Spearman table (top pairs by |spearman| not shown in heatmap — use a ranked table)
  var spearmanHtml = '';
  if (spearmans.length > 0) {
    var spRows = spearmans.slice().sort(function(a, b) { return Math.abs(b.spearman) - Math.abs(a.spearman); })
      .slice(0, 30)
      .map(function(c) {
        var strength = Math.abs(c.spearman) >= 0.7 ? 'Strong' : (Math.abs(c.spearman) >= 0.3 ? 'Moderate' : 'Weak');
        var delta = (c.pearson != null) ? (c.spearman - c.pearson).toFixed(3) : 'N/A';
        var nmiCell = (c.nmi != null) ? c.nmi.toFixed(3) : '\u2014';
        return '<tr><td>' + esc(c._table) + '</td><td>' + esc(c.col1) + '</td><td>' + esc(c.col2) + '</td>'
          + '<td>' + c.spearman.toFixed(3) + '</td><td>' + (c.pearson != null ? c.pearson.toFixed(3) : 'N/A') + '</td>'
          + '<td>' + delta + '</td><td>' + nmiCell + '</td><td>' + strength + '</td></tr>';
      }).join('');
    spearmanHtml = '<h3 style="margin-top:2rem">Spearman Rank Correlation (Top 30)</h3>'
      + '<p style="font-size:0.8rem;color:var(--text-dim)">High Spearman with low Pearson indicates a monotone but nonlinear relationship. NMI (Normalized Mutual Information) measures any dependency, including nonlinear.</p>'
      + '<div class="scroll-x"><table><thead><tr><th>Table</th><th>Col 1</th><th>Col 2</th><th>Spearman</th><th>Pearson</th><th>\u0394</th><th>NMI</th><th>Strength</th></tr></thead>'
      + '<tbody>' + spRows + '</tbody></table></div>';
  }

  // Cramer's V table
  var cramerHtml = '';
  if (cramers.length > 0) {
    var crRows = cramers.map(function(c) {
      var strength = c.cramers_v >= 0.3 ? 'Strong' : (c.cramers_v >= 0.1 ? 'Moderate' : 'Weak');
      return '<tr><td>' + esc(c._table) + '</td><td>' + esc(c.col1) + '</td><td>' + esc(c.col2) + '</td>'
        + '<td>' + c.cramers_v.toFixed(3) + '</td><td>' + strength + '</td></tr>';
    }).join('');
    cramerHtml = '<h3 style="margin-top:2rem">Cram\u00e9r\'s V (Categorical)</h3>'
      + '<div class="scroll-x"><table><thead><tr><th>Table</th><th>Column 1</th><th>Column 2</th><th>V</th><th>Strength</th></tr></thead>'
      + '<tbody>' + crRows + '</tbody></table></div>';
  }

  setContent(el,
    '<div class="header"><h2>Correlations</h2></div>'
    + '<div class="kpi-grid" style="margin-bottom:2rem">'
    + '<div class="kpi-card"><div class="kpi-label">Pearson Pairs</div><div class="kpi-value">' + pearsons.length + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Spearman Pairs</div><div class="kpi-value">' + spearmans.length + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">NMI Pairs</div><div class="kpi-value">' + numericCorrs.filter(function(c){return c.nmi!=null;}).length + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Cram\u00e9r\'s V Pairs</div><div class="kpi-value">' + cramers.length + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Tables Analyzed</div><div class="kpi-value">' + Object.keys(tableGroups).length + '</div></div>'
    + '</div>'
    + '<div style="display:flex;align-items:center;gap:1rem;margin-bottom:1.5rem;font-size:0.75rem">'
    + '<span><span style="display:inline-block;width:12px;height:12px;background:rgba(59,130,246,0.8);border-radius:2px"></span> Negative</span>'
    + '<span><span style="display:inline-block;width:12px;height:12px;background:var(--bg-alt);border:1px solid var(--border);border-radius:2px"></span> Zero</span>'
    + '<span><span style="display:inline-block;width:12px;height:12px;background:rgba(239,68,68,0.8);border-radius:2px"></span> Positive</span>'
    + '</div>'
    + heatmaps
    + spearmanHtml
    + cramerHtml
  );
}

// ============================================================
// RENDER: MISSING VALUES
// ============================================================
function renderMissing() {
  var el = document.getElementById("page-missing");

  // Collect null data across all tables
  var rows = [];
  TABLES.forEach(function(t) {
    if (t.error) return;
    (t.columns || []).forEach(function(c) {
      if (c.null_rate > 0) {
        rows.push({table: t.name, column: c.name, null_rate: c.null_rate, null_count: c.null_count || 0, total: t.total_row_count});
      }
    });
  });

  if (rows.length === 0) {
    setContent(el,
      '<div class="header"><h2>Missing Values</h2></div>'
      + '<div class="empty-state" style="padding:3rem">No null values detected across any columns.<br>'
      + '<span style="font-size:0.8rem">All columns have complete data.</span></div>'
    );
    return;
  }

  rows.sort(function(a, b) { return b.null_rate - a.null_rate; });

  // Heatmap: rows = tables, columns = top-20 columns by null rate
  var topCols = rows.slice(0, 40);
  var tableSet = {};
  topCols.forEach(function(r) {
    if (!tableSet[r.table]) tableSet[r.table] = {};
    tableSet[r.table][r.column] = r.null_rate;
  });
  var tableNames = Object.keys(tableSet);

  function nullColor(rate) {
    if (rate === 0) return 'var(--green)';
    if (rate < 0.1) return 'rgba(234,179,8,0.5)';
    if (rate < 0.5) return 'rgba(249,115,22,0.7)';
    return 'rgba(239,68,68,0.8)';
  }

  // Summary KPIs
  var totalNulls = rows.reduce(function(s, r) { return s + r.null_count; }, 0);
  var tablesWithNulls = new Set(rows.map(function(r) { return r.table; }));
  var maxRate = rows.length > 0 ? rows[0].null_rate : 0;

  // Table rows
  var tblRows = rows.slice(0, 50).map(function(r) {
    return '<tr>'
      + '<td>' + esc(r.table) + '</td>'
      + '<td>' + esc(r.column) + '</td>'
      + '<td>' + nullBarHTML(r.null_rate) + '</td>'
      + '<td>' + r.null_count.toLocaleString() + '</td>'
      + '<td>' + r.total.toLocaleString() + '</td>'
      + '</tr>';
  }).join('');

  // SVG heatmap: rows = tables, columns = top columns by null rate
  var colNames = [];
  var colSeen = {};
  topCols.forEach(function(r) {
    if (!colSeen[r.table + '.' + r.column]) {
      colSeen[r.table + '.' + r.column] = true;
      if (colNames.indexOf(r.column) === -1 && colNames.length < 20) colNames.push(r.column);
    }
  });
  var cellSize = 28;
  var labelW = 160;
  var labelH = 80;
  var svgW = labelW + colNames.length * cellSize;
  var svgH = labelH + tableNames.length * cellSize;
  var heatmapSVG = '';
  if (tableNames.length > 0 && colNames.length > 0) {
    var rects = '';
    // Column labels (rotated)
    colNames.forEach(function(cn, ci) {
      rects += '<text x="' + (labelW + ci * cellSize + cellSize / 2) + '" y="' + (labelH - 4) + '" '
        + 'text-anchor="end" font-size="9" fill="var(--text)" '
        + 'transform="rotate(-45 ' + (labelW + ci * cellSize + cellSize / 2) + ' ' + (labelH - 4) + ')">'
        + esc(cn.length > 15 ? cn.slice(0, 14) + '\u2026' : cn) + '</text>';
    });
    // Table rows
    tableNames.forEach(function(tn, ti) {
      rects += '<text x="' + (labelW - 4) + '" y="' + (labelH + ti * cellSize + cellSize / 2 + 3) + '" '
        + 'text-anchor="end" font-size="9" fill="var(--text)">'
        + esc(tn.length > 20 ? tn.slice(0, 19) + '\u2026' : tn) + '</text>';
      colNames.forEach(function(cn, ci) {
        var rate = (tableSet[tn] && tableSet[tn][cn]) || 0;
        var color = nullColor(rate);
        rects += '<rect x="' + (labelW + ci * cellSize) + '" y="' + (labelH + ti * cellSize) + '" '
          + 'width="' + (cellSize - 2) + '" height="' + (cellSize - 2) + '" rx="3" '
          + 'fill="' + color + '" stroke="var(--border)" stroke-width="0.5">'
          + '<title>' + esc(tn) + '.' + esc(cn) + ': ' + (rate * 100).toFixed(1) + '% null</title></rect>';
      });
    });
    heatmapSVG = '<div style="margin-bottom:2rem"><h3 style="margin-bottom:0.5rem;font-size:0.95rem">Null Rate Heatmap</h3>'
      + '<div class="scroll-x"><svg viewBox="0 0 ' + svgW + ' ' + svgH + '" width="100%" style="max-width:' + svgW + 'px">'
      + rects + '</svg></div></div>';
  }

  setContent(el,
    '<div class="header"><h2>Missing Values</h2></div>'
    + '<div class="kpi-grid" style="margin-bottom:2rem">'
    + '<div class="kpi-card"><div class="kpi-label">Columns with Nulls</div><div class="kpi-value">' + rows.length + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Tables Affected</div><div class="kpi-value">' + tablesWithNulls.size + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Total Null Cells</div><div class="kpi-value">' + totalNulls.toLocaleString() + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Worst Column</div><div class="kpi-value" style="color:var(--red)">' + pct(maxRate) + '</div></div>'
    + '</div>'
    + '<div style="display:flex;align-items:center;gap:1rem;margin-bottom:1rem;font-size:0.75rem">'
    + '<span><span style="display:inline-block;width:12px;height:12px;background:var(--green);border-radius:2px"></span> 0%</span>'
    + '<span><span style="display:inline-block;width:12px;height:12px;background:rgba(234,179,8,0.5);border-radius:2px"></span> 1-10%</span>'
    + '<span><span style="display:inline-block;width:12px;height:12px;background:rgba(249,115,22,0.7);border-radius:2px"></span> 10-50%</span>'
    + '<span><span style="display:inline-block;width:12px;height:12px;background:rgba(239,68,68,0.8);border-radius:2px"></span> >50%</span>'
    + '</div>'
    + heatmapSVG
    + '<div class="scroll-x"><table><thead><tr><th>Table</th><th>Column</th><th>Null Rate</th><th>Null Count</th><th>Total Rows</th></tr></thead>'
    + '<tbody>' + tblRows + '</tbody></table></div>'
  );
}

// ============================================================
// RENDER: RELATIONSHIPS
// ============================================================
function renderRelationships() {
  var el = document.getElementById("page-relationships");
  var declared = RELS.filter(function(r) { return r.relationship_type === "declared_fk"; });
  var inferred = RELS.filter(function(r) { return r.relationship_type !== "declared_fk"; });

  var relCards = RELS.map(function(r) {
    var isDeclared = r.relationship_type === "declared_fk";
    return '<div class="rel-card">'
      + '<div><div class="rel-table">' + esc(r.source_table) + '</div><div class="rel-cols">' + esc((r.source_columns || []).join(", ")) + '</div></div>'
      + '<div class="rel-arrow">\u2192</div>'
      + '<div><div class="rel-table">' + esc(r.target_table) + '</div><div class="rel-cols">' + esc((r.target_columns || []).join(", ")) + '</div></div>'
      + '<div style="margin-left:auto"><span class="badge ' + (isDeclared ? 'badge-blue' : 'badge-green') + '">' + (isDeclared ? "Declared FK" : "Inferred") + '</span></div>'
      + '</div>';
  }).join("");

  var connectedTables = new Set();
  RELS.forEach(function(r) { connectedTables.add(r.source_table); connectedTables.add(r.target_table); });

  setContent(el,
    '<div class="header"><h2>Relationships</h2></div>'
    + '<div class="kpi-grid" style="margin-bottom:2rem">'
    + '<div class="kpi-card"><div class="kpi-label">Total Relationships</div><div class="kpi-value">' + RELS.length + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Declared FKs</div><div class="kpi-value" style="color:var(--blue)">' + declared.length + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Inferred Joins</div><div class="kpi-value" style="color:var(--green)">' + inferred.length + '</div></div>'
    + '<div class="kpi-card"><div class="kpi-label">Connected Tables</div><div class="kpi-value">' + connectedTables.size + '</div></div>'
    + '</div>'
    + (RELS.length > 0 ? relCards : '<div class="data-table-wrap"><div class="empty-state" style="padding:3rem">No relationships discovered.<br><span style="font-size:0.8rem">This can happen when tables use unique column name prefixes (like TPC-DS) or have no declared foreign keys.</span></div></div>')
  );
}

// ============================================================
// INIT
// ============================================================
renderOverview();
renderTables("");
renderColumns("");
renderQuality();
renderCorrelations();
renderMissing();
renderPatterns();
renderRelationships();
handleHash();
</script>
</body>
</html>"""
