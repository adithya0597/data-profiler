# Multi-Engine Data Profiler

Extracts statistical metadata from databases for data quality analysis and knowledge graph enrichment. Profiles tables across **Snowflake**, **Databricks**, **DuckDB**, and **SQLite**, producing structured output consumable by data catalogs, lineage systems, and constraint engines.

## Design Intent

This project implements the **metadata ingestion layer** of a larger knowledge platform — the component responsible for extracting typed, structured facts from heterogeneous data sources so that downstream systems (catalogs, lineage graphs, constraint engines) can consume them without per-engine translation logic.

The architecture reflects this:

- **Deterministic, typed metadata.** The 8-canonical-type system is a portable schema language: every engine's native type vocabulary normalizes to a single taxonomy, so a `TIMESTAMP_NTZ` from Snowflake and a `DATETIME` from DuckDB produce structurally identical metadata. This is the same problem a knowledge graph's type system solves — one representation, many sources.
- **Declarative dispatch.** The `AGGREGATE_MAP` in `stats_worker.py` is a type→expression dispatch table — given a canonical type, it emits the correct SQL aggregates for that type on that engine. Adding a new metric or a new engine means extending the map, not rewriting control flow. This pattern is a step toward a DSL for profiling rules: define *what* to compute declaratively, let the engine adapter handle *how*.
- **Graph-ready output.** The relationship worker discovers FK edges and functional dependencies statistically. The OpenMetadata exporter serializes table/column entities in catalog-native schema. These are not reporting features — they produce the nodes and edges a knowledge graph ingests directly.
- **Engine abstraction via capability flags.** Each adapter declares what it supports (`supports_hll`, `supports_percentiles`, `supports_stddev`) and the profiling logic adapts without branching. This is the adapter pattern designed for a world where new engines are added by implementing an interface, not by modifying the core.

The natural evolution is toward a **declarative profiling DSL** — where type mappings, anomaly rules, constraint patterns, and output schemas are defined as configuration rather than code, and the profiler becomes an interpreter that executes those definitions against any connected engine. The current `ProfilerConfig` (Pydantic model with typed toggles for every analysis pass) and `AGGREGATE_MAP` (type-keyed expression templates) are the foundation for that direction.

---

## Feature Highlights

### Schema Intelligence
- **10 canonical types** normalize across engine vocabularies — `TIMESTAMP_NTZ`, `TIMESTAMPTZ`, and `DATETIME` all map to `datetime`; Snowflake VARIANT/OBJECT/ARRAY and Databricks MAP/STRUCT map to `semi_structured` — enabling cross-engine metadata comparison
- **FK relationship inference** — discovers foreign key relationships statistically from column names, types, cardinality, and value overlap (no DDL access required)
- **Semantic FK discovery** — detects naming-convention relationships (e.g., `ss_customer_sk` → `customer.c_customer_sk`) via suffix pattern matching (`*_sk`, `*_id`, `*_key`)
- **Composite key detection** — identifies multi-column FK relationships by matching declared composite primary keys across tables
- **Constraint suggestions** — derives `NOT NULL`, `UNIQUE`, and `CHECK` range recommendations from observed data distributions
- **Functional dependency detection** — identifies columns that determine other columns within a table

### Statistical Depth
- **30+ metrics per column** including mean, stddev, percentiles (p5/p25/p50/p75/p95), IQR, MAD, skewness, kurtosis, coefficient of variation, monotonicity flag
- **Histograms** (20-bin auto-range) with KDE density overlay
- **Benford's Law** analysis with chi-squared p-value (Wilson-Hilferty approximation, no scipy)
- **Pearson and Spearman correlation matrix** for numeric columns
- **Cramér's V** for string×string pairs, **NMI** for mixed-type pairs

### Data Quality
- **19 anomaly rules**: high null rate, single-value columns, all-unique columns, empty string dominance, PII patterns, Benford deviation, IQR outliers, z-score outliers, skewness, boolean imbalance, stale data, suspicious uniform length, low-cardinality numerics, leading/trailing whitespace
- **PII and semantic pattern detection**: email, SSN, credit card, phone, IPv4, US ZIP, UUID, URL
- **Duplicate detection** across tables via row fingerprinting
- **Data quality scoring** (0–100) per table — persisted in output (NDJSON/YAML/Parquet/CSV), not just dashboard-only

### Graph & Catalog Integration
- **Property graph export (JSON-LD, GraphML)** — tables, columns, and relationships as nodes and edges with stable URN identifiers (`urn:profiler:{db}:{schema}:{table}:{col}`), ready for Neo4j, Apache Atlas, DataHub, or any graph database
- **OpenMetadata-compatible JSON export** — table and column entities in OpenMetadata's schema, ready for catalog ingestion
- **NDJSON streaming output** — each table flushes to disk on completion; a crash at table 200/247 preserves 199 profiles
- **YAML, Parquet, and CSV output** — multiple formats for different downstream consumers
- **Interactive HTML dashboard** — 8-section dark-theme dashboard with drill-down, correlation heatmaps, and quality scoring

### Incremental Profiling
- **Delta detection** — skips unchanged tables between runs using a 3-strategy cascade: schema hash → watermark column → row count comparison
- **42x speedup** on repeated runs (tested on TPC-DS 1GB: 66s full → 1.6s incremental with all tables unchanged)
- **Watermark-based detection** — for append-only tables, compares `MAX(watermark_column)` against prior run
- **Automatic baseline storage** — every run stores profile snapshots for future incremental comparison, no configuration needed
- **Statistical merging** via Welford's parallel algorithm — merges mean, variance, stddev, min/max, counts across profile snapshots

---

## Quick Start

Requires **Python 3.10+**.

```bash
# Install
pip install -e ".[dev]"

# Run demo (self-bootstrapping — generates synthetic data if TPC-DS not present)
python demo.py

# Profile a database
profiler run --engine duckdb --dsn "duckdb:///data/mydb.duckdb" --sample 10000 -o profiles/output.json
```

The demo runs 5 phases automatically and produces these files in `profiles/`:

| File | Description |
|------|-------------|
| `synthetic_duckdb.ndjson` | Per-table profiling results (DuckDB) |
| `demo_sqlite.ndjson` | Per-table profiling results (SQLite cross-engine comparison) |
| `profile_openmetadata.json` | OpenMetadata-compatible catalog export |
| `profile_report.html` | Self-contained HTML report — open in any browser |
| `profile_dashboard.html` | Interactive 8-section dashboard — open in any browser |

No external data required. If `data/tpcds_1gb.duckdb` is present, the demo uses the full TPC-DS 1GB dataset (24 tables, ~19.6M rows) instead of generating synthetic data.

---

## Architecture

```
data_profiler/
├── adapters/               # One adapter per engine
│   ├── base.py             # Abstract interface: connect(), sample_clause(), quote_identifier()
│   ├── duckdb.py           # HLL, reservoir sampling, approx_quantile, skewness/kurtosis
│   ├── snowflake.py        # TABLESAMPLE BERNOULLI, PERCENTILE_CONT, HLL_COUNT
│   ├── databricks.py       # Backtick quoting, TABLESAMPLE, approx_count_distinct
│   └── sqlite.py           # Exact COUNT(DISTINCT), Python-side stddev/percentiles
├── workers/
│   ├── schema_worker.py    # Table/column discovery via SQLAlchemy Inspector
│   ├── stats_worker.py     # Type-aware aggregate dispatch, batched stats queries per table
│   ├── relationship_worker.py  # FK inference: declared, semantic (naming patterns), composite, and value overlap
│   ├── delta_worker.py     # Incremental delta detection (schema hash, watermark, row count)
│   └── merge_worker.py     # Welford's parallel algorithm for statistical merging
├── schema/
│   └── portable_types.py   # 10 canonical types with cross-engine type mapping
├── enrichment/
│   ├── anomaly.py          # 19 rule-based anomaly flags
│   ├── patterns.py         # PII and semantic pattern detection (regex-based)
│   ├── constraint_suggester.py  # NOT NULL / UNIQUE / CHECK range recommendations
│   └── constraints.py      # Declared PK, FK, UNIQUE, CHECK constraint discovery via Inspector
├── persistence/
│   ├── checkpoint.py       # SQLite-based resume support (crash recovery)
│   ├── serializers.py      # NDJSON, YAML, Parquet, CSV serializers
│   ├── openmetadata.py     # OpenMetadata-compatible catalog export
│   ├── graph_model.py      # Property graph builder (nodes, edges, URNs)
│   ├── jsonld_serializer.py  # JSON-LD graph serializer (@context + @graph)
│   ├── graphml_serializer.py # GraphML XML serializer (Gephi/yEd compatible)
│   └── profile_store.py    # SQLite-backed profile snapshots for incremental mode
├── cli.py                  # Click CLI with Rich progress bars
├── config.py               # Pydantic configuration model (all runtime toggles)
├── run.py                  # Orchestrator: parallel workers, checkpoint integration
├── report.py               # Self-contained HTML report (inline CSS/JS)
└── dashboard.py            # Interactive 8-section dark-theme dashboard
```

### Key Design Decisions

**Batched aggregate query.** Core column statistics (min, max, mean, stddev, null count, percentiles) are computed in a single `SELECT` per table, batched for wide tables. Distinct counts run as a separate full-table HLL query when sampling is active. Distribution analysis (histograms, Benford, correlations) and per-column value queries (top-N, uniqueness) run as follow-up queries after the main aggregate pass.

**Type-aware aggregate dispatch.** `AGGREGATE_MAP` in `stats_worker.py` maps each canonical type to valid SQL expressions. No `AVG` on strings, no `STDDEV` on booleans. Avoids silently computing meaningless values.

**Engine-specific sampling.** Each adapter uses the engine's native sampling mechanism: `USING SAMPLE (n ROWS) (reservoir, 42)` (DuckDB), `TABLESAMPLE BERNOULLI` (Snowflake/Databricks), `ORDER BY RANDOM() LIMIT` (SQLite). Distinct counts always run on the full table via HLL regardless of sample size.

**Identifier quoting.** All column and table names are passed through `adapter.quote_identifier()` before interpolation into SQL. DuckDB/Snowflake/SQLite use double-quotes; Databricks uses backticks. Prevents SQL injection via column names from arbitrary source systems.

---

## Column Statistics

Each column profile includes:

| Field | Types | Description |
|-------|-------|-------------|
| `null_count`, `null_rate` | all | Null values and fraction |
| `approx_distinct`, `distinct_mode` | all | HLL estimate or exact COUNT(DISTINCT) |
| `unique_count`, `uniqueness_ratio` | all | Values appearing exactly once |
| `min`, `max` | numeric, date, datetime, string | Range boundaries |
| `mean`, `stddev`, `variance` | numeric | Central tendency and spread |
| `sum` | numeric | Column total |
| `p5`, `p25`, `median`, `p75`, `p95` | numeric | Percentiles |
| `iqr`, `range` | numeric | Interquartile range and full range |
| `mad` | numeric | Median absolute deviation |
| `skewness`, `kurtosis` | numeric | Distribution shape |
| `cv` | numeric | Coefficient of variation (stddev/mean) |
| `zero_count`, `negative_count` | numeric | Special value counts |
| `infinite_count` | float | Inf/-Inf count |
| `is_monotonic_increasing/decreasing` | numeric | Monotonicity flag |
| `min_length`, `max_length`, `avg_length` | string | String length statistics |
| `empty_count`, `whitespace_count` | string | Empty and whitespace-only string counts |
| `leading_trailing_whitespace_count` | string | Values with leading/trailing spaces |
| `true_count`, `true_rate` | boolean | Boolean distribution |
| `date_range_days`, `granularity_guess` | date/datetime | Date span and cadence (daily/weekly/monthly/yearly) |
| `top_values`, `bottom_values` | all | Top/bottom 5 frequent values |
| `histogram` | numeric, date | 20-bin frequency distribution |
| `benford` | numeric | Leading-digit distribution + chi-sq p-value |
| `anomalies` | all | List of triggered anomaly rule names |

---

## Sample Output

```json
{
  "name": "employees",
  "total_row_count": 5000,
  "sampled_row_count": 5000,
  "data_quality_score": 94.2,
  "columns": [
    {
      "name": "salary",
      "canonical_type": "float",
      "null_rate": 0.0,
      "approx_distinct": 4987,
      "min": 42300.0,
      "max": 298400.0,
      "mean": 98241.6,
      "stddev": 41823.1,
      "p25": 67800.0,
      "median": 89200.0,
      "p75": 121400.0,
      "skewness": 1.24,
      "anomalies": ["benford_deviation"]
    }
  ]
}
```

---

## CLI Reference

The CLI has three commands: `run` (profile tables), `dashboard` (generate interactive HTML), and `export` (catalog-compatible output).

### `profiler run`

```bash
profiler run \
  --engine duckdb \
  --dsn "duckdb:///data/mydb.duckdb" \
  --sample 10000 \
  --concurrency 4 \
  --output-format json \
  --output profiles/output.json
```

| Flag | Default | Description |
|------|---------|-------------|
| `--engine` | required | `snowflake`, `databricks`, `duckdb`, `sqlite` |
| `--dsn` | required | SQLAlchemy connection string |
| `--database` | all | Database/catalog to profile |
| `--schema` | all | Schema filter |
| `--sample` | 10000 | Rows sampled per table (0 = full scan) |
| `--concurrency` | 4 | Parallel table workers |
| `--stats-depth` | full | `full` = all statistics; `fast` = schema only |
| `--exact-distinct` | false | Force `COUNT(DISTINCT)` instead of HLL |
| `--column-batch-size` | 80 | Max columns per aggregate SELECT (for wide tables) |
| `--output-format` | json | `json`, `yaml`, `parquet`, `html`, `jsonld`, `graphml` |
| `--output`, `-o` | auto | Output file path (default: `profiles/{run_id}.ndjson`) |
| `--resume` | — | Resume a previous run by run ID |
| `--incremental` | false | Only re-profile tables that changed since prior run |
| `--watermark-column` | — | Timestamp/sequence column for append-only delta detection |
| `--prior-run-id` | — | Run ID to compare against (auto-detected if omitted) |
| `-v`, `--verbose` | false | Enable debug logging |

### `profiler dashboard`

Profiles a database and generates an interactive HTML dashboard in one step.

```bash
profiler dashboard \
  --engine duckdb \
  --dsn "duckdb:///data/mydb.duckdb" \
  --sample 10000 \
  -o profiles/dashboard.html

# Open the result
open profiles/dashboard.html
```

### `profiler export`

Profiles a database and exports results in catalog-compatible format.

```bash
profiler export \
  --engine snowflake \
  --dsn "snowflake://user:pass@account/mydb/public?warehouse=WH" \
  --format openmetadata-json \
  -o profiles/catalog_export.json
```

---

## DSN Connection Strings

```bash
# DuckDB (local file)
duckdb:///path/to/file.duckdb

# SQLite
sqlite:///path/to/file.db

# Snowflake
snowflake://user:pass@account/database/schema?warehouse=WH&role=ROLE

# Databricks
databricks://token:TOKEN@hostname:443/sql/1.0/warehouses/ID?catalog=CATALOG&schema=SCHEMA
```

---

## Usage Examples

### Quick scan of a local DuckDB file

```bash
# Schema + basic stats only (fast mode, no histograms/correlations/Benford)
profiler run --engine duckdb --dsn "duckdb:///data/warehouse.duckdb" --stats-depth fast -o profiles/quick.json
```

### Full audit with dashboard

```bash
# Full profiling + interactive dashboard
profiler dashboard --engine duckdb --dsn "duckdb:///data/warehouse.duckdb" --sample 50000 -o profiles/audit.html
open profiles/audit.html
```

### Profile a Snowflake warehouse

```bash
# Profile a specific schema in Snowflake, sampling 10K rows per table
profiler run \
  --engine snowflake \
  --dsn "snowflake://analyst:$SF_PASS@xy12345.us-east-1/PROD_DB/PUBLIC?warehouse=COMPUTE_WH&role=DATA_READER" \
  --database PROD_DB \
  --schema PUBLIC \
  --sample 10000 \
  --concurrency 8 \
  -o profiles/snowflake_prod.json
```

Snowflake adapter uses `SAMPLE (n ROWS)` for native row sampling, `APPROX_COUNT_DISTINCT` for HLL, and `APPROX_PERCENTILE` for quantiles. Parallelism is safe — each worker gets its own connection from the pool.

### Profile a Databricks SQL warehouse

```bash
profiler run \
  --engine databricks \
  --dsn "databricks://token:$DBX_TOKEN@adb-12345.azuredatabricks.net:443/sql/1.0/warehouses/abc123?catalog=main&schema=default" \
  --sample 10000 \
  -o profiles/databricks_main.json
```

Databricks adapter uses backtick quoting, `TABLESAMPLE (n PERCENT)` with Bernoulli sampling, `APPROX_COUNT_DISTINCT`, and `PERCENTILE_APPROX`. Supports native `SKEWNESS()` and `KURTOSIS()`.

### Profile a SQLite database

```bash
profiler run \
  --engine sqlite \
  --dsn "sqlite:///data/app.db" \
  --sample 5000 \
  -o profiles/sqlite_app.json
```

SQLite has no native HLL, STDDEV, or percentiles — the profiler uses exact `COUNT(DISTINCT)`, skips stddev/percentiles (returns null), and samples via `ORDER BY RANDOM() LIMIT`. This is a deliberate accuracy-over-approximation tradeoff: SQLite profiles report only what the engine can compute exactly.

### Export to OpenMetadata catalog

```bash
# Profile + export in catalog-native schema for ingestion
profiler export \
  --engine duckdb \
  --dsn "duckdb:///data/warehouse.duckdb" \
  --format openmetadata-json \
  -o profiles/openmetadata.json
```

Output contains table and column entities with FK relationships, ready for catalog ingestion via OpenMetadata's API.

### Export as property graph (for Gephi, Neo4j, Atlas)

```bash
# JSON-LD — nodes and edges with @context for semantic web / knowledge graph ingestion
profiler run --engine duckdb --dsn "duckdb:///data/warehouse.duckdb" --output-format jsonld -o profiles/graph.jsonld

# GraphML — open directly in Gephi or yEd for visual exploration
profiler run --engine duckdb --dsn "duckdb:///data/warehouse.duckdb" --output-format graphml -o profiles/graph.graphml
```

The graph includes Database, Schema, Table, and Column nodes connected by HAS_SCHEMA, HAS_TABLE, HAS_COLUMN edges, plus FK_DECLARED, FK_SEMANTIC, FK_INFERRED, FK_COMPOSITE, FUNCTIONAL_DEP, and SIMILAR_TO relationship edges. Each node carries its full statistical profile as properties.

### Incremental profiling (skip unchanged tables)

```bash
# First run — profiles all tables and stores snapshots automatically
profiler run --engine duckdb --dsn "duckdb:///data/warehouse.duckdb" -o profiles/baseline.json

# Later run — only re-profiles tables that changed (schema, row count, or watermark)
profiler run --engine duckdb --dsn "duckdb:///data/warehouse.duckdb" --incremental -o profiles/update.json

# With watermark column — detects new rows in append-only tables
profiler run --engine duckdb --dsn "duckdb:///data/warehouse.duckdb" --incremental --watermark-column updated_at -o profiles/delta.json
```

Delta detection checks (in order): schema hash change → watermark column advance → row count change → unchanged (skip). On a TPC-DS 1GB dataset (24 tables, 19.5M rows), incremental mode completes in 1.6s vs 66s for a full run when no tables have changed.

### Resume a crashed run

```bash
# First run crashes at table 47/100
profiler run --engine snowflake --dsn "..." --sample 10000 -o profiles/output.json
# Output shows: Run ID: a1b2c3d4 (resumable)

# Resume — skips the 46 tables already completed
profiler run --engine snowflake --dsn "..." --resume a1b2c3d4 -o profiles/output.json
```

Progress is checkpointed to `profiler_checkpoint.db` after each table completes. Resume skips tables marked as done and retries tables that were in-progress or errored.

### Profile only specific tables

```python
# Programmatic usage with table filtering
from data_profiler.config import ProfilerConfig
from data_profiler.run import run_profiler

config = ProfilerConfig(
    engine="duckdb",
    dsn="duckdb:///data/warehouse.duckdb",
    sample_size=10000,
)
# The CLI profiles all tables; for selective profiling, use the Python API
# and filter results, or use --schema to limit scope
run_id, results = run_profiler(config)
```

---

## Tests

```bash
# Run all tests (534 tests)
pytest tests/ -v

# Unit tests only (no database required, fast)
pytest tests/test_portable_types.py tests/test_anomaly.py tests/test_aggregates.py tests/test_stats.py -v

# End-to-end accuracy tests (builds DuckDB datasets, validates profiler output)
pytest tests/test_e2e.py -v

# Graph output tests (JSON-LD and GraphML serialization)
pytest tests/test_graph.py -v

# Incremental profiling tests (delta detection, merge, profile store)
pytest tests/test_incremental.py -v
```

Tests cover type mapping, aggregate dispatch, anomaly detection, pattern matching, constraint suggestion, relationship inference, graph output (JSON-LD + GraphML), incremental profiling (delta detection + Welford merging), dashboard generation, report generation, OpenMetadata export, duplicate detection, and cross-engine consistency.

---

## Assumptions

- **Privileges:** SELECT on all profiled tables, `INFORMATION_SCHEMA` access for schema discovery.
- **Scope:** Base tables only. Views are excluded (materialized views on Snowflake are included).
- **Sampling:** When `sample_size >= total_row_count`, a full table scan is used instead.
- **HLL accuracy:** ±2% relative error. The `--exact-distinct` flag forces exact counting when precision is required.
- **Relationship discovery:** Assumes FK columns follow a `{singular_table_name}_id` or `{column_name}` naming convention. Non-conventional names require explicit DDL input.
- **Benford's Law:** Applied only to positive numeric columns with >1000 rows. Not meaningful for IDs, codes, or constrained ranges.
- **Read-only:** The profiler never modifies the source database.
