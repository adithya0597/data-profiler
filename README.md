# Multi-Engine Data Profiler

Extracts statistical metadata from databases for data quality analysis and knowledge graph enrichment. Profiles tables across **Snowflake**, **Databricks**, **DuckDB**, and **SQLite**, producing structured output consumable by data catalogs, lineage systems, and constraint engines.

## Feature Highlights

### Schema Intelligence
- **8 canonical types** normalize across engine vocabularies — `TIMESTAMP_NTZ`, `TIMESTAMPTZ`, and `DATETIME` all map to `datetime`, enabling cross-engine metadata comparison
- **FK relationship inference** — discovers foreign key relationships statistically from column names, types, cardinality, and value overlap (no DDL access required)
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
- **Data quality scoring** (0–100) aggregated per table and per database

### Catalog Integration
- **OpenMetadata-compatible JSON export** — table and column entities in OpenMetadata's schema, ready for catalog ingestion
- **NDJSON streaming output** — each table flushes to disk on completion; a crash at table 200/247 preserves 199 profiles
- **YAML, Parquet, and CSV output** — multiple formats for different downstream consumers
- **Interactive HTML dashboard** — 8-section dark-theme dashboard with drill-down, correlation heatmaps, and quality scoring

---

## Quick Start

```bash
# Install
pip install -e ".[dev]"

# Run demo (self-bootstrapping — generates synthetic data if TPC-DS not present)
python demo.py

# Profile a database
profiler run --engine duckdb --dsn "duckdb:///data/mydb.duckdb" --sample 10000 -o profiles/output.ndjson
```

The demo generates a synthetic 5-table dataset with FK relationships, PII columns, and numeric distributions — no external data required. If `data/tpcds_1gb.duckdb` is present, the demo uses the full TPC-DS 1GB dataset instead.

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
│   └── relationship_worker.py  # FK inference via name + cardinality + value overlap
├── schema/
│   └── portable_types.py   # 8 canonical types with cross-engine type mapping
├── enrichment/
│   ├── anomaly.py          # 19 rule-based anomaly flags
│   ├── patterns.py         # PII and semantic pattern detection (regex-based)
│   ├── constraint_suggester.py  # NOT NULL / UNIQUE / CHECK range recommendations
│   └── constraints.py      # Declared PK, FK, UNIQUE, CHECK constraint discovery via Inspector
├── persistence/
│   ├── checkpoint.py       # SQLite-based resume support (crash recovery)
│   ├── serializers.py      # NDJSON, YAML, Parquet, CSV serializers
│   └── openmetadata.py     # OpenMetadata-compatible catalog export
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

```bash
profiler run \
  --engine duckdb \
  --dsn "duckdb:///data/mydb.duckdb" \
  --sample 10000 \
  --concurrency 4 \
  --output-format ndjson \
  --output profiles/output.ndjson
```

| Flag | Default | Description |
|------|---------|-------------|
| `--engine` | required | `snowflake`, `databricks`, `duckdb`, `sqlite` |
| `--dsn` | required | SQLAlchemy connection string |
| `--sample` | 10000 | Rows sampled per table (0 = full scan) |
| `--concurrency` | 4 | Parallel table workers |
| `--exact-distinct` | false | Force `COUNT(DISTINCT)` instead of HLL |
| `--output-format` | ndjson | `ndjson`, `yaml`, `parquet`, `csv` |
| `--output` | auto | Output file path |
| `--resume` | — | Resume a previous run by run ID |

---

## DSN Connection Strings

```
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

## Tests

```bash
# Run all tests (328 tests)
pytest tests/ -v

# Unit tests only (no database required, fast)
pytest tests/test_portable_types.py tests/test_anomaly.py tests/test_aggregates.py tests/test_stats.py -v

# Integration tests (requires TPC-DS database)
pytest tests/test_integration.py -v
```

Tests cover type mapping, aggregate dispatch, anomaly detection, pattern matching, constraint suggestion, relationship inference, dashboard generation, report generation, OpenMetadata export, duplicate detection, and cross-engine consistency.

---

## Assumptions

- **Privileges:** SELECT on all profiled tables, `INFORMATION_SCHEMA` access for schema discovery.
- **Scope:** Base tables only. Views are excluded (materialized views on Snowflake are included).
- **Sampling:** When `sample_size >= total_row_count`, a full table scan is used instead.
- **HLL accuracy:** ±2% relative error. The `--exact-distinct` flag forces exact counting when precision is required.
- **Relationship discovery:** Assumes FK columns follow a `{singular_table_name}_id` or `{column_name}` naming convention. Non-conventional names require explicit DDL input.
- **Benford's Law:** Applied only to positive numeric columns with >1000 rows. Not meaningful for IDs, codes, or constrained ranges.
- **Read-only:** The profiler never modifies the source database.
