# Design Notes: Multi-Engine Data Profiler

> "A data profiler is a metadata extractor. Every statistic it computes is a fact about your data that a knowledge graph can consume."

---

## 1. Design Philosophy

Most data profilers are built for analysts: give me counts, nulls, a histogram. This profiler is built for **metadata systems**: give me structured facts about every column that can feed a catalog, a lineage graph, or a constraint engine.

The distinction shapes every architectural choice:

- **Output is first-class.** The profile is not a report to be read once — it is a structured artifact consumed by downstream systems. Every field in `ColumnProfile` is explicitly named, typed, and serializable to NDJSON, Parquet, or OpenMetadata JSON.
- **Schema intelligence over raw statistics.** The 8-canonical-type system exists so that downstream systems can reason about columns without understanding each engine's native type vocabulary. A Snowflake `TIMESTAMP_NTZ` and a DuckDB `TIMESTAMPTZ` both map to `datetime` — one fact, not two.
- **Relationships are first-class.** FK inference, functional dependency detection, and cross-column correlation are not optional add-ons. They are the primary value a profiler delivers to a knowledge graph. A column stat without its relational context is isolated signal; with it, it becomes a graph edge.

---

## 2. Architecture

```
CLI (cli.py)
  └── Orchestrator (run.py)
        ├── SchemaWorker   — discovers tables, columns, native types
        ├── StatsWorker    — computes per-column statistics via SQL
        ├── RelationshipWorker — infers FK relationships statistically
        └── Enrichment layer
              ├── AnomalyDetector  — 19 rule-based anomaly flags
              ├── PatternMatcher   — PII and semantic pattern detection
              ├── ConstraintSuggester — derives DDL recommendations
              └── DuplicateDetector   — cross-table fingerprint matching
  └── Persistence
        ├── Serializers    — NDJSON, YAML, Parquet, CSV
        └── OpenMetadata   — catalog-compatible JSON export
  └── Report / Dashboard
        ├── report.py      — self-contained HTML with inline CSS/JS
        └── dashboard.py   — 8-section interactive dark-theme dashboard
```

### Key decision: single SQL pass per table

All column statistics for a table are computed in **one SQL query** (per sampling tier). The `AGGREGATE_MAP` in `stats_worker.py` dispatches type-aware expression templates, concatenates them into a single `SELECT`, and executes once. This means:

- Network round-trips are minimized (critical for remote engines like Snowflake/Databricks)
- The query optimizer sees the full projection and can share scans
- Sampling is applied once at the `FROM` clause level, not per-column

The tradeoff: the query can be wide (400+ expressions for large tables). In practice, every engine tested handles this without issue.

### Why not an ORM?

SQLAlchemy is used **only for connection pooling, reflection, and DDL introspection** — not for building statistics queries. ORM query builders cannot express the heterogeneous aggregate patterns needed (e.g., `approx_quantile`, `HLL_COUNT.INIT`, conditional `CASE WHEN` sums). Raw SQL via `text()` gives full control with engine-specific dialect.

### Adapter pattern

Each engine gets an adapter (`adapters/duckdb.py`, `adapters/snowflake.py`, etc.) that overrides:

- `quote_identifier(name)` — backticks for Databricks, double-quotes for others
- `sample_clause(n, total)` — `USING SAMPLE` for DuckDB, `TABLESAMPLE` for Snowflake, `LIMIT` for SQLite
- `supports_hll`, `supports_stddev`, `supports_percentiles` — capability flags
- `approx_distinct_expr(col)` — HLL expression per engine
- `stddev_expr(col)` — dialect-specific standard deviation

The `BaseAdapter` provides safe defaults (exact `COUNT(DISTINCT)`, no percentiles). Engine-specific capabilities layer on top without changing the core profiling logic.

---

## 3. Type System

The profiler maps every engine's native type vocabulary to 8 canonical types:

| Canonical | Examples |
|-----------|---------|
| `integer` | INT, BIGINT, TINYINT, NUMBER(10,0) |
| `float`   | FLOAT, DOUBLE, DECIMAL(18,4), REAL |
| `string`  | VARCHAR, TEXT, CHAR, STRING, CLOB |
| `boolean` | BOOLEAN, BIT, BOOL |
| `date`    | DATE |
| `datetime` | TIMESTAMP, DATETIME, TIMESTAMPTZ |
| `binary`  | BLOB, BYTEA, VARBINARY |
| `unknown` | anything not matched |

**Why 8?** Each type maps to a distinct set of meaningful statistics. `mean` on a `datetime` is undefined. `min_length` on an `integer` is meaningless. By dispatching through a type→aggregates map, the profiler avoids silently computing nonsense values.

**How it enables graph interop:** OpenMetadata, Apache Atlas, and DataHub all use canonical type enumerations. The 8-type system maps directly to these schemas — the export layer does not need per-engine translation logic. A profile captured from DuckDB and one from Snowflake for the same logical table produce structurally identical metadata.

---

## 4. Sampling Strategy

For large tables, profiling the full dataset is impractical. The profiler uses a two-tier strategy:

**Tier 1 — Sampled statistics (fast)**
- Applied when `sample_size < table_row_count`
- DuckDB: `USING SAMPLE {n} ROWS (reservoir, 42)` — reservoir sampling for statistical guarantees
- Snowflake: `TABLESAMPLE BERNOULLI ({pct} PERCENT)` — percentage-based
- SQLite: no native sampling; uses `ORDER BY RANDOM() LIMIT {n}` (expensive but necessary)
- Databricks: `TABLESAMPLE ({n} ROWS)` — row-count sampling

**Tier 2 — Full-table distinct counts (accurate)**
- `approx_count_distinct` (HLL) runs on the full table in a separate query, regardless of sampling
- This is the most expensive single operation; batched per-table with all columns in one HLL query

**Why HLL instead of exact `COUNT(DISTINCT)`?**
Exact distinct counting requires a full sort or hash table over the column. For a 1GB table with VARCHAR columns, this can exceed memory and take minutes. HLL (HyperLogLog) gives ±2% accuracy in O(1) memory in constant time. For profiling purposes — is this column a PK candidate? does it have high cardinality? — 2% error is irrelevant. The `exact_distinct` config flag forces exact counting for use cases where precision matters.

---

## 5. Statistical Choices

### Standard deviation and percentiles
`STDDEV`, `approx_quantile` (DuckDB), and `PERCENTILE_CONT` (Snowflake/Databricks) are computed in-database. SQLite has none of these — values are `null` for SQLite columns. This is a deliberate non-shim: computing approximate percentiles in Python over a 10K-row sample would give different accuracy semantics than in-database computation over the full table.

### Benford's Law
The leading-digit distribution is computed entirely in SQL:
```sql
SELECT FLOOR(ABS(col) / POWER(10, FLOOR(LOG10(ABS(col))))) AS digit, COUNT(*)
FROM t WHERE col > 0
GROUP BY digit ORDER BY digit
```
Chi-squared p-value uses the Wilson-Hilferty normal approximation (no scipy dependency):
```python
chi2 = sum((observed - expected)**2 / expected for ...)
k = 9  # degrees of freedom
x = (chi2 / k) ** (1/3)
mu = 1 - 2/(9*k)
sigma = sqrt(2/(9*k))
z = (x - mu) / sigma
p_value = 0.5 * erfc(z / sqrt(2))  # from math module
```

### Correlation
- **Pearson**: standard in-SQL `CORR()` for numeric pairs
- **Spearman**: computed via rank transformation in Python (two-pass but avoids a second SQL query per pair)
- **Cramér's V**: chi-squared statistic for string×string pairs, computed over the cross-join of top-50 value distributions
- **NMI (Normalized Mutual Information)**: information-theoretic measure for mixed-type pairs

### MAD (Median Absolute Deviation)
MAD requires the median, which is not available mid-aggregate. Computed in Python over the sampled result set: `median(abs(x - median(xs)) for x in xs)`. This is a post-processing step, not a SQL aggregate.

---

## 6. Relationship Discovery

The relationship worker infers foreign key relationships **without access to DDL** — purely from statistical patterns in column data. The heuristic chain:

1. **Name matching**: `dept_id` in `employees` → candidate FK for any column named `dept_id` in another table
2. **Type compatibility**: both columns must map to the same canonical type
3. **Cardinality check**: the referencing column's `approx_distinct` must be ≤ the referenced column's (FKs can't have more unique values than their PK)
4. **Value overlap**: a sample of values from the FK column is checked against the PK column. Overlap ≥ 80% confirms the relationship.

This is not foolproof — it misses FKs with non-matching names (e.g., `manager_id` → `emp_id`), and it can produce false positives when two tables happen to share integer sequences. But for the common case of well-named columns in normalized schemas, it recovers most FK relationships without any catalog metadata.

**Why this matters for knowledge graphs:** An auto-discovered FK relationship is a graph edge. The relationship worker turns a flat set of column profiles into a connected graph structure that catalog systems can ingest directly via the OpenMetadata export.

---

## 7. Constraint Suggestion

The constraint suggester derives DDL recommendations from column statistics:

| Rule | Condition | Suggestion |
|------|-----------|-----------|
| NOT NULL | `null_count == 0` across full scan | `NOT NULL` constraint |
| UNIQUE | `approx_distinct ≈ row_count` (>99.5%) | `UNIQUE` constraint |
| CHECK range | numeric, no nulls, known min/max | `CHECK (col BETWEEN min AND max)` |
| FK | relationship worker confirms FK | `REFERENCES table(col)` |
| ENUM | `approx_distinct ≤ 10` AND `canonical_type == "string"` | `CHECK (col IN (...))` |

These suggestions are emitted as structured objects (not raw SQL strings) so that downstream DDL generation tools can render them in any target dialect.

**Design note:** Suggestions are conservative. A NOT NULL suggestion is only made when null_count is exactly zero across the full table scan — not the sample. The `full_scan_null_count` field tracks this separately from the sampled `null_count` to avoid false recommendations.

---

## 8. Configurability

All runtime behavior is controlled by `ProfilerConfig` (Pydantic model in `config.py`):

```python
class ProfilerConfig(BaseModel):
    engine: str                    # duckdb | snowflake | databricks | sqlite
    dsn: str                       # SQLAlchemy connection string
    sample_size: int = 10_000      # rows per table for sampled stats
    concurrency: int = 4           # parallel table workers
    exact_distinct: bool = False   # force COUNT(DISTINCT) over HLL
    include_tables: list[str] = [] # allowlist (empty = all tables)
    exclude_tables: list[str] = [] # denylist
    run_relationships: bool = True
    run_constraints: bool = True
    run_duplicates: bool = False   # expensive cross-table operation
    run_benford: bool = True
    run_correlations: bool = True
    correlation_max_columns: int = 50  # cap to avoid O(n²) explosion
    hll_guard: bool = True         # skip HLL if table < 10K rows (use exact)
    output_format: str = "ndjson"  # ndjson | yaml | parquet | csv
    output_path: str = "profiles/" # output directory
```

The `hll_guard` flag prevents HLL from being used on small tables where `COUNT(DISTINCT)` is cheaper and exact. `correlation_max_columns` caps the O(n²) correlation matrix computation — for a 500-column table, computing all pairs is 125,000 queries; the cap selects the top-N columns by variance.

---

## 9. What I'd Build Next

### Graph-first output
The current OpenMetadata export is a flat list of table and column entities. A real catalog integration would produce a **property graph** — nodes for tables, columns, and datasets; edges for FK relationships, functional dependencies, and column lineage. The relationship worker already discovers the edges; the missing piece is a graph serialization format (RDF, labeled property graph, or Apache TinkerPop Gremlin).

### Incremental profiling
The current profiler does a full scan on every run. For production use, you want **incremental profiling**: detect new rows since the last run (via a watermark column or CDC stream), profile only the delta, and merge statistics. The checkpoint system (`persistence/checkpoint.py`) is the foundation — it persists partial results and supports resume. The missing piece is statistical merging: `mean` and `stddev` can be merged exactly via Welford's online algorithm; histograms can be merged by bin; HLL sketches can be union-merged natively.

### Lineage tracking
Column-level lineage — "this output column was derived from these input columns via this transformation" — is the highest-value metadata for a data quality platform. The profiler could detect lineage by comparing column value distributions across tables (high Pearson correlation + matching cardinality + matching min/max suggests a copy or near-copy relationship).

### Real-time streaming profiles
Batch profiling on snapshots misses drift. A streaming variant would maintain running sketches (Count-Min Sketch for frequency, HLL for cardinality, t-digest for quantiles) over a Kafka topic or Delta Lake stream, emitting profile updates as the data changes rather than on a schedule.

### Atlas/DataHub native export
The OpenMetadata export covers OpenMetadata's entity schema. Apache Atlas and DataHub have different entity models (Atlas uses TYPEDEF-based type system; DataHub uses MCE/MCPs with aspect versioning). Adding native exports for each would make the profiler a universal metadata feeder — profiling once, delivering to any catalog.

---

## Assumptions

1. The profiler is read-only — it never modifies the source database.
2. `sample_size` rows are representative. Reservoir sampling (DuckDB) provides statistical guarantees; other engines use engine-native sampling which may introduce ordering bias on sorted datasets.
3. HLL cardinality estimates have ±2% relative error. The `hll_guard` mitigates the worst cases.
4. Relationship discovery assumes FK column names follow a `{table_singular_id}` or `{column_name}` convention. Non-conventional naming requires explicit DDL input.
5. Benford's Law analysis is only meaningful for naturally-occurring numeric data (financial amounts, IDs, measurements). It is not applied to columns flagged as `boolean`, `date`, or where `min < 0`.
