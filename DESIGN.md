# Design Notes: Multi-Engine Data Profiler

> "A data profiler is a metadata extractor. Every statistic it computes is a fact about your data that a knowledge graph can consume."

---

## 1. Design Philosophy

Most data profilers are built for analysts: give me counts, nulls, a histogram. This profiler is built for **metadata systems**: give me structured facts about every column that can feed a catalog, a lineage graph, or a constraint engine.

The distinction shapes every architectural choice:

- **Output is first-class.** The profile is not a report to be read once — it is a structured artifact consumed by downstream systems. Every field in `ColumnProfile` is explicitly named, typed, and serializable to NDJSON, Parquet, or OpenMetadata JSON.
- **Schema intelligence over raw statistics.** The 10-canonical-type system exists so that downstream systems can reason about columns without understanding each engine's native type vocabulary. A Snowflake `TIMESTAMP_NTZ` and a DuckDB `TIMESTAMPTZ` both map to `datetime` — one fact, not two.
- **Relationships are first-class.** FK inference, functional dependency detection, and cross-column correlation are not optional add-ons. They are the primary value a profiler delivers to a knowledge graph. A column stat without its relational context is isolated signal; with it, it becomes a graph edge.

---

## 2. Architecture

```
CLI (cli.py)
  └── Orchestrator (run.py)
        ├── SchemaWorker   — discovers tables, columns, native types
        ├── StatsWorker    — computes per-column statistics via SQL
        ├── RelationshipWorker — FK discovery (declared, semantic, composite, inferred)
        ├── DeltaWorker    — incremental delta detection (schema hash, watermark, row count)
        ├── MergeWorker    — Welford's parallel algorithm for statistical merging
        └── Enrichment layer
              ├── AnomalyDetector  — 19 rule-based anomaly flags
              ├── PatternMatcher   — PII and semantic pattern detection
              ├── ConstraintSuggester — derives DDL recommendations
              └── DuplicateDetector   — cross-table fingerprint matching
  └── Persistence
        ├── Serializers    — NDJSON, YAML, Parquet, CSV
        ├── GraphModel     — property graph builder (nodes, edges, URNs)
        ├── JSONLDSerializer — JSON-LD graph export (@context + @graph)
        ├── GraphMLSerializer — GraphML XML export (Gephi/yEd compatible)
        ├── ProfileStore   — SQLite-backed profile snapshots for incremental mode
        └── OpenMetadata   — catalog-compatible JSON export
  └── Report / Dashboard
        ├── report.py      — self-contained HTML with inline CSS/JS
        └── dashboard.py   — 8-section interactive dark-theme dashboard
```

### Key decision: batched aggregate query per table

Core column statistics (null counts, min/max, mean, stddev, percentiles) are computed in a **single aggregate query** per table. The `AGGREGATE_MAP` dispatches type-aware SQL expressions, concatenates them into one `SELECT`, and executes once per batch. This minimizes network round-trips for the heaviest computation. Distinct counts run as a separate full-table HLL query when sampling is active. Distribution analysis (histograms, Benford's Law, correlations) and per-column queries (top-N values, uniqueness counts) run as follow-up passes. The tradeoff favors correctness (HLL on full data, not the sample) and modularity (each analysis can be toggled independently) over minimizing total query count.

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

The profiler maps every engine's native type vocabulary to 10 canonical types:

| Canonical | Examples |
|-----------|---------|
| `integer` | INT, BIGINT, TINYINT, NUMBER(10,0) |
| `float`   | FLOAT, DOUBLE, DECIMAL(18,4), REAL |
| `string`  | VARCHAR, TEXT, CHAR, STRING, CLOB |
| `boolean` | BOOLEAN, BIT, BOOL |
| `date`    | DATE |
| `datetime` | TIMESTAMP, DATETIME, TIMESTAMPTZ |
| `binary`  | BLOB, BYTEA, VARBINARY |
| `time`    | TIME, TIME_WITH_TIMEZONE |
| `semi_structured` | VARIANT, OBJECT, ARRAY, MAP, STRUCT, GEOGRAPHY, GEOMETRY |
| `unknown` | anything not matched |

**Why 10?** Each type maps to a distinct set of meaningful statistics. `mean` on a `datetime` is undefined. `min_length` on an `integer` is meaningless. By dispatching through a type→aggregates map, the profiler avoids silently computing nonsense values. The `time` type gets min/max, while `semi_structured` types (Snowflake VARIANT, Databricks MAP/STRUCT, spatial types) get count-only since nested aggregation is not meaningful.

**How it enables graph interop:** OpenMetadata, Apache Atlas, and DataHub all use canonical type enumerations. The 10-type system maps directly to these schemas — the export layer does not need per-engine translation logic. A profile captured from DuckDB and one from Snowflake for the same logical table produce structurally identical metadata.

---

## 4. Sampling Strategy

For large tables, profiling the full dataset is impractical. The profiler uses a two-tier strategy:

**Tier 1 — Sampled statistics (fast)**
- Applied when `sample_size < table_row_count`
- DuckDB: `USING SAMPLE {n} ROWS (reservoir, 42)` — reservoir sampling for statistical guarantees
- Snowflake: `TABLESAMPLE BERNOULLI ({pct} PERCENT)` — percentage-based
- SQLite: no native sampling; uses `ORDER BY RANDOM() LIMIT {n}` (expensive but necessary)
- Databricks: `TABLESAMPLE ({pct} PERCENT)` — percentage-based Bernoulli sampling

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

The relationship worker discovers FK relationships through a multi-phase pipeline, combining DDL metadata with statistical and naming-convention heuristics:

### Phase 1: Declared FKs
Foreign keys reported by SQLAlchemy's `Inspector.get_foreign_keys()` are collected with full confidence (1.0). These are ground truth from the database catalog.

### Phase 1.5: Semantic FK Discovery
Columns ending in `_sk`, `_id`, or `_key` are analyzed using naming conventions common in data warehouse schemas (TPC-DS, Kimball-style star schemas):

1. **Suffix stripping**: `ss_customer_sk` → stem `ss_customer`
2. **Prefix stripping**: Remove the source table's column prefix (`ss_` for `store_sales`) → entity `customer`
3. **Table matching**: Check if a table named `customer`, `customer_dim`, or `customers` exists
4. **PK column matching**: Search the target table for a unique column matching `{entity}_sk`, `{entity}_id`, or any column ending in `{entity}{suffix}` (e.g., `c_customer_sk`)
5. **Type compatibility**: Source and target columns must share the same canonical type

Emits `relationship_type="semantic_fk"` with confidence 0.6 (heuristic) or refined by value overlap when an engine connection is available.

### Phase 1.75: Composite Key Detection
Tables with declared multi-column primary keys (from `TableConstraints.primary_key`) are matched against other tables:

1. Identify tables with composite PKs (≥ 2 columns)
2. For each composite PK, check if all PK columns exist in another table with compatible types
3. Validate with multi-column tuple `INTERSECT` SQL when engine is available

Emits `relationship_type="inferred_composite"` with confidence 0.5 (heuristic) or refined by overlap.

### Phase 2: Inferred Relationships
For columns not already matched by earlier phases:

1. **Name matching**: `dept_id` in `employees` → candidate FK for any column named `dept_id` in another table
2. **Type compatibility**: both columns must map to the same canonical type
3. **Uniqueness check**: at least one side must be `all_unique` (potential PK)
4. **Value overlap**: when an engine is available, a sample `INTERSECT` confirms the relationship

Emits `relationship_type="inferred"` with confidence 0.5 (heuristic) or refined by overlap.

### Limitations
Semantic FK discovery relies on naming conventions — tables with non-standard column naming (e.g., `manager_id` → `emp_id`) are only caught by Phase 2's exact name match. Composite detection only matches declared composite PKs, not arbitrary multi-column uniqueness (which would be combinatorially explosive).

**Why this matters for knowledge graphs:** Each discovered FK relationship is a graph edge. The multi-phase pipeline recovers relationships that single-strategy approaches miss — declared FKs cover well-documented schemas, semantic FKs handle warehouse naming patterns, and inferred FKs catch the rest. The result is a connected graph structure that catalog systems can ingest directly via the OpenMetadata export.

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

## 8. Data Quality Score

Each profiled table receives a 0–100 quality score computed after all enrichment passes (anomaly detection, constraint suggestion) and persisted in `ProfiledTable.quality_score`. The score is available in all output formats (NDJSON, YAML, Parquet, CSV) — not just the dashboard.

### Formula
```
score = 100
score -= min(30, total_anomalies × 3)      # anomaly penalty
score -= min(20, avg_null_rate × 40)        # null penalty
score -= min(15, duplicate_rate × 100)      # duplicate penalty
score = max(0, round(score))
```

The capped penalties mean the theoretical minimum score is 35 (all three penalties maxed: 30 + 20 + 15 = 65). A table with an error gets score 0; a table with no columns gets score 100.

### Why persist it?
The dashboard previously computed the score in JavaScript, making it invisible to NDJSON/YAML/Parquet consumers. Downstream systems (catalog UIs, data quality dashboards, alerting pipelines) can now consume the quality score directly from the profiler output without reimplementing the formula. The dashboard falls back to JS computation for backward compatibility with older exports.

---

## 9. Configurability

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

## 10. Property Graph Output

The profiler exports profiling results as a property graph — nodes for databases, schemas, tables, and columns; edges for containment, FK relationships, functional dependencies, and correlations. Two serialization formats are supported: JSON-LD (for semantic web / knowledge graph ingestion) and GraphML (for visual exploration in Gephi or yEd).

### Graph Model

The graph is built by `GraphBuilder` (`persistence/graph_model.py`), which is format-agnostic. Both serializers share the same builder — adding a future format (RDF, Cypher) requires only a new serializer, not a new graph builder.

**Node types:**

| Node Type | Key Properties |
|-----------|----------------|
| Database  | name, engine |
| Schema    | name |
| Table     | name, row_count, quality_score, profiled_at, duration_seconds |
| Column    | name, canonical_type, null_rate, approx_distinct, mean, min, max, patterns, anomalies |

**Edge types:**

| Edge Label | Source → Target | Derived From |
|------------|-----------------|--------------|
| HAS_SCHEMA | Database → Schema | Run header metadata |
| HAS_TABLE | Schema → Table | Each profiled table result |
| HAS_COLUMN | Table → Column | Each column in a table |
| FK_DECLARED | Column → Column | `relationship_type="declared_fk"` |
| FK_SEMANTIC | Column → Column | `relationship_type="semantic_fk"` |
| FK_INFERRED | Column → Column | `relationship_type="inferred"` |
| FK_COMPOSITE | Table → Table | `relationship_type="inferred_composite"` (columns as edge property) |
| FUNCTIONAL_DEP | Column → Column | `ProfiledTable.functional_dependencies` |
| SIMILAR_TO | Column → Column | `ProfiledTable.correlations` (threshold > 0.7) |

### Node Identifiers

All nodes use stable URN identifiers: `urn:profiler:{database}:{schema}:{table}:{column}`. Names are lowercased and whitespace-normalized. These URNs are consistent across runs and formats, enabling graph merging and temporal comparison.

### JSON-LD Format

The JSON-LD serializer (`persistence/jsonld_serializer.py`) produces a `{"@context": {...}, "@graph": [...]}` structure. The `@context` maps property names to a `profiler:` namespace alongside `schema.org` terms. Each node becomes a JSON object with `@id` (the URN) and `@type` (the node type). Each edge becomes a JSON object with `source` and `target` URN references.

### GraphML Format

The GraphML serializer (`persistence/graphml_serializer.py`) produces valid XML with `<key>` declarations for all node and edge properties. Property types are mapped to GraphML primitives (`string`, `int`, `double`). The output loads directly in Gephi — use ForceAtlas2 layout and partition by the `label` attribute for effective visualization.

### Why no new dependencies

JSON-LD uses stdlib `json`. GraphML uses stdlib `xml.etree.ElementTree`. No new packages are introduced.

### Validated topology (TPC-DS 1GB)

Against the TPC-DS 1GB dataset (24 tables, 425 columns, 19.5M rows), the graph produces 451 nodes (1 Database + 1 Schema + 24 Tables + 425 Columns) and 633 edges (1 HAS_SCHEMA + 24 HAS_TABLE + 425 HAS_COLUMN + 13 FK_SEMANTIC + 170 FUNCTIONAL_DEP). Both JSON-LD and GraphML produce identical topology.

---

## 11. Incremental Profiling

For production use, full-scan profiling on every run is wasteful when most tables haven't changed. The incremental profiling system detects changes since the last run and only re-profiles modified tables. Unchanged tables reuse prior results directly.

### Delta Detection Cascade

The `DeltaWorker` (`workers/delta_worker.py`) checks tables in order — the first check that triggers a change causes re-profiling:

1. **Schema hash:** SHA-256 of sorted `(column_name, canonical_type)` pairs. If the hash differs from the prior run, the table schema changed → re-profile.
2. **Watermark column:** If configured (`--watermark-column`), compares `SELECT MAX(watermark_col)` against the prior stored value. If the watermark advanced, new rows were appended → re-profile.
3. **Row count:** `COUNT(*)` compared to the prior `total_row_count`. If different → re-profile.
4. **Unchanged:** If none of the above triggered, the table is unchanged → reuse the prior profile.

These are metadata-only queries (schema reflection, `MAX()`, `COUNT(*)`) — no full table scans occur for unchanged tables.

### Profile Store

`ProfileStore` (`persistence/profile_store.py`) shares the checkpoint database's SQLite connection and thread lock. Every profiling run — incremental or not — stores profile snapshots in a `profile_snapshots` table:

```
(run_id, table_name, profile_json, row_count, column_hash, watermark_value, database, schema_name, stored_at)
```

This means the very first run against a database is a full profile, but it stores the baseline automatically. The next `--incremental` run finds prior snapshots without any additional configuration.

### Statistical Merging (Welford's Parallel Algorithm)

For append-only tables detected via watermark, the `MergeWorker` (`workers/merge_worker.py`) queries only new rows and merges statistics with the prior profile:

| Stat | Merge Strategy |
|------|----------------|
| null_count, sum, zero_count, negative_count | Additive |
| min, max | `min(old, new)`, `max(old, new)` |
| mean | Weighted: `(n_a × μ_a + n_b × μ_b) / (n_a + n_b)` |
| variance, stddev | Welford parallel: `M2_ab = M2_a + M2_b + δ² × n_a × n_b / n_ab` |
| approx_distinct | `max(prior, delta)` — conservative estimate |
| histogram, top_values, benford | Not mergeable — use delta's values |
| patterns, anomalies | Union of both sets |

### Performance

On TPC-DS 1GB (24 tables, 19.5M rows), incremental mode with no changes completes in **1.6 seconds** versus **66 seconds** for a full run — a **42× speedup**. The time is dominated by metadata queries (24 `COUNT(*)` checks). Tables with changes are re-profiled normally.

### Key Design Decisions

- **Always store baselines.** `profile_store` is always active, not gated behind `--incremental`. This removes the footgun of needing to remember `--store-baseline` on the first run.
- **Cheap delta detection.** Schema hash and row count are metadata-only queries. Watermark is a single `MAX()`. No full table scans for unchanged tables.
- **Conservative distinct estimate.** `max(prior, delta)` for `approx_distinct` is always ≥ truth. True HLL union requires engine-specific binary sketch access — deferred.
- **Shared SQLite.** ProfileStore uses CheckpointDB's connection and lock — no second database file, thread-safe by construction.

---

## 12. What's Next

### Lineage tracking
Column-level lineage — "this output column was derived from these input columns via this transformation" — is the highest-value metadata for a data quality platform. The profiler could detect lineage by comparing column value distributions across tables (high Pearson correlation + matching cardinality + matching min/max suggests a copy or near-copy relationship).

### Real-time streaming profiles
Batch profiling on snapshots misses drift. A streaming variant would maintain running sketches (Count-Min Sketch for frequency, HLL for cardinality, t-digest for quantiles) over a Kafka topic or Delta Lake stream, emitting profile updates as the data changes rather than on a schedule.

### Atlas/DataHub native export
The OpenMetadata and property graph exports cover generic catalog schemas. Apache Atlas and DataHub have specific entity models (Atlas uses TYPEDEF-based type system; DataHub uses MCE/MCPs with aspect versioning). Adding native exports for each would make the profiler a universal metadata feeder — profiling once, delivering to any catalog.

### Declarative profiling DSL
The `ProfilerConfig` (Pydantic model with typed toggles) and `AGGREGATE_MAP` (type→expression dispatch) are the foundation for a declarative system where type mappings, anomaly rules, constraint patterns, and output schemas are defined as configuration rather than code. The profiler would become an interpreter that executes those definitions against any connected engine.

### HLL sketch union for incremental distinct counts
The current incremental mode uses `max(prior, delta)` as a conservative estimate for approximate distinct counts. True HLL sketch union requires engine-specific binary sketch access (DuckDB exposes this; Snowflake/Databricks do not). Once implemented, incremental distinct counts would be as accurate as full-scan HLL.

---

## Assumptions

1. The profiler is read-only — it never modifies the source database.
2. `sample_size` rows are representative. Reservoir sampling (DuckDB) provides statistical guarantees; other engines use engine-native sampling which may introduce ordering bias on sorted datasets.
3. HLL cardinality estimates have ±2% relative error. The `hll_guard` mitigates the worst cases.
4. Relationship discovery covers declared FKs, naming-convention patterns (`*_sk`, `*_id`, `*_key`), composite PK matching, and exact column name matching. Non-conventional naming with no column name overlap may still require explicit DDL input.
5. Benford's Law analysis is only meaningful for naturally-occurring numeric data (financial amounts, IDs, measurements). It is not applied to columns flagged as `boolean`, `date`, or where `min < 0`.
