# Candidate Take-Home: Data Profiler Tool

## Project Brief

Build a configurable data-profiling utility that can scan hundreds of tables across different databases and persist a compact summary of table/column metadata and statistics. You are encouraged to use LLM-based coding assistants.

## Requirements

### Supported Databases

- At minimum: Snowflake, Databricks (SQL warehouse), DuckDB, and SQLite.  
- We can provide access to a demo warehouse with hundreds of tables/columns; feel free to mock additional sources if needed for local testing.

### Collected Metadata

- **Per table:** row count, complete schema (column names, types, nullability, comments if available).  
- **Per column:** min value, max value, distinct-count estimate, physical datatype.  
- Optional but encouraged: histograms, null-counts, or additional descriptive statistics where practical.  
- Define a portable schema (e.g., JSON Schema–like) to represent table/column types so profiles from different engines can be compared.

### Tool Behavior

- **Performance:** support strategies such as sampling, predicate-pushdown, or parallel workers to keep runtimes reasonable on large schemas.  
- **Configurability:** expose knobs to trade accuracy vs. speed (sample size, concurrency, stats depth, etc.).  
- **Resilience (optional):** persist incremental state so a run can resume after failure without restarting from scratch.  
- **Output:** serialize results as a persistent data structure (JSON, YAML, Parquet, pickle, etc.) that downstream tools can consume.

## Deliverables

1. Minimal working project (Python preferred, but other languages are acceptable).  
2. Design notes or well-documented code explaining architecture, configurability, and type-schema decisions.  
3. Optional: automated tests (unit/integration) or a demo/script that exercises the profiler end to end.

## Expectations

- Aim for pragmatic engineering quality: clear abstractions for database adapters, profiling workers, and persistence.  
- Document assumptions about privileges, network connectivity, or warehouse features (e.g., Snowflake `RESULT_SCAN`).  
- Showcase how you validate correctness/performance (logs, metrics, test cases, or reasoned analysis).

Feel free to reach out if you need the demo database credentials or have clarifying questions.  
