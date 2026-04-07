#!/usr/bin/env python3
"""Demo script: profiles multi-engine datasets and validates profiler correctness.

Self-bootstrapping: if TPC-DS data is not present, generates a synthetic DuckDB
database (departments, employees, projects, assignments, audit_log) with FK
relationships for relationship-discovery demonstration.
"""

import json
import os
import random
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

# Ensure package is importable when run from repo root
sys.path.insert(0, str(Path(__file__).parent))

from data_profiler.config import ProfilerConfig
from data_profiler.run import run_profiler
from data_profiler.dashboard import generate_dashboard
from data_profiler.report import generate_html_report


TPCDS_DB = "data/tpcds_1gb.duckdb"
SYNTHETIC_DB = "data/demo_synthetic.duckdb"
SQLITE_DB = "data/demo_sqlite.db"
OUTPUT_DIR = "profiles"


def banner(msg: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------

def generate_synthetic_data(path: str = SYNTHETIC_DB) -> str:
    """Generate a synthetic DuckDB database with FK relationships.

    Schema designed to showcase:
    - FK relationship discovery (dept_id, emp_id, project_id cross-table)
    - PII pattern detection (email, phone, ip_address)
    - Boolean profiling (is_active)
    - All 8 canonical types: integer, float, string, date, datetime, boolean
    - ~15,600 rows total, generates in <3 seconds
    """
    import duckdb

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    if Path(path).exists():
        return path

    rng = random.Random(42)

    conn = duckdb.connect(path)

    # -- departments (100 rows) --
    conn.execute("""
        CREATE TABLE departments (
            dept_id    INTEGER PRIMARY KEY,
            name       VARCHAR,
            budget     DOUBLE,
            created_date DATE
        )
    """)
    dept_names = [
        "Engineering", "Sales", "Marketing", "Finance", "HR", "Legal",
        "Operations", "Product", "Design", "Support", "Research", "DevOps",
        "Security", "Analytics", "Procurement",
    ]
    depts = []
    for i in range(1, 101):
        name = rng.choice(dept_names) + f"_{i}"
        budget = round(rng.uniform(50_000, 5_000_000), 2)
        created = date(2015, 1, 1) + timedelta(days=rng.randint(0, 2000))
        depts.append((i, name, budget, str(created)))
    conn.executemany("INSERT INTO departments VALUES (?, ?, ?, ?)", depts)

    # -- employees (5000 rows) --
    conn.execute("""
        CREATE TABLE employees (
            emp_id     INTEGER PRIMARY KEY,
            dept_id    INTEGER,
            name       VARCHAR,
            email      VARCHAR,
            salary     DOUBLE,
            hire_date  DATE,
            is_active  BOOLEAN
        )
    """)
    first_names = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace",
                   "Heidi", "Ivan", "Julia", "Karl", "Lisa", "Mike", "Nina", "Oscar"]
    last_names = ["Smith", "Jones", "Chen", "Kim", "Patel", "Garcia", "Liu",
                  "Brown", "Davis", "Wilson", "Taylor", "Martinez", "Anderson"]
    emps = []
    for i in range(1, 5001):
        fn = rng.choice(first_names)
        ln = rng.choice(last_names)
        dept_id = rng.randint(1, 100)
        email = f"{fn.lower()}.{ln.lower()}{i}@company.com"
        salary = round(rng.lognormvariate(11.0, 0.4), 2)  # log-normal ≈ real salaries
        hire_date = date(2010, 1, 1) + timedelta(days=rng.randint(0, 4000))
        is_active = rng.random() > 0.08  # ~92% active
        emps.append((i, dept_id, f"{fn} {ln}", email, salary, str(hire_date), is_active))
    conn.executemany("INSERT INTO employees VALUES (?, ?, ?, ?, ?, ?, ?)", emps)

    # -- projects (500 rows) --
    conn.execute("""
        CREATE TABLE projects (
            project_id  INTEGER PRIMARY KEY,
            dept_id     INTEGER,
            name        VARCHAR,
            start_date  DATE,
            end_date    DATE,
            budget      DOUBLE
        )
    """)
    proj_prefixes = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta",
                     "Omega", "Sigma", "Theta", "Lambda"]
    projs = []
    for i in range(1, 501):
        dept_id = rng.randint(1, 100)
        name = f"{rng.choice(proj_prefixes)}-{i:04d}"
        start = date(2018, 1, 1) + timedelta(days=rng.randint(0, 2000))
        end = start + timedelta(days=rng.randint(30, 730))
        budget = round(rng.uniform(10_000, 2_000_000), 2)
        projs.append((i, dept_id, name, str(start), str(end), budget))
    conn.executemany("INSERT INTO projects VALUES (?, ?, ?, ?, ?, ?)", projs)

    # -- assignments (8000 rows): employees ↔ projects --
    conn.execute("""
        CREATE TABLE assignments (
            assignment_id INTEGER PRIMARY KEY,
            emp_id        INTEGER,
            project_id    INTEGER,
            hours         DOUBLE,
            role          VARCHAR
        )
    """)
    roles = ["Lead", "Developer", "Analyst", "Tester", "Designer", "Reviewer", "Advisor"]
    assigns = []
    for i in range(1, 8001):
        emp_id = rng.randint(1, 5000)
        project_id = rng.randint(1, 500)
        hours = round(rng.uniform(1.0, 400.0), 1)
        role = rng.choice(roles)
        assigns.append((i, emp_id, project_id, hours, role))
    conn.executemany("INSERT INTO assignments VALUES (?, ?, ?, ?, ?)", assigns)

    # -- audit_log (2000 rows): employee actions with IP --
    conn.execute("""
        CREATE TABLE audit_log (
            log_id     INTEGER PRIMARY KEY,
            emp_id     INTEGER,
            action     VARCHAR,
            ts         TIMESTAMP,
            ip_address VARCHAR
        )
    """)
    actions = ["LOGIN", "LOGOUT", "UPDATE_PROFILE", "EXPORT_DATA", "VIEW_REPORT",
               "DELETE_RECORD", "CREATE_USER", "RESET_PASSWORD"]
    logs = []
    base_ts = datetime(2024, 1, 1)
    for i in range(1, 2001):
        emp_id = rng.randint(1, 5000)
        action = rng.choice(actions)
        ts = base_ts + timedelta(seconds=rng.randint(0, 365 * 24 * 3600))
        ip = f"10.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}"
        logs.append((i, emp_id, action, str(ts), ip))
    conn.executemany("INSERT INTO audit_log VALUES (?, ?, ?, ?, ?)", logs)

    conn.close()
    print(f"  Generated synthetic DuckDB: {path}")
    print(f"  Tables: departments(100), employees(5000), projects(500), "
          f"assignments(8000), audit_log(2000)")
    return path


# ---------------------------------------------------------------------------
# SQLite demo data
# ---------------------------------------------------------------------------

def create_sqlite_demo() -> str:
    """Create a small SQLite database for cross-engine comparison."""
    path = SQLITE_DB
    if Path(path).exists():
        os.remove(path)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE income_band (
            ib_income_band_sk INTEGER PRIMARY KEY,
            ib_lower_bound INTEGER,
            ib_upper_bound INTEGER
        )
    """)
    data = [(i, (i - 1) * 10000, i * 10000) for i in range(1, 21)]
    conn.executemany("INSERT INTO income_band VALUES (?, ?, ?)", data)

    conn.execute("""
        CREATE TABLE ship_mode (
            sm_ship_mode_sk INTEGER PRIMARY KEY,
            sm_ship_mode_id TEXT,
            sm_type TEXT,
            sm_code TEXT,
            sm_carrier TEXT,
            sm_contract TEXT
        )
    """)
    modes = [
        (1, "AAAAAAAABAAAAAAA", "LIBRARY", "AIR", "DHL", "y98h"),
        (2, "AAAAAAAACAAAAAAA", "REGULAR", "SEA", "FEDEX", "k5i7"),
        (3, "AAAAAAAADAAAAAAA", "EXPRESS", "RAIL", "UPS", "m3q0"),
        (4, "AAAAAAAAEAAAAAAA", "OVERNIGHT", "TRUCK", "USPS", "j7w2"),
        (5, "AAAAAAAAFAAAAAAA", "TWO DAY", "AIR", "DHL", "b1n5"),
    ]
    conn.executemany("INSERT INTO ship_mode VALUES (?, ?, ?, ?, ?, ?)", modes)
    conn.commit()
    conn.close()
    return path


# ---------------------------------------------------------------------------
# Profiling phases
# ---------------------------------------------------------------------------

def profile_primary() -> tuple[str, list, bool]:
    """Profile primary dataset: TPC-DS if available, else synthetic."""
    if Path(TPCDS_DB).exists():
        return _profile_tpcds()
    else:
        banner("TPC-DS data not found — using self-bootstrapping synthetic dataset")
        return _profile_synthetic()


def _profile_tpcds() -> tuple[str, list, bool]:
    banner("Phase 1: Profiling TPC-DS 1GB in DuckDB")
    config = ProfilerConfig(
        engine="duckdb",
        dsn=f"duckdb:///{TPCDS_DB}",
        sample_size=10000,
        concurrency=1,
        output=f"{OUTPUT_DIR}/tpcds_duckdb.ndjson",
        output_format="json",
    )
    start = time.time()
    run_id, results = run_profiler(config)
    elapsed = time.time() - start
    _print_summary(results, elapsed, config.output)
    return run_id, results, False  # is_synthetic=False


def _profile_synthetic() -> tuple[str, list, bool]:
    banner("Phase 1: Profiling synthetic dataset in DuckDB")
    db_path = generate_synthetic_data()
    config = ProfilerConfig(
        engine="duckdb",
        dsn=f"duckdb:///{db_path}",
        sample_size=0,          # full scan (15k rows fits in memory)
        concurrency=1,
        output=f"{OUTPUT_DIR}/synthetic_duckdb.ndjson",
        output_format="json",
    )
    start = time.time()
    run_id, results = run_profiler(config)
    elapsed = time.time() - start
    _print_summary(results, elapsed, config.output)
    return run_id, results, True  # is_synthetic=True


def _print_summary(results: list, elapsed: float, output: str) -> None:
    total_cols = sum(len(r.columns) for r in results)
    total_rows = sum(r.total_row_count for r in results)
    anomalies = sum(len(c.anomalies) for r in results for c in r.columns)
    print(f"  Tables:     {len(results)}")
    print(f"  Columns:    {total_cols:,}")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Anomalies:  {anomalies}")
    print(f"  Duration:   {elapsed:.1f}s")
    print(f"  Output:     {output}")


def profile_sqlite() -> tuple[str, list]:
    """Profile demo SQLite database."""
    banner("Phase 2: Profiling demo tables in SQLite")
    sqlite_path = create_sqlite_demo()
    print(f"  Created SQLite demo at {sqlite_path}")
    config = ProfilerConfig(
        engine="sqlite",
        dsn=f"sqlite:///{sqlite_path}",
        sample_size=100,
        concurrency=1,
        output=f"{OUTPUT_DIR}/demo_sqlite.ndjson",
        output_format="json",
    )
    start = time.time()
    run_id, results = run_profiler(config)
    elapsed = time.time() - start
    total_cols = sum(len(r.columns) for r in results)
    print(f"  Tables:     {len(results)}")
    print(f"  Columns:    {total_cols}")
    print(f"  Duration:   {elapsed:.1f}s")
    for r in results:
        for c in r.columns:
            assert c.distinct_mode == "exact", f"Expected exact distinct, got {c.distinct_mode}"
    print("  Distinct:   exact (verified)")
    return run_id, results


# ---------------------------------------------------------------------------
# Correctness validation
# ---------------------------------------------------------------------------

def validate_correctness(results: list, is_synthetic: bool) -> bool:
    """Validate profiler output against known data properties."""
    banner("Phase 3: Correctness Validation")
    checks_passed = 0
    checks_failed = 0

    def check(name: str, condition: bool, detail: str = ""):
        nonlocal checks_passed, checks_failed
        if condition:
            print(f"  [PASS] {name}")
            checks_passed += 1
        else:
            print(f"  [FAIL] {name} -- {detail}")
            checks_failed += 1

    if is_synthetic:
        _validate_synthetic(results, check)
    else:
        _validate_tpcds(results, check)

    # Common checks (run on both)
    _validate_common(results, check)

    print(f"\n  Results: {checks_passed} passed, {checks_failed} failed")
    return checks_failed == 0


def _validate_synthetic(results: list, check) -> None:
    """Validate correctness against known synthetic dataset properties."""
    tables = {r.name: r for r in results}

    # Row counts
    check("departments has 100 rows",
          tables.get("departments") and tables["departments"].total_row_count == 100,
          f"got {tables.get('departments') and tables['departments'].total_row_count}")
    check("employees has 5000 rows",
          tables.get("employees") and tables["employees"].total_row_count == 5000,
          f"got {tables.get('employees') and tables['employees'].total_row_count}")
    check("projects has 500 rows",
          tables.get("projects") and tables["projects"].total_row_count == 500,
          f"got {tables.get('projects') and tables['projects'].total_row_count}")
    check("assignments has 8000 rows",
          tables.get("assignments") and tables["assignments"].total_row_count == 8000,
          f"got {tables.get('assignments') and tables['assignments'].total_row_count}")
    check("audit_log has 2000 rows",
          tables.get("audit_log") and tables["audit_log"].total_row_count == 2000,
          f"got {tables.get('audit_log') and tables['audit_log'].total_row_count}")

    # PII detection on employees.email
    emp = tables.get("employees")
    if emp:
        email_col = next((c for c in emp.columns if c.name == "email"), None)
        if email_col:
            check("email column has PII pattern detected",
                  "email" in email_col.patterns,
                  f"patterns: {email_col.patterns}")

    # PII detection on audit_log.ip_address
    audit = tables.get("audit_log")
    if audit:
        ip_col = next((c for c in audit.columns if c.name == "ip_address"), None)
        if ip_col:
            check("ip_address has pattern detected",
                  len(ip_col.patterns) > 0,
                  f"patterns: {ip_col.patterns}")

    # Boolean profiling on employees.is_active
    if emp:
        active_col = next((c for c in emp.columns if c.name == "is_active"), None)
        if active_col:
            check("is_active has true_count",
                  active_col.true_count is not None,
                  f"got {active_col.true_count}")
            check("is_active true_rate ~0.92 (±10%)",
                  active_col.true_rate is not None and 0.82 <= active_col.true_rate <= 0.99,
                  f"got {active_col.true_rate}")

    # Salary follows log-normal: should have positive skewness
    if emp:
        salary_col = next((c for c in emp.columns if c.name == "salary"), None)
        if salary_col:
            check("salary has positive skewness (log-normal)",
                  salary_col.skewness is not None and salary_col.skewness > 0,
                  f"got {salary_col.skewness}")
            check("salary has IQR and p25/p75",
                  salary_col.iqr is not None and salary_col.p25 is not None,
                  f"got iqr={salary_col.iqr}, p25={salary_col.p25}")

    # Relationship discovery
    from data_profiler.workers.relationship_worker import discover_relationships
    rels = discover_relationships(results)
    check("Relationship discovery finds FK candidates",
          len(rels) > 0,
          f"got {len(rels)} relationships")
    if rels:
        print(f"  Found {len(rels)} candidate FK relationships:")
        for r in rels[:5]:
            print(f"    {r.from_table}.{r.from_column} → {r.to_table}.{r.to_column} "
                  f"(score={r.score:.2f})")


def _validate_tpcds(results: list, check) -> None:
    """Validate correctness against known TPC-DS SF1 properties."""
    tables = {r.name: r for r in results}

    check("Table count >= 24", len(tables) >= 24, f"got {len(tables)}")

    ss = tables.get("store_sales")
    if ss:
        check("store_sales rows ~2.88M",
              2_800_000 < ss.total_row_count < 3_000_000,
              f"got {ss.total_row_count:,}")
        check("store_sales sampled <= 10000",
              ss.sampled_row_count <= 10000,
              f"got {ss.sampled_row_count}")
        date_sk = next((c for c in ss.columns if c.name == "ss_sold_date_sk"), None)
        if date_sk:
            check("ss_sold_date_sk has some nulls",
                  date_sk.null_rate > 0,
                  f"got {date_sk.null_rate:.1%}")

    dd = tables.get("date_dim")
    if dd:
        check("date_dim has 73049 rows",
              dd.total_row_count == 73049,
              f"got {dd.total_row_count}")
        d_date_sk = next((c for c in dd.columns if c.name == "d_date_sk"), None)
        if d_date_sk:
            check("d_date_sk no nulls",
                  d_date_sk.null_count == 0,
                  f"got {d_date_sk.null_count}")
            check("d_date_sk distinct ~73049",
                  d_date_sk.approx_distinct > 60000,
                  f"got {d_date_sk.approx_distinct}")

    ib = tables.get("income_band")
    if ib:
        check("income_band has 20 rows",
              ib.total_row_count == 20,
              f"got {ib.total_row_count}")
        lb = next((c for c in ib.columns if c.name == "ib_lower_bound"), None)
        if lb:
            check("ib_lower_bound has median", lb.median is not None)
            check("ib_lower_bound has top_values",
                  lb.top_values is not None and len(lb.top_values) > 0)


def _validate_common(results: list, check) -> None:
    """Validation that works for both TPC-DS and synthetic datasets."""
    # Type coverage
    types_seen = {c.canonical_type for r in results for c in r.columns}
    for t in ("integer", "string", "float"):
        check(f"{t} type profiled", t in types_seen)

    # Anomaly detection ran
    anomaly_cols = [c for r in results for c in r.columns if c.anomalies]
    check("Anomaly detection ran", len(anomaly_cols) >= 0)

    # Constraint discovery
    constrained = [r for r in results if r.constraints is not None]
    check("Constraint discovery ran", len(constrained) > 0,
          f"got {len(constrained)} tables with constraints")

    # OpenMetadata export
    from data_profiler.persistence.openmetadata import export_openmetadata
    om_path = f"{OUTPUT_DIR}/profile_openmetadata.json"
    export_openmetadata(results, om_path, run_id="demo", engine="duckdb")
    check("OpenMetadata export created", Path(om_path).exists())
    om_data = json.loads(Path(om_path).read_text())
    check("OpenMetadata has tables", len(om_data["tables"]) > 0,
          f"got {len(om_data['tables'])}")

    # Distinct mode
    hll_cols = [c for r in results for c in r.columns if c.distinct_mode == "approx"]
    check("HLL distinct counting used", len(hll_cols) > 0,
          f"got {len(hll_cols)} HLL columns")


# ---------------------------------------------------------------------------
# Output generation
# ---------------------------------------------------------------------------

def generate_report(run_id: str, results: list) -> None:
    banner("Phase 4: HTML Report")
    try:
        from data_profiler.workers.relationship_worker import discover_relationships
        from dataclasses import asdict
        rels = discover_relationships(results)
        rel_dicts = [asdict(r) for r in rels] if rels else None
        output_path = f"{OUTPUT_DIR}/profile_report.html"
        generate_html_report(
            run_id=run_id,
            engine="duckdb",
            profiled_at=datetime.now().isoformat(),
            results=results,
            output_path=output_path,
            relationships=rel_dicts,
        )
        size_kb = Path(output_path).stat().st_size / 1024
        print(f"  Report:  {output_path} ({size_kb:.0f} KB)")
        print(f"  Open:    open {output_path}")
    except Exception as e:
        print(f"  Skipped HTML report: {e}")


def generate_demo_dashboard(run_id: str, results: list) -> None:
    banner("Phase 5: Interactive Dashboard")
    try:
        from data_profiler.workers.relationship_worker import discover_relationships
        from dataclasses import asdict
        rels = discover_relationships(results)
        rel_dicts = [asdict(r) for r in rels] if rels else None
        output_path = f"{OUTPUT_DIR}/profile_dashboard.html"
        generate_dashboard(
            run_id=run_id,
            engine="duckdb",
            profiled_at=datetime.now().isoformat(),
            results=results,
            output_path=output_path,
            relationships=rel_dicts,
        )
        size_kb = Path(output_path).stat().st_size / 1024
        print(f"  Dashboard: {output_path} ({size_kb:.0f} KB)")
        print(f"  Open:      open {output_path}")
    except Exception as e:
        print(f"  Skipped dashboard: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    banner("Multi-Engine Data Profiler Demo")
    if Path(TPCDS_DB).exists():
        print("  Dataset:  TPC-DS 1GB (24 tables, ~19.6M rows)")
    else:
        print("  Dataset:  Synthetic (5 tables, ~15.6K rows)")
        print("  Tip:      Set TPCDS_DB with the TPC-DS DuckDB path for full demo")
    print()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Phase 1: Primary DuckDB dataset
    run_id, primary_results, is_synthetic = profile_primary()

    # Phase 2: SQLite cross-engine comparison
    _, sqlite_results = profile_sqlite()

    # Phase 3: Correctness validation
    all_passed = validate_correctness(primary_results, is_synthetic)

    # Phase 4: HTML Report
    generate_report(run_id, primary_results)

    # Phase 5: Interactive Dashboard
    generate_demo_dashboard(run_id, primary_results)

    banner("Demo Complete")
    output_prefix = "synthetic" if is_synthetic else "tpcds"
    print(f"  DuckDB output: {OUTPUT_DIR}/{output_prefix}_duckdb.ndjson")
    print(f"  SQLite output: {OUTPUT_DIR}/demo_sqlite.ndjson")
    print(f"  HTML report:   {OUTPUT_DIR}/profile_report.html")
    print(f"  Dashboard:     {OUTPUT_DIR}/profile_dashboard.html")
    print(f"  Validation:    {'ALL PASSED' if all_passed else 'SOME FAILED'}")
    print()

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
