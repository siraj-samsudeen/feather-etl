# feather-etl: Product Requirements Document

**Version:** 1.2
**Date:** 2026-03-26
**Status:** Draft

---

## 1. Product Overview

### What

feather-etl is a lightweight, config-driven Python ETL **package** for extracting data from multiple source types, transforming it with plain SQL in local DuckDB, and optionally syncing final tables to a remote destination.

The core package works entirely with file-based sources (DuckDB, SQLite, CSV, Excel, JSON) and local DuckDB as the destination — fully testable without any external servers. Client projects extend it with database sources (SQL Server) and cloud destinations (MotherDuck).

### What It Replaces

A stack of three heavyweight frameworks:

| Current Tool | What It Does | feather-etl Equivalent |
|-------------|-------------|----------------------|
| Python dlt | Extract from sources | `extract.py` — multi-source extraction |
| SQLMesh | Bronze → Silver → Gold SQL transforms | `.sql` files + `load.py` |
| Dagster | Orchestration and scheduling | `scheduler.py` + `cli.py` |

### Why

The current stack introduces a "complexity tax": hundreds of transitive dependencies, proprietary paradigms, fragmented configuration, and a steep learning curve. feather-etl replaces all three with a single Python package (~1000 LOC core) using standard Python and SQL patterns.

### Design Philosophy

- **Package-first.** feather-etl is a reusable Python package, not a standalone application. Client projects import and configure it.
- **File-first.** Core functionality works with file-based sources (CSV, DuckDB, SQLite, Excel, JSON). No external servers needed for development or testing.
- **Config-driven.** YAML defines what to extract, how to transform, when to schedule. Adding a table means editing YAML, not writing Python.
- **Local-first.** Extract and transform locally in DuckDB (free compute). Push only final gold tables to remote destinations (minimizes cloud cost).
- **Testable.** The entire pipeline is end-to-end testable using file-based sources and local DuckDB — no mocking required for core functionality.

---

## 2. Users and Use Cases

### Primary Users

1. **Package developer** — builds and tests feather-etl itself, using file-based sources
2. **Data platform operator** — uses feather-etl in a client project, configuring SQL Server sources and MotherDuck destinations

### Use Cases

**UC1: Package Development and Testing**
Developer writes tests using CSV/DuckDB files as sources. Runs the full extract → load → transform pipeline locally. Verifies DQ checks, state tracking, and schema drift detection — all without connecting to SQL Server or MotherDuck.

**UC2: Initial Client Setup**
Operator creates a client project that depends on feather-etl. Configures `feather.yaml` with SQL Server source, table definitions, schedules, and transform SQL. Runs `feather setup` then `feather run` for initial load.

**UC3: Scheduled Extraction**
feather-etl runs on a schedule (APScheduler or OS cron). Hot tables sync hourly/twice-daily via timestamp watermarks. Cold tables check for changes daily/weekly and only refresh if data changed.

**UC4: Manual Run**
Operator triggers extraction for a specific table or tier: `feather run --table sales_invoice` or `feather run --tier hot`.

**UC5: Debugging a Data Issue**
Operator checks run history (`feather history`), sees a DQ failure, queries `_runs` and `_dq_results` in the state database, traces the issue to source data.

**UC6: Adding a New Table**
Operator adds a table entry to `feather.yaml`, optionally adds silver/gold SQL. Runs `feather setup` then `feather run --table new_table`.

**UC7: Schema Change Detection**
Source system changes columns. feather-etl detects drift via schema snapshot comparison, logs it, sends a Slack alert.

---

## 3. Functional Requirements

### FR1: Source Abstraction

**FR1.1** The system shall support multiple source types, configured per-table:

| Source Type | Reader | Change Detection | Incremental Support |
|------------|--------|-----------------|-------------------|
| `duckdb` | DuckDB ATTACH | File mtime + file hash | Yes (if timestamp column exists) |
| `sqlite` | DuckDB `sqlite_scan()` | File mtime + file hash | Yes (if timestamp column exists) |
| `csv` | DuckDB `read_csv()` | File mtime + file hash | No (full refresh only) |
| `excel` | DuckDB `st_read()` | File mtime + file hash | No (full refresh only) |
| `json` | DuckDB `read_json()` | File mtime + file hash | No (full refresh only) |
| `sqlserver` | pyodbc → PyArrow | CHECKSUM_AGG + COUNT(*) | Yes (timestamp watermark) |

**FR1.2** File-based sources shall use **dual change detection**:
1. Check file modification time (`os.path.getmtime()`) — cheap, skip if unchanged
2. If mtime changed, compute file hash (`hashlib.md5` of file contents) and compare to stored hash — definitive
3. If hash unchanged (file was touched but content identical): skip extraction
4. If hash changed: extract (full refresh or incremental depending on strategy)

**FR1.3** Database sources (SQL Server) shall use **CHECKSUM_AGG + COUNT(*)** for cold tables and **timestamp watermarks** for hot tables (unchanged from v1.0).

**FR1.4** All source types shall return data as PyArrow Tables to the loader. The source type determines HOW data is read, but the loader always receives the same format.

**FR1.5** File-based sources shall read data via DuckDB's native readers (executed on a temporary DuckDB connection), then export to PyArrow:
```python
# CSV example
temp_con.execute("SELECT * FROM read_csv(?)", [file_path])
arrow_table = temp_con.fetcharrow()
```

**FR1.6** For file-based sources with `strategy: incremental`, the system shall apply the same timestamp watermark logic as SQL Server: `WHERE {timestamp_column} >= {effective_watermark} ORDER BY {timestamp_column}`.

### FR2: Configuration

**FR2.1** The system shall be configured via a single YAML file (`feather.yaml`).

**FR2.2** The config shall support environment variable substitution using `${VAR_NAME}` syntax, resolved at load time via `os.path.expandvars()`.

**FR2.3** Source configuration shall specify connection details per source type:
```yaml
source:
  type: csv                          # file-based (for testing)
  path: ./test_data/                 # directory containing source files

  # OR

  type: duckdb
  path: ./test_data/source.duckdb

  # OR

  type: sqlserver                    # database (for production)
  connection_string: "${SQL_SERVER_CONNECTION_STRING}"
```

**FR2.4** Destination configuration:
```yaml
destination:
  path: ./feather_data.duckdb        # local DuckDB (always)

sync:                                 # optional remote sync
  type: motherduck
  token: "${MOTHERDUCK_TOKEN}"
  database: "client_analytics"
```

**FR2.5** Each table shall be independently configurable with:
- `name` — logical name (used in CLI, state, logs)
- `source_table` — source table name or filename (e.g., `dbo.SALESINVOICE` or `customers.csv`)
- `target_table` — local DuckDB target (e.g., `bronze.sales_invoice`)
- `strategy` — `incremental` or `full` (replaces `checksum` as the generic term; checksum is an implementation detail of SQL Server change detection)
- `schedule` — human-readable schedule name or tier shortcut
- `primary_key` — list of primary key columns
- `timestamp_column` — (required for incremental) column used for watermarking
- `checksum_columns` — (optional, sqlserver only) explicit column list for BINARY_CHECKSUM
- `filter` — (optional) SQL WHERE clause applied at extraction
- `quality_checks` — (optional) declarative DQ checks: `not_null`, `unique`
- `column_map` — (optional) source→target column rename mapping

**FR2.6** Schedules shall use human-readable names: `hourly`, `every 2 hours`, `twice daily`, `daily`, `weekly`.

**FR2.7** Schedule tiers shall be supported as shortcuts:
```yaml
schedule_tiers:
  hot: "twice daily"
  cold: "weekly"
```

**FR2.8** Global defaults shall be configurable:
- `overlap_window_minutes` — lookback window for incremental extraction (default: 2)
- `batch_size` — rows per fetch batch (default: 120,000)

**FR2.9** Config validation shall enforce:
- `strategy: incremental` requires `timestamp_column`
- `schedule` must resolve to a known schedule name
- `primary_key` is required for all tables
- `source.type` must be a recognized source type
- File-based source types require `source.path` to exist

### FR3: Extraction

**FR3.1** The extractor shall support two extraction paths:

**File-based extraction** (DuckDB, SQLite, CSV, Excel, JSON):
- Uses DuckDB's native readers to load file contents
- Returns PyArrow Table
- Change detection via file mtime + hash (stored in `_watermarks`)

**Database extraction** (SQL Server):
- Uses pyodbc with `SET NOCOUNT ON`
- Chunked fetching (`cursor.fetchmany(batch_size)`) → PyArrow
- Change detection via CHECKSUM_AGG + COUNT(*) for full-refresh tables
- Timestamp watermark for incremental tables

**FR3.2** For **incremental** tables (any source type):
- Filter: `WHERE {timestamp_column} >= {effective_watermark} [AND {filter}] ORDER BY {timestamp_column}`
- Effective watermark = stored watermark minus `overlap_window_minutes`

**FR3.3** For **full** strategy tables:
- File sources: check mtime → check hash → skip if unchanged, full extract if changed
- SQL Server: check CHECKSUM_AGG + COUNT(*) → skip if unchanged, full extract if changed

**FR3.4** The extractor shall support extracting source schema metadata:
- File sources: infer from DuckDB's column metadata after reading
- SQL Server: query `INFORMATION_SCHEMA.COLUMNS`

**FR3.5** The extractor shall be stateless — it receives parameters and returns a PyArrow Table. All state management is external.

### FR4: Loading

**FR4.1** Data shall be loaded into a local DuckDB file (configured in `destination.path`).

**FR4.2** The local DuckDB shall have schemas: `bronze`, `silver`, `gold`, `_quarantine`.

**FR4.3** For **full** (full refresh) tables, use the **swap pattern**:
1. `CREATE TABLE {target}_new AS SELECT * FROM staging_data`
2. `DROP TABLE IF EXISTS {target}`
3. `ALTER TABLE {target}_new RENAME TO {final_name}`
All within a single transaction.

**FR4.4** For **incremental** tables, use **partition-based overwrite**:
1. `DELETE FROM {target} WHERE {timestamp_column} >= {min_timestamp_in_batch}`
2. `INSERT INTO {target} SELECT *, current_timestamp AS _etl_loaded_at, {run_id} AS _etl_run_id FROM staging_data`

**FR4.5** Every bronze row shall include two metadata columns:
- `_etl_loaded_at` (TIMESTAMP) — when the row was loaded
- `_etl_run_id` (VARCHAR) — links to the `_runs` table

**FR4.6** If a `column_map` is configured, renaming is applied at the PyArrow level (zero-copy) before loading.

**FR4.7** Loading and state update shall be wrapped in a single DuckDB transaction.

### FR5: Transforms

**FR5.1** Transforms shall be plain `.sql` files stored in `transforms/silver/` and `transforms/gold/`.

**FR5.2** Silver transforms shall be DuckDB **views** (`CREATE OR REPLACE VIEW`).

**FR5.3** Gold transforms shall be DuckDB **materialized tables** (`CREATE OR REPLACE TABLE ... AS SELECT ...`), rebuilt after each extraction run.

**FR5.4** SQL files may use `string.Template` variable substitution (`${variable}`).

**FR5.5** Transform execution order shall be determined by `graphlib.TopologicalSorter` (stdlib) based on declared dependencies.

**FR5.6** `feather setup` shall execute all transform SQL files.

**FR5.7** After each extraction run, gold tables shall be rebuilt.

### FR6: Remote Sync (Optional)

**FR6.1** Remote sync shall be optional, configured in the `sync` section of YAML. If not configured, the pipeline stops at local DuckDB.

**FR6.2** Only **gold** tables shall be synced to the remote destination.

**FR6.3** MotherDuck sync shall use DuckDB ATTACH: `ATTACH 'md:{database}?motherduck_token={token}'`.

**FR6.4** For each gold table: `CREATE OR REPLACE TABLE md.{database}.gold.{table} AS SELECT * FROM local.gold.{table}`.

**FR6.5** A single remote connection shall be reused across all gold table syncs.

**FR6.6** The sync step shall be the last step in the pipeline, after gold tables are rebuilt.

### FR7: State Management

**FR7.1** State shall be stored in a local DuckDB file (`feather_state.duckdb`), separate from the data DuckDB.

**FR7.2** The state database shall contain these tables:

**`_watermarks`** — per-table extraction state:

| Column | Type | Purpose |
|--------|------|---------|
| table_name | VARCHAR PK | Table identifier |
| strategy | VARCHAR | incremental or full |
| last_value | VARCHAR | Last watermark (ISO timestamp) or NULL |
| last_checksum | INTEGER | Last CHECKSUM_AGG value (sqlserver) or NULL |
| last_row_count | INTEGER | Last COUNT(*) or NULL |
| last_file_mtime | DOUBLE | Last file modification time (file sources) or NULL |
| last_file_hash | VARCHAR | Last file content hash (file sources) or NULL |
| last_run_at | TIMESTAMP | When last successfully run |
| retry_count | INTEGER | Consecutive failure count |
| retry_after | TIMESTAMP | Skip until this time (backoff) |
| boundary_hashes | JSON | PK hashes at watermark boundary |

**`_runs`** — per-table run history:

| Column | Type | Purpose |
|--------|------|---------|
| run_id | VARCHAR PK | `{table_name}_{iso_timestamp}` |
| table_name | VARCHAR | Table identifier |
| started_at | TIMESTAMP | Run start |
| ended_at | TIMESTAMP | Run end |
| duration_sec | DOUBLE | Duration |
| status | VARCHAR | success, failure, skipped |
| rows_extracted | INTEGER | Rows read from source |
| rows_loaded | INTEGER | Rows written to DuckDB |
| rows_skipped | INTEGER | Rows filtered/deduplicated |
| error_message | VARCHAR | NULL on success |
| watermark_before | VARCHAR | Watermark at start |
| watermark_after | VARCHAR | Watermark at end |
| freshness_max_ts | TIMESTAMP | MAX(timestamp_column) from loaded data |
| schema_changes | JSON | Detected drift, if any |

**`_run_steps`** — granular per-step logging:

| Column | Type | Purpose |
|--------|------|---------|
| run_id | VARCHAR | FK to _runs |
| step | VARCHAR | extract, load, quality, schema, sync |
| message | VARCHAR | Step details |
| started_at | TIMESTAMP | Step start |
| ended_at | TIMESTAMP | Step end |

**`_dq_results`** — data quality check results:

| Column | Type | Purpose |
|--------|------|---------|
| run_id | VARCHAR | FK to _runs |
| table_name | VARCHAR | Table checked |
| check_type | VARCHAR | not_null, unique, freshness, row_count |
| column_name | VARCHAR | Column checked (NULL for table-level) |
| result | VARCHAR | pass, fail, warn |
| details | VARCHAR | Description of finding |
| checked_at | TIMESTAMP | When checked |

**`_schema_snapshots`** — source schema for drift detection:

| Column | Type | Purpose |
|--------|------|---------|
| table_name | VARCHAR | PK (with column_name) |
| column_name | VARCHAR | PK (with table_name) |
| data_type | VARCHAR | Source data type |
| snapshot_at | TIMESTAMP | When snapshot taken |

**FR7.3** Watermarks shall only be updated after successful load (and sync, if configured). On failure, the watermark stays unchanged.

**FR7.4** Boundary deduplication: for incremental tables, hash PKs of rows at the max watermark timestamp. Store in `boundary_hashes`. On next run, skip rows whose PK hash matches.

### FR8: Data Quality

**FR8.1** DQ checks shall be declarative, configured per-table in YAML:
```yaml
quality_checks:
  not_null: [invoice_no, customer_code]
  unique: [invoice_no]
```

**FR8.2** Supported check types:
- `not_null` — fail if any NULLs in specified columns
- `unique` — fail if any duplicate values in specified columns
- `row_count` — warn if 0 rows (always runs)

**FR8.3** DQ checks run after each load, against the local DuckDB table.

**FR8.4** DQ failures are logged to `_dq_results` and trigger alerts, but do NOT block the pipeline.

### FR9: Schema Drift Detection

**FR9.1** On each extraction, compare source schema against stored snapshot in `_schema_snapshots`.

**FR9.2** Detect: `added` (new column), `removed` (column gone), `type_changed` (data type changed).

**FR9.3** Schema changes logged in `_runs` and trigger alerts.

**FR9.4** First run saves baseline — no drift reported.

**FR9.5** For file sources, schema is inferred from the file contents via DuckDB. For SQL Server, schema comes from `INFORMATION_SCHEMA.COLUMNS`.

### FR10: Scheduling

**FR10.1** Two scheduling modes:
- **Built-in:** APScheduler v3.x with SQLite job store, `feather schedule`
- **External:** One-shot CLI (`feather run --tier hot`) for OS cron

**FR10.2** APScheduler: `coalesce=True`, `max_instances=1`, `replace_existing=True`.

**FR10.3** Schedule resolution: table schedule → tier lookup → named schedule → cron kwargs.

### FR11: CLI

**FR11.1** CLI built with typer, entry point: `feather`.

**FR11.2** Commands:

| Command | Purpose |
|---------|---------|
| `feather setup` | Init state DB, create schemas, apply transforms |
| `feather run` | Run all tables |
| `feather run --table X` | Run single table |
| `feather run --tier hot` | Run tables matching a tier |
| `feather status` | Show watermarks and last run status |
| `feather history [--table X]` | Show run history |
| `feather schedule` | Start APScheduler daemon |

**FR11.3** All commands accept `--config PATH` (default: `feather.yaml`).

### FR12: Alerting

**FR12.1** Alerts via Slack incoming webhook (`requests.post()`).

**FR12.2** Severities: critical (failures), warning (DQ issues), info (schema drift).

**FR12.3** No-op if webhook not configured.

### FR13: Retry and Error Handling

**FR13.1** On failure: record run, increment retry_count, set retry_after (linear backoff), do NOT update watermark, send alert.

**FR13.2** Skip table if `current_time < retry_after`.

**FR13.3** Reset retry_count on success.

**FR13.4** Individual table failures do not prevent other tables from running.

---

## 4. Non-Functional Requirements

**NFR1: Dependencies.** All 7 runtime dependencies ship with the package:

| Package | Purpose |
|---------|---------|
| duckdb | Local processing, file readers, MotherDuck sync |
| pyarrow | Zero-copy data interchange |
| pyyaml | Config parsing |
| typer | CLI |
| pyodbc | SQL Server extraction |
| apscheduler | Built-in scheduling |
| requests | Slack alerting |

The package is complete out of the box — all source types, scheduling, and alerting are included. File-based sources are a testing strategy, not a reason to split dependencies.

**NFR2: Code Size.** Core package under 1,200 lines of Python (increased from 1,000 to accommodate source abstraction).

**NFR3: Performance.** Full extraction of ~700K rows within 5 minutes. Incremental extraction of ~1,000 rows within 10 seconds. File-based extraction within seconds regardless of size (DuckDB native readers are fast).

**NFR4: Project Tooling.** Use `uv` for dependency management.

**NFR5: Python Version.** Python 3.10+.

**NFR6: Testing.** End-to-end tests use file-based sources (CSV, DuckDB files) → local DuckDB. No mocking needed for core pipeline tests. Only SQL Server and MotherDuck connectivity require mocking. Target: 80% coverage.

**NFR7: Logging.** Python stdlib `logging` — console (human-readable) + JSONL file (structured, queryable).

---

## 5. Connector Interface Design

### Design Decision: Reimplement dlt Patterns, Not Reuse dlt

**Decision:** Option B — study dlt's patterns (Incremental, sql_table, state management) and reimplement the ~300 lines we need. Not a fork; a clean reimplementation of proven logic.

**Rationale:**
- dlt pulls in SQLAlchemy, jsonpath-ng, pendulum, and ~30 transitive deps — defeats our 7-dependency goal
- dlt's internal modules are deeply coupled — can't import `Incremental` without the rest
- The extraction logic we need is genuinely simple: pyodbc + fetchmany + Arrow (~100 lines), watermark tracking (~80 lines), file change detection (~50 lines)
- File sources don't benefit from dlt at all — dlt doesn't have native CSV/DuckDB file sources
- Our Protocol-based interface is dlt-compatible — if we ever want to wrap a dlt source as an adapter, the door is open

**Patterns borrowed from dlt:**
- `Incremental.last_value` + `last_value_func(max)` for watermark tracking
- Boundary dedup via `unique_hashes` of PK values at cursor boundary
- `lag` / overlap window concept (subtract N minutes from watermark at query time, never store the adjusted value)
- State committed atomically with data

**Patterns borrowed from Singer SDK:**
- Specialization hierarchy: base Source → FileSource → DatabaseSource (reduces boilerplate)
- `discover()` as first-class concept

**Patterns borrowed from ingestr:**
- Python `Protocol` for structural typing (no forced inheritance)
- Minimal interface: a connector is just a class with the right methods

### Source Protocol

```python
from typing import Protocol, Iterator

@dataclass
class StreamSchema:
    """Describes a discoverable table/stream."""
    name: str
    columns: list[tuple[str, str]]  # [(column_name, data_type), ...]
    primary_key: list[str] | None
    supports_incremental: bool

@dataclass
class ChangeResult:
    """Result of change detection check."""
    changed: bool
    reason: str  # "mtime_changed", "hash_changed", "checksum_changed", "first_run", "unchanged"
    metadata: dict  # source-specific (file_hash, checksum, row_count, etc.)

class Source(Protocol):
    """Any class with these methods is a valid feather-etl source.
    No inheritance required — structural typing via Protocol."""

    def check(self) -> bool:
        """Verify connectivity/access. Return True if OK."""
        ...

    def discover(self) -> list[StreamSchema]:
        """List available tables/streams with schema info."""
        ...

    def extract(self, table: str,
                columns: list[str] | None = None,
                filter: str | None = None,
                watermark_column: str | None = None,
                watermark_value: str | None = None) -> pa.Table:
        """Extract data as PyArrow Table.
        For incremental: applies WHERE watermark_column >= watermark_value.
        For full: extracts all rows (optionally filtered)."""
        ...

    def detect_changes(self, table: str,
                       last_state: dict | None = None) -> ChangeResult:
        """Has this table/file changed since last check?
        last_state contains source-specific metadata from previous run
        (file_hash, file_mtime, checksum, row_count, etc.)."""
        ...

    def get_schema(self, table: str) -> list[tuple[str, str]]:
        """Column names + types for drift detection."""
        ...
```

### Destination Protocol

```python
class Destination(Protocol):
    """Target for loaded data."""

    def load_full(self, table: str, data: pa.Table, run_id: str) -> int:
        """Full refresh via swap pattern. Return rows loaded."""
        ...

    def load_incremental(self, table: str, data: pa.Table, run_id: str,
                         timestamp_column: str) -> int:
        """Partition overwrite for incremental. Return rows loaded."""
        ...

    def execute_sql(self, sql: str) -> None:
        """Execute SQL (for transforms)."""
        ...

    def sync_to_remote(self, tables: list[str], remote_config: dict) -> None:
        """Push tables to remote destination. No-op if not configured."""
        ...
```

### Concrete Base Classes

These are optional — a connector can implement `Source` Protocol directly for custom sources (e.g., REST APIs). But for common patterns, base classes eliminate boilerplate.

```python
class FileSource:
    """Base for file-based sources (CSV, Excel, JSON, DuckDB, SQLite).

    Subclasses provide:
    - reader_function: DuckDB SQL function name (read_csv, read_json, etc.)
    - file_extension: expected file extension

    Base class handles:
    - detect_changes() via file mtime + MD5 hash
    - extract() via DuckDB native reader → PyArrow
    - discover() via DuckDB schema inference
    - get_schema() via DuckDB column metadata
    - Incremental filtering (WHERE clause on DuckDB reader output)
    """

class DatabaseSource:
    """Base for database sources (SQL Server, PostgreSQL, etc.).

    Subclasses provide:
    - connect(): return a DB-API 2.0 connection
    - get_checksum_query(): return SQL for table checksum (source-specific)
    - get_schema_query(): return SQL for schema metadata (source-specific)

    Base class handles:
    - detect_changes() via checksum + row count comparison
    - extract() via cursor.fetchmany() → PyArrow
    - discover() via schema query
    - get_schema() via schema query
    - SET NOCOUNT ON, chunked fetching, ORDER BY for incremental
    """
```

### Concrete Implementations

| Class | Extends | Lines | What It Provides |
|-------|---------|-------|-----------------|
| `CsvSource` | `FileSource` | ~20 | Sets `reader_function = "read_csv"` |
| `JsonSource` | `FileSource` | ~20 | Sets `reader_function = "read_json"` |
| `DuckDBFileSource` | `FileSource` | ~30 | ATTACH + SELECT (not a reader function) |
| `SqliteSource` | `FileSource` | ~25 | Sets `reader_function = "sqlite_scan"` |
| `ExcelSource` | `FileSource` | ~25 | Sets `reader_function = "st_read"` |
| `SqlServerSource` | `DatabaseSource` | ~80 | pyodbc connect, CHECKSUM_AGG query, INFORMATION_SCHEMA |
| `DuckDBDestination` | (implements Destination) | ~100 | Swap + partition overwrite + ATTACH for MotherDuck |

### Source Registry

Sources are resolved from config by type name:

```python
SOURCE_REGISTRY = {
    "csv": CsvSource,
    "json": JsonSource,
    "duckdb": DuckDBFileSource,
    "sqlite": SqliteSource,
    "excel": ExcelSource,
    "sqlserver": SqlServerSource,
}

def create_source(config: SourceConfig) -> Source:
    """Factory: resolve source type → instantiate."""
    cls = SOURCE_REGISTRY[config.type]
    return cls(config)
```

New source types are added by implementing the `Source` Protocol (or extending `FileSource`/`DatabaseSource`) and registering in the registry.

---

## 6. Architecture

### Data Flow

```
Sources
├── File-based (testing/development)
│   ├── CSV files ──────────────┐
│   ├── Excel files ────────────┤
│   ├── JSON files ─────────────┤ DuckDB native readers
│   ├── SQLite databases ───────┤ → PyArrow Table
│   └── DuckDB files ──────────┘
│
├── Database (production)
│   └── SQL Server ── pyodbc ── → PyArrow Table
│
▼
PyArrow Table (common format from all sources)
│
▼
Local DuckDB: feather_data.duckdb
├── bronze schema (raw data + _etl_loaded_at + _etl_run_id)
├── silver views (column renames, filters) — LOCAL ONLY
├── gold materialized tables (JOINs, denormalized)
└── _quarantine schema (bad rows)

Local DuckDB: feather_state.duckdb
├── _watermarks (with file mtime/hash for file sources)
├── _runs, _run_steps, _dq_results, _schema_snapshots

Optional: Remote Sync
└── MotherDuck (via ATTACH — gold tables only)
    └── Rill Data (dashboards)
```

### Module Breakdown

```
src/feather/
├── __init__.py                 ~10 LOC   Version, top-level exports
├── config.py                  ~130 LOC   YAML parsing, validation, dataclasses
├── sources/
│   ├── __init__.py             ~10 LOC   Source Protocol, ChangeResult, StreamSchema
│   ├── base.py                ~120 LOC   FileSource + DatabaseSource base classes
│   ├── csv.py                  ~20 LOC   CsvSource
│   ├── json.py                 ~20 LOC   JsonSource
│   ├── duckdb_file.py          ~30 LOC   DuckDBFileSource
│   ├── sqlite.py               ~25 LOC   SqliteSource
│   ├── excel.py                ~25 LOC   ExcelSource
│   ├── sqlserver.py            ~80 LOC   SqlServerSource (pyodbc + CHECKSUM_AGG)
│   └── registry.py             ~20 LOC   SOURCE_REGISTRY + create_source()
├── destinations/
│   ├── __init__.py             ~10 LOC   Destination Protocol
│   └── duckdb.py              ~100 LOC   DuckDBDestination (swap + partition + sync)
├── state.py                   ~110 LOC   Watermarks, run history, DQ results
├── quality.py                  ~80 LOC   DQ checks (not_null, unique, row_count)
├── schema.py                   ~60 LOC   Schema drift detection + snapshots
├── alerts.py                   ~40 LOC   Slack webhook
├── pipeline.py                ~150 LOC   run_table() orchestrator
├── scheduler.py                ~80 LOC   APScheduler + presets
└── cli.py                      ~80 LOC   typer CLI
                               --------
                              ~1200 LOC
```

### Core Pipeline Flow (`run_table`)

```
1. Check retry backoff → skip if in backoff window
2. Read watermark from state
3. Check for changes (source-type-specific):
   ├── File sources: mtime check → hash check → skip if unchanged
   └── SQL Server (full strategy): CHECKSUM_AGG + COUNT(*) → skip if unchanged
4. Extract data (source-type-specific):
   ├── File sources: DuckDB native reader → PyArrow
   └── SQL Server: pyodbc → chunked fetch → PyArrow
   For incremental: apply watermark filter + overlap window
5. Apply column_map (if configured)
6. Load to local DuckDB (swap or partition overwrite)
7. Run DQ checks → log results
8. Detect schema drift → log + alert
9. Rebuild gold tables (rematerialize)
10. Sync gold to remote (if configured)
11. Update state (watermark + run record) in single transaction
12. Alert on issues
13. Return RunResult
```

### Example Config: File-Based Testing

```yaml
source:
  type: csv
  path: ./test_data/

destination:
  path: ./test_output.duckdb

state:
  path: ./test_state.duckdb

schedule_tiers:
  hot: daily
  cold: weekly

tables:
  - name: sales_invoice
    source_table: sales_invoice.csv
    target_table: bronze.sales_invoice
    strategy: incremental
    timestamp_column: modified_date
    schedule: hot
    primary_key: [invoice_id]
    quality_checks:
      not_null: [invoice_id, customer_code]

  - name: customer_master
    source_table: customers.csv
    target_table: bronze.customer_master
    strategy: full
    schedule: cold
    primary_key: [customer_code]
    column_map:
      CustomerCode: customer_code
      CustomerName: customer_name
```

### Example Config: Production (SQL Server + MotherDuck)

```yaml
source:
  type: sqlserver
  connection_string: "${SQL_SERVER_CONNECTION_STRING}"

destination:
  path: ./feather_data.duckdb

sync:
  type: motherduck
  token: "${MOTHERDUCK_TOKEN}"
  database: "client_analytics"

state:
  path: ./feather_state.duckdb

defaults:
  overlap_window_minutes: 2

schedule_tiers:
  hot: "twice daily"
  cold: weekly

alerts:
  slack_webhook: "${SLACK_WEBHOOK_URL}"

tables:
  - name: sales_invoice
    source_table: dbo.SALESINVOICE
    target_table: bronze.sales_invoice
    strategy: incremental
    timestamp_column: ModifiedDate
    schedule: hot
    primary_key: [ID]
    filter: "STATUS <> 1"
    quality_checks:
      not_null: [SI_NO, Custome_Code]

  - name: customer_master
    source_table: dbo.CUSTOMERMASTER
    target_table: bronze.customer_master
    strategy: full
    checksum_columns: [customercode, customername, salestype, city, states]
    schedule: cold
    primary_key: [customercode]
    quality_checks:
      not_null: [customercode, customername]
      unique: [customercode]
```

---

## 6. Feature List for Implementation

Features ordered by dependency. Each feature is a self-contained unit suitable for `/spec` implementation.

### Phase 1: Foundation

| # | Feature | Description | Depends On |
|---|---------|-------------|-----------|
| F1 | Project scaffold | uv project, pyproject.toml, directory structure, all 7 deps | — |
| F2 | Configuration | YAML parsing, env vars, validation, source type handling, typed dataclasses | F1 |
| F3 | State management | State DB init, watermark CRUD (including file mtime/hash fields), run recording | F1 |
| F4 | Source/Destination protocols | Source Protocol, Destination Protocol, ChangeResult, StreamSchema dataclasses, source registry | F1 |
| F5 | FileSource base + CSV source | FileSource base class (mtime+hash detection, DuckDB reader, incremental filter), CsvSource implementation | F2, F4 |
| F6 | Additional file sources | DuckDBFileSource, SqliteSource, JsonSource, ExcelSource | F5 |
| F7 | DatabaseSource base + SQL Server | DatabaseSource base class (cursor→Arrow, checksum detection), SqlServerSource (pyodbc, CHECKSUM_AGG) | F2, F4 |
| F8 | DuckDB destination | DuckDBDestination: schema creation, swap pattern, partition overwrite, metadata columns, column_map | F2, F4 |
| F9 | Core pipeline | `run_table()` wiring source + destination + state. `run_all()`. | F2, F3, F4, F5, F7, F8 |
| F10 | CLI (basic) | `feather setup`, `feather run`, `feather status` | F9 |

### Phase 2: Observability

| # | Feature | Description | Depends On |
|---|---------|-------------|-----------|
| F11 | Data quality checks | Declarative DQ (not_null, unique, row_count), results to `_dq_results` | F9 |
| F12 | Schema drift detection | Schema comparison via source.get_schema(), snapshots, change classification | F3, F5, F7 |
| F13 | Alerting | Slack webhook, alert on failures/DQ/drift | F11, F12 |
| F14 | Run history CLI | `feather history` command with filtering | F3, F10 |

### Phase 3: Transforms and Remote Sync

| # | Feature | Description | Depends On |
|---|---------|-------------|-----------|
| F15 | Silver views | Transform SQL files, `string.Template` substitution, view creation | F8 |
| F16 | Gold materialized tables | Materialized transforms, topological ordering, rebuild after load | F15 |
| F17 | Remote sync (MotherDuck) | Optional ATTACH + push gold tables via destination.sync_to_remote() | F16 |

### Phase 4: Scheduling

| # | Feature | Description | Depends On |
|---|---------|-------------|-----------|
| F18 | Schedule resolution | Human-readable presets, tier shortcuts, CronTrigger mapping | F2 |
| F19 | APScheduler integration | SQLite job store, per-table jobs, coalesce, max_instances | F9, F18 |
| F20 | `feather schedule` command | Daemon mode | F19, F10 |
| F21 | `feather run --tier` | Tier-based one-shot for OS cron | F9, F18 |

### Phase 5: Hardening

| # | Feature | Description | Depends On |
|---|---------|-------------|-----------|
| F22 | Retry with backoff | Failure tracking, linear backoff, auto-reset | F3, F9 |
| F23 | Boundary deduplication | PK hashing at watermark boundary, dedup on next run | F5, F9 |
| F24 | Structured logging | JSONL handler, console handler, table-prefixed messages | F9 |
| F25 | Quarantine | Route DQ-failing rows to `_quarantine` schema | F8, F11 |

---

## 7. Testing Strategy

### Test Layers

**Layer 1: File-based end-to-end tests (no mocking)**

The core pipeline is fully testable with file-based sources — no external servers needed:

```
test_data/
├── sales_invoice.csv       # hot table with timestamps
├── customers.csv           # cold table, full refresh
├── employees.csv           # cold table with column_map
├── source.duckdb           # DuckDB file source
└── source.sqlite           # SQLite file source
```

Tests create a `feather.yaml` pointing to these files, run the full pipeline, and verify:
- Bronze tables populated correctly in output DuckDB
- State updated (watermarks, run history)
- DQ checks executed and results recorded
- Schema drift detected when test files are modified
- Gold tables rebuilt after load
- Second run skips unchanged files (mtime + hash)
- Incremental run picks up only new rows

**Layer 2: SQL Server tests (mocked connectivity)**

SQL Server extraction tests mock `pyodbc.connect()` and cursor behavior to test:
- Query construction (WHERE clause, ORDER BY, SET NOCOUNT ON)
- Chunked Arrow conversion from cursor rows
- CHECKSUM_AGG change detection logic
- Incremental watermark filtering

**Layer 3: MotherDuck sync tests (mocked ATTACH)**

Mock DuckDB ATTACH for MotherDuck to test gold table sync logic.

**Layer 4: Integration tests (real servers, manual)**

Run manually against real SQL Server and MotherDuck in dev environment. Not automated in CI.

### Test Fixtures

```python
@pytest.fixture
def test_data_dir(tmp_path):
    """Create test CSV/DuckDB files in a temp directory."""

@pytest.fixture
def test_config(test_data_dir, tmp_path):
    """Generate feather.yaml pointing to test data."""

@pytest.fixture
def duckdb_con():
    """Fresh in-memory DuckDB connection."""
```

---

## 8. Open Questions

1. **MotherDuck region** — Where is the instance hosted? Latency from India matters.
2. **ROWVERSION columns** — Do Icube ERP tables have SQL Server's ROWVERSION column?
3. **Excel reader** — DuckDB's `st_read()` requires the `spatial` extension. Verify it handles `.xlsx` reliably, or use `openpyxl` as fallback.

---

## 9. Deferred Ideas

See `docs/research.md` section "Ideas Evaluated and Deferred to Later Stages" for 13 deferred ideas with revisit triggers.

---

## 10. Reference

- Research: `docs/research.md` (7 agents synthesized)
- Existing POC: `~/Desktop/NonDropBoxProjects/afans-reporting-dev/`
- Source DB snapshot: `afans-reporting-dev/discovery/discovery.duckdb`
