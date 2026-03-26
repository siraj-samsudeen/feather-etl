# feather-etl: Product Requirements Document

**Version:** 1.3
**Date:** 2026-03-26
**Status:** Draft

| Version | Changes |
|---------|---------|
| 1.0 | Initial draft |
| 1.1 | Source abstraction, file-based sources added |
| 1.2 | NFR updates, connector interface design |
| 1.3 | Excel reader corrected (read_xlsx + openpyxl); Slack replaced with SMTP email; bronze made optional; append strategy added; silver redefined as canonical mapping layer; connector/transform library roadmap added; schema drift behavior (Option B) defined; product scope clarified for multi-client deployment |

---

## 1. Product Overview

### What

feather-etl is a config-driven Python ETL **package** for extracting data from heterogeneous ERP and database sources, transforming it with plain SQL in local DuckDB, and syncing final tables to a remote destination for dashboards and analytics.

The core package works entirely with file-based sources (DuckDB, SQLite, CSV, Excel, JSON) and local DuckDB as the destination — fully testable without any external servers. Client deployments extend it with database sources (SQL Server, SAP B1, SAP S4 HANA, custom ERPs) and cloud destinations (MotherDuck).

feather-etl is designed to be deployed across many clients, each with their own source systems and schemas. It is the entire data platform for small-to-medium clients who have no existing data infrastructure, and a lightweight extraction layer for larger clients who already have their own bronze/raw layer.

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
- **Multi-client by design.** Each client deployment is an independent `feather.yaml` configuration. The package is source-system agnostic — SAP B1, custom Indian ERPs, SQL Server all use the same pipeline. A connector and canonical transform library (planned, not v1) will allow reuse of silver mappings across clients with the same source system.
- **Layers are optional.** Bronze, silver, and gold schemas exist but are not all mandatory. Small clients with no compliance requirement can skip bronze and land directly into silver. Large enterprise clients who already have a bronze layer can use feather-etl as an extraction-only layer.

---

## 2. Users and Use Cases

### Primary Users

1. **Package developer** — builds and tests feather-etl itself, using file-based sources
2. **Data platform operator** (internal team) — deploys and configures feather-etl per client; writes silver/gold SQL transforms; manages schedules and alerts
3. **Client analyst** — works at the client site; does last-mile customisation of gold transforms and dashboards, guided by LLM agents; does not write extraction config or Python

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
Source system changes columns. feather-etl detects drift via schema snapshot comparison, logs it, sends an email alert. Added columns are loaded automatically. Removed columns are loaded as NULL. Type changes attempt a cast; failures are quarantined with a critical alert.

---

## 3. Functional Requirements

### FR1: Source Abstraction

**FR1.1** The system shall support multiple source types, configured per-table:

| Source Type | Reader | Change Detection | Incremental Support |
|------------|--------|-----------------|-------------------|
| `duckdb` | DuckDB ATTACH | File mtime + file hash | Yes (if timestamp column exists) |
| `sqlite` | DuckDB `sqlite_scan()` | File mtime + file hash | Yes (if timestamp column exists) |
| `csv` | DuckDB `read_csv()` | File mtime + file hash | No (full refresh only) |
| `excel` | `read_xlsx()` via DuckDB `excel` extension; `.xls` files fall back to `openpyxl` | File mtime + file hash | No (full refresh only) |
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

# Excel .xlsx example (DuckDB excel extension)
temp_con.execute("INSTALL excel; LOAD excel;")
temp_con.execute("SELECT * FROM read_xlsx(?)", [file_path])
arrow_table = temp_con.fetcharrow()

# Excel .xls fallback (openpyxl)
import openpyxl
# convert to Arrow via PyArrow directly
```

For `.xlsx` files, use DuckDB's `excel` extension (`read_xlsx()`). For `.xls` files, fall back to `openpyxl` and convert to a PyArrow Table before passing to the loader. If a file path ends in `.xls`, the system shall log a warning that the native DuckDB reader does not support this format and openpyxl is being used.

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
- `target_table` — local DuckDB target (e.g., `silver.sales_invoice`). Defaults to `silver.{name}` if omitted.
- `strategy` — `incremental`, `full`, or `append`:
  - `full` — swap pattern (drop and recreate). Use for small reference tables with no history requirement.
  - `incremental` — partition overwrite keyed on timestamp watermark. Use for large transactional tables.
  - `append` — insert only, never delete. Use for audit trail, compliance, or when the operator wants to preserve full history. Requires `timestamp_column`.
- `schedule` — human-readable schedule name or tier shortcut
- `primary_key` — list of primary key columns
- `timestamp_column` — (required for `incremental` and `append`) column used for watermarking
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
- `strategy: append` requires `timestamp_column`
- `schedule` must resolve to a known schedule name
- `primary_key` is required for all tables
- `source.type` must be a recognized source type
- File-based source types require `source.path` to exist
- `target_table` schema prefix must be one of: `bronze`, `silver`, `gold` (or omitted, defaulting to `silver`)
- All relative paths (`destination.path`, `state.path`, `source.path`) are resolved relative to the `feather.yaml` file location, not CWD. Validation shall resolve and log the absolute path for each at startup so the operator can confirm locations before a run.

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

**FR4.2** The local DuckDB shall create schemas on `feather setup`: `bronze`, `silver`, `gold`, `_quarantine`. All four schemas are created regardless of whether they are used — this keeps the structure consistent across deployments and makes enterprise clients' expectations met out of the box.

> **Decision: bronze is optional per table, not per deployment.**
> The schema exists in every DuckDB file, but no table is forced into it. Bronze serves two distinct purposes depending on how it is configured:
>
> **1. Development cache** (`strategy: full` or `strategy: incremental`)
> During active development, the operator extracts all columns from the source ERP into bronze once, then iterates on silver/gold transforms locally without hitting the source database again. ERP connections are slow, VPN-gated, and sometimes rate-sensitive — a local bronze snapshot eliminates that friction entirely. Silver views read from bronze; the operator tweaks the silver SQL and reruns `feather run` against the local cache. When transforms are stable, the operator can drop bronze and reconfigure to land directly into silver for production.
>
> **2. Audit trail / compliance cache** (`strategy: append`)
> For regulated clients (Unilever, IOM, WHO scale), bronze is append-only — every extracted row is preserved forever with `_etl_loaded_at` and `_etl_run_id`. This enables point-in-time reconstruction, compliance audits, and re-derivation of silver/gold if transform logic changes.
>
> Small Indian SMB clients with no compliance requirement and stable transforms configure `target_table: silver.sales_invoice` and skip bronze entirely — landing column-selected, renamed data directly into silver. The operator decides per table, per client.

**FR4.3** For **full** strategy tables, use the **swap pattern** (atomic, no partial reads during load):
1. `CREATE TABLE {target}_new AS SELECT * FROM staging_data`
2. `DROP TABLE IF EXISTS {target}`
3. `ALTER TABLE {target}_new RENAME TO {final_name}`
All within a single transaction.

**FR4.4** For **incremental** strategy tables, use **partition-based overwrite**:
1. `DELETE FROM {target} WHERE {timestamp_column} >= {min_timestamp_in_batch}`
2. `INSERT INTO {target} SELECT *, current_timestamp AS _etl_loaded_at, {run_id} AS _etl_run_id FROM staging_data`

**FR4.5** For **append** strategy tables, use **insert-only**:
1. `INSERT INTO {target} SELECT *, current_timestamp AS _etl_loaded_at, {run_id} AS _etl_run_id FROM staging_data`
No deletes. Rows accumulate. The watermark advances so only new rows are fetched each run. Use for audit trail, compliance, or full history preservation. If the target table does not yet exist, it is created on first run.

**FR4.6** Every loaded row shall include two metadata columns regardless of target schema (bronze, silver, or gold):
- `_etl_loaded_at` (TIMESTAMP) — when the row was loaded
- `_etl_run_id` (VARCHAR) — links to the `_runs` table

**FR4.7** If a `column_map` is configured, renaming is applied at the PyArrow level (zero-copy) before loading.

**FR4.8** Loading and state update shall be wrapped in a single DuckDB transaction.

**FR4.10** All three load strategies shall be idempotent — running `feather run --table X` twice in a row shall produce the same result as running it once:
- `full` — guaranteed by the swap pattern (second run produces identical table)
- `incremental` — guaranteed by partition delete + insert on the same watermark window
- `append` — on retry after a partial failure, the system shall first delete any rows where `_etl_run_id` matches the failed run's ID, then re-insert. This prevents duplicate rows from partial writes.

**FR4.9** Schema drift during load shall be handled as follows:
- `added` column: ALTER TABLE to add the new column to the target, then load normally. Historical rows will have NULL for the new column.
- `removed` column: load the batch with the missing column as NULL. Target table retains the column definition.
- `type_changed`: attempt DuckDB cast. If cast succeeds, load proceeds. If cast fails, affected rows are routed to `_quarantine.{table_name}` and a `[CRITICAL]` email alert is sent. The run is marked as `partial_success` in `_runs`.

### FR5: Transforms

**FR5.1** Transforms shall be plain `.sql` files stored in `transforms/silver/` and `transforms/gold/`.

**FR5.2** Silver transforms shall be DuckDB **views** (`CREATE OR REPLACE VIEW`).

> **Decision: silver is the canonical mapping layer.**
> Silver is where source-system-specific naming, column selection, and light cleaning happen. A SAP B1 table `ORDR` and a custom ERP table `SalesHeader` both become `silver.sales_order` with the same 10-15 columns in the same shape. Client analysts and LLM agents work against silver, not bronze. This is also the layer where the planned connector/transform library (v2) will provide reusable canonical mappings per source system — so the operator deploying a second SAP B1 client can reuse the silver transforms from the first.

**FR5.3** Gold transforms shall be DuckDB **materialized tables** (`CREATE OR REPLACE TABLE ... AS SELECT ...`), rebuilt after each extraction run. Gold is client-specific — KPIs, aggregations, denormalized tables shaped for Rill dashboards or BI tools. Only gold tables are synced to MotherDuck.

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

**FR7.1** State shall be stored in a local DuckDB file, separate from the data DuckDB. Path resolution order:
1. Use `state.path` from `feather.yaml` if configured (absolute or relative to `feather.yaml` location)
2. Otherwise default to `{feather.yaml directory}/feather_state.duckdb`

The config file's directory is the anchor — not the current working directory — so the state DB location is stable regardless of how or where the process is invoked (CLI, cron, APScheduler daemon).

The data DuckDB (`destination.path`) follows the same resolution: relative paths are resolved against `feather.yaml`'s directory, not CWD.

**FR7.2** The state database shall contain these tables:

**`_watermarks`** — per-table extraction state:

| Column | Type | Purpose |
|--------|------|---------|
| table_name | VARCHAR PK | Table identifier |
| strategy | VARCHAR | incremental, full, or append |
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
| status | VARCHAR | success, failure, skipped, partial_success |
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

**FR7.3** Watermark update rules:
- Watermark advances **only** when the entire pipeline step succeeds for that table
- If sync is **not** configured: watermark advances after successful load
- If sync is **configured**: watermark advances only after both load AND sync succeed
- If load fails: watermark unchanged, data load transaction rolled back
- If load succeeds but sync fails: watermark unchanged, loaded data remains in local DuckDB, next run re-extracts the same window and re-syncs

> **Decision: sync failure rolls back the watermark, not the load.**
> The load is committed to local DuckDB (it's in a transaction that already closed). Only the watermark advancement is withheld. This means on re-run, some rows may be re-loaded into local DuckDB (handled by idempotency, FR4.10) and re-synced to MotherDuck. This trades a small amount of redundant work for a hard guarantee: the watermark only reflects data that is fully visible in MotherDuck. The alternative — advancing the watermark after load regardless of sync — risks MotherDuck silently falling behind with no automated recovery path.

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

**FR8.4** DQ failures are logged to `_dq_results` and trigger email alert at `[WARNING]` severity (if configured), but do NOT block the pipeline.

### FR9: Schema Drift Detection

**FR9.1** On each extraction, compare source schema against stored snapshot in `_schema_snapshots`.

**FR9.2** Detect: `added` (new column), `removed` (column gone), `type_changed` (data type changed).

**FR9.3** Schema changes logged in `_runs` and trigger email alert at `[INFO]` severity (if configured).

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

**FR12.1** Alerts via SMTP email using Python stdlib `smtplib` + `email`. No additional dependency required.

**FR12.2** Email configuration via `feather.yaml` `alerts` section:
```yaml
alerts:
  smtp_host: "smtp.gmail.com"
  smtp_port: 587
  smtp_user: "${ALERT_EMAIL_USER}"
  smtp_password: "${ALERT_EMAIL_PASSWORD}"
  alert_to: "operator@example.com"
  alert_from: "feather-etl@example.com"   # optional, defaults to smtp_user
```

**FR12.3** Severities map to email subject prefixes:
- `[CRITICAL]` — pipeline failures, load errors
- `[WARNING]` — DQ check failures
- `[INFO]` — schema drift detected

**FR12.4** No-op if `alerts` section is not configured. Pipeline runs silently.

### FR13: Retry and Error Handling

**FR13.1** On failure: record run, increment `retry_count`, compute `retry_after` using linear backoff, do NOT update watermark, send `[CRITICAL]` email alert (if configured).

> **Decision: linear backoff, base 15 minutes, cap 2 hours.**
> Formula: `retry_after = now + min(retry_count × 15 minutes, 120 minutes)`
> After 1 failure: wait 15 min. After 2: 30 min. After 3: 45 min. Capped at 120 min from failure 8 onwards.
> Exponential backoff was rejected — a backoff that reaches 8–16 hours defeats the purpose of a scheduled pipeline. Linear keeps the retry window predictable and within the operator's scheduling expectations. At cap, a persistent failure alerts every ~4 scheduled runs rather than going silent.

**FR13.2** Skip table if `current_time < retry_after`. Record status `skipped` in `_runs` with `error_message` referencing the original failure.

**FR13.3** Reset `retry_count` to 0 and clear `retry_after` on any successful run.

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
| openpyxl | `.xls` fallback reader for Excel sources (DuckDB `excel` extension handles `.xlsx` natively) |

Alerting uses Python stdlib `smtplib` — no extra dependency. The package is complete out of the box — all source types, scheduling, and alerting are included.

**NFR2: Code Size.** Core package under 1,210 lines of Python (increased from 1,000 to accommodate source abstraction; +10 for openpyxl Excel fallback).

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
| `ExcelSource` | `FileSource` | ~35 | `read_xlsx()` via DuckDB `excel` extension for `.xlsx`; `openpyxl` fallback for `.xls` |
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

### Planned: Connector and Canonical Transform Library (v2, not in scope for v1)

feather-etl v1 is source-system agnostic — the operator writes all silver transforms per client. In v2, a separate `feather-connectors` package will provide:

- **Source connectors** for common systems: SAP B1, SAP S4 HANA, Tally, custom Indian ERPs. Each connector ships with pre-built extraction config (table names, primary keys, timestamp columns, filters) so the operator configures credentials, not schema.
- **Canonical silver transform library** — reusable `.sql` files that map source-system-specific column names to standard canonical names (`sales_order`, `customer`, `invoice`, etc.). A second SAP B1 client reuses the same silver transforms as the first, with only the gold layer customised per client.
- **LLM agent interface** — client analysts interact with the canonical silver layer via an LLM-guided query interface. They do not write SQL or touch YAML.

This is explicitly out of scope for v1. The v1 Protocol-based architecture is designed to accommodate v2 connectors without changes to the core package.

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
│   ├── SQL Server ─────────────┐
│   ├── SAP B1 ─────────────────┤ pyodbc → PyArrow Table
│   └── Custom ERPs ────────────┘
│
▼
PyArrow Table (common format from all sources)
│
▼  column_map applied (zero-copy rename/select)
│
▼
Local DuckDB: feather_data.duckdb
│
├── bronze schema  ← OPTIONAL (compliance/audit clients only)
│   Raw ERP data, all columns, strategy: append
│   _etl_loaded_at + _etl_run_id on every row
│
├── silver schema  ← PRIMARY working layer
│   Canonical column names, selected columns, light cleaning
│   Views over bronze (if bronze used) OR direct load target
│   Client analysts + LLM agents work here
│
├── gold schema    ← Dashboard layer
│   Materialized tables: KPIs, aggregations, denormalized
│   Rebuilt after every extraction run
│   Only layer synced to MotherDuck
│
└── _quarantine schema  ← Bad rows from type_changed drift

Local DuckDB: feather_state.duckdb
├── _watermarks (watermark + file mtime/hash + retry state)
├── _runs, _run_steps, _dq_results, _schema_snapshots

Optional: Remote Sync
└── MotherDuck (via ATTACH — gold tables only)
    └── Rill Data / BI tools (dashboards)
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
│   ├── excel.py                ~35 LOC   ExcelSource (read_xlsx + openpyxl fallback)
│   ├── sqlserver.py            ~80 LOC   SqlServerSource (pyodbc + CHECKSUM_AGG)
│   └── registry.py             ~20 LOC   SOURCE_REGISTRY + create_source()
├── destinations/
│   ├── __init__.py             ~10 LOC   Destination Protocol
│   └── duckdb.py              ~100 LOC   DuckDBDestination (swap + partition + sync)
├── state.py                   ~110 LOC   Watermarks, run history, DQ results
├── quality.py                  ~80 LOC   DQ checks (not_null, unique, row_count)
├── schema.py                   ~60 LOC   Schema drift detection + snapshots
├── alerts.py                   ~40 LOC   SMTP email alerts (smtplib)
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
   (incremental + append strategies skip change detection — always extract)
4. Extract data (source-type-specific):
   ├── File sources: DuckDB native reader → PyArrow
   └── SQL Server / SAP / custom ERP: pyodbc → chunked fetch → PyArrow
   For incremental + append: apply watermark filter + overlap window
5. Apply column_map (zero-copy rename/select in PyArrow)
6. Load to local DuckDB:
   ├── full     → swap pattern (atomic drop + recreate)
   ├── incremental → partition overwrite (delete + insert)
   └── append   → insert only (no deletes)
7. Run DQ checks → log to _dq_results → [WARNING] email if failures
8. Detect schema drift → handle per FR4.9 → [INFO] or [CRITICAL] email
9. Rebuild gold tables (rematerialize all gold transforms)
10. Sync gold to remote (if configured)
11. Update state (watermark + run record) in single transaction
12. Return RunResult (success / failure / skipped / partial_success)
```

### Example Config: Development Workflow (bronze as local cache)

During active development, extract all columns into bronze once, then iterate on silver/gold SQL transforms locally without hitting the source DB again.

```yaml
source:
  type: sqlserver
  connection_string: "${SQL_SERVER_CONNECTION_STRING}"

destination:
  path: ./feather_dev.duckdb

state:
  path: ./feather_dev_state.duckdb

tables:
  - name: sales_invoice
    source_table: dbo.SALESINVOICE
    target_table: bronze.sales_invoice   # all columns, local cache
    strategy: full                       # swap — just a snapshot
    primary_key: [ID]
    schedule: daily                      # refresh once a day during dev
```

Silver transforms in `transforms/silver/sales_invoice.sql` read from bronze. The operator tweaks the SQL and runs `feather run` locally — no source DB hit. When transforms are stable and ready for production, reconfigure `target_table: silver.sales_invoice` with `column_map` to bypass bronze.

---

### Example Config: File-Based Testing (silver-direct, no bronze)

This is the typical pattern for Indian SMB clients — data lands directly in silver, column-selected and renamed at extraction time. No bronze layer needed.

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
    target_table: silver.sales_invoice   # lands directly in silver
    strategy: incremental
    timestamp_column: modified_date
    schedule: hot
    primary_key: [invoice_id]
    column_map:                          # select + rename 10 of 150 columns
      InvoiceID: invoice_id
      CustCode: customer_code
      InvDate: invoice_date
      NetAmt: net_amount
      ModifiedDate: modified_date
    quality_checks:
      not_null: [invoice_id, customer_code]

  - name: customer_master
    source_table: customers.csv
    target_table: silver.customer_master
    strategy: full
    schedule: cold
    primary_key: [customer_code]
    column_map:
      CustomerCode: customer_code
      CustomerName: customer_name
```

### Example Config: Production with Bronze (compliance / audit client)

For clients requiring raw data preservation (audit trail, compliance). Bronze uses `append` strategy — rows accumulate, nothing is deleted. Silver views over bronze for canonical naming.

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
  smtp_host: "smtp.gmail.com"
  smtp_port: 587
  smtp_user: "${ALERT_EMAIL_USER}"
  smtp_password: "${ALERT_EMAIL_PASSWORD}"
  alert_to: "operator@example.com"

tables:
  - name: sales_invoice
    source_table: dbo.SALESINVOICE
    target_table: bronze.sales_invoice   # raw, all columns, append-only
    strategy: append                     # insert only — full audit trail
    timestamp_column: ModifiedDate
    schedule: hot
    primary_key: [ID]
    filter: "STATUS <> 1"
    quality_checks:
      not_null: [SI_NO, Custome_Code]

  - name: customer_master
    source_table: dbo.CUSTOMERMASTER
    target_table: bronze.customer_master
    strategy: full                       # small reference table, swap ok
    checksum_columns: [customercode, customername, salestype, city, states]
    schedule: cold
    primary_key: [customercode]
    quality_checks:
      not_null: [customercode, customername]
      unique: [customercode]
```

Silver views for the above would live in `transforms/silver/sales_invoice.sql`:
```sql
CREATE OR REPLACE VIEW silver.sales_invoice AS
SELECT
    ID          AS invoice_id,
    SI_NO       AS invoice_no,
    Custome_Code AS customer_code,
    NetAmount   AS net_amount,
    ModifiedDate AS modified_date,
    _etl_loaded_at,
    _etl_run_id
FROM bronze.sales_invoice
```

---

## 7. Feature List for Implementation

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
| F8 | DuckDB destination | DuckDBDestination: schema creation, swap pattern, partition overwrite, append (insert-only), metadata columns, column_map | F2, F4 |
| F9 | Core pipeline | `run_table()` wiring source + destination + state. `run_all()`. | F2, F3, F4, F5, F7, F8 |
| F10 | CLI (basic) | `feather setup`, `feather run`, `feather status` | F9 |

### Phase 2: Observability

| # | Feature | Description | Depends On |
|---|---------|-------------|-----------|
| F11 | Data quality checks | Declarative DQ (not_null, unique, row_count), results to `_dq_results` | F9 |
| F12 | Schema drift detection | Schema comparison via source.get_schema(), snapshots, change classification | F3, F5, F7 |
| F13 | Alerting | SMTP email (smtplib), [CRITICAL]/[WARNING]/[INFO] severities, no-op if unconfigured | F11, F12 |
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

## 8. Testing Strategy

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

## 9. Open Questions

1. **MotherDuck region** — Where is the instance hosted? Latency from India matters.
2. **ROWVERSION columns** — Do Icube ERP tables have SQL Server's ROWVERSION column?
3. **Excel reader** — ~~DuckDB's `st_read()` requires the `spatial` extension.~~ **Resolved:** Use DuckDB's `excel` extension (`read_xlsx()`) for `.xlsx` files — it is a core extension, ships with DuckDB, and is more efficient than the spatial extension's incidental xlsx support (which may be removed). For `.xls` files, fall back to `openpyxl` (added as 8th dependency). Constraint: `.xls` fallback produces a warning in logs; `.xlsx` is the recommended format for all new source files.

---

## 10. Deferred Ideas

See `docs/research.md` section "Ideas Evaluated and Deferred to Later Stages" for 13 deferred ideas with revisit triggers.

---

## 11. Reference

- Research: `docs/research.md` (7 agents synthesized)
- Existing POC: `~/Desktop/NonDropBoxProjects/afans-reporting-dev/`
- Source DB snapshot: `afans-reporting-dev/discovery/discovery.duckdb`
