# feather-etl: Product Requirements Document

**Version:** 1.4
**Date:** 2026-03-26
**Status:** Draft

| Version | Changes |
|---------|---------|
| 1.0 | Initial draft |
| 1.1 | Source abstraction, file-based sources added |
| 1.2 | NFR updates, connector interface design |
| 1.3 | Excel reader corrected (read_xlsx + openpyxl); Slack replaced with SMTP email; bronze made optional; append strategy added; silver redefined as canonical mapping layer; connector/transform library roadmap added; schema drift behavior (Option B) defined; product scope clarified for multi-client deployment |
| 1.4 | Out-of-scope section added (Section 3); NFR3 performance targets made concrete (file size + hardware baseline); NFR8 security added (credential redaction, file permissions, plain-text warning); NFR9 reliability added (state DB recovery, scheduler resilience); UC8 backfill, UC9 validate dry run, UC10 state recovery added; feather validate CLI command added to FR11 and F10 |

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

**UC8: Backfill / Re-extraction**
A DQ issue is found in historical data, or a silver transform bug is discovered that requires re-deriving data from an earlier point. Operator resets the watermark for a table directly in `feather_state.duckdb` (`UPDATE _watermarks SET last_value = '2024-01-01' WHERE table_name = 'sales_invoice'`), then runs `feather run --table sales_invoice`. The pipeline re-extracts from the reset watermark. For `append` strategy tables, the operator also truncates or drops the target table before resetting the watermark. There is no CLI command for this in v1 — it is a manual state DB operation. `feather backfill` is deferred (see Section 3 out-of-scope).

**UC9: Config Validation (Dry Run)**
Operator has edited `feather.yaml` to add a new table or change schedules and wants to verify the config is valid before running. Runs `feather validate` — the system parses and validates the config, resolves all paths, checks that source files exist (for file-based sources), and prints a summary of what would run without executing anything. Exits with code 0 on success, non-zero with specific error messages on failure.

**UC10: State DB Recovery**
The `feather_state.duckdb` file is lost or corrupted (disk failure, accidental deletion). Operator runs `feather setup` — the system recreates the state DB from scratch with empty tables. All watermarks are lost; the next `feather run` treats every table as a first run and performs full re-extraction. For `incremental` tables this means re-extracting all history from the source; for `append` tables the operator should manually truncate the target table before running to avoid duplicates. The operator is informed of the reset via a `[WARNING]` log.

---

## 3. Out of Scope (v1)

The following are explicitly not part of feather-etl v1. They are documented here to prevent scope creep and to give clear answers when clients or team members ask.

| Out of scope | Notes |
|---|---|
| REST API / web UI for monitoring | Operators use `feather status`, `feather history`, and direct DuckDB queries. A web UI is a v3+ consideration. |
| Multi-tenant support | Each client is a separate deployment with its own `feather.yaml`. There is no shared multi-tenant runtime. |
| Support for destination types other than local DuckDB + MotherDuck | Snowflake, BigQuery, Redshift are not supported. The Destination Protocol makes adding them possible without core changes. |
| Real-time / streaming ingestion | feather-etl is a batch pipeline. Minimum granularity is `hourly`. |
| Automated schema evolution (ALTER TABLE beyond drift handling) | FR4.9 handles drift permissively at load time. There is no migration framework or schema versioning. |
| Role-based access control | No user management, no permissions layer. Access is controlled at the OS/file level. |
| Data lineage tracking beyond run metadata | `_runs` and `_run_steps` provide operational lineage. Column-level lineage is not tracked. |
| dbt compatibility or SQL dialect translation | Transforms are plain DuckDB SQL. No dbt models, no Jinja, no cross-dialect support. |
| Docker / Kubernetes deployment | feather-etl runs as a Python process. Containerisation is the operator's responsibility. |
| Connector and canonical transform library | Planned for v2 as `feather-connectors`. See Section 5 for the roadmap. |
| LLM agent interface for client analysts | Planned for v2+. v1 provides the data layer (silver/gold) that the agent interface will sit on top of. |
| Backfill / historical re-extraction via CLI flag | Operators can reset a watermark directly in `feather_state.duckdb` to trigger re-extraction. A `feather backfill` command is deferred. |

---

## 4. Functional Requirements

### FR1: Source Abstraction

feather-etl supports heterogeneous source systems — CSV files, DuckDB databases, SQL Server instances — yet the loader never knows which source produced the data. This is possible because every source returns a PyArrow Table as the common interchange format. The source type determines *how* data is read, but the loader always receives the same columnar format. This decoupling is what makes the pipeline testable end-to-end with file-based sources and deployable against production databases without changing loader or transform code.

| Source Type | Reader | Change Detection | Incremental Support |
|------------|--------|-----------------|-------------------|
| `duckdb` | DuckDB ATTACH | File mtime + file hash | Yes (if timestamp column exists) |
| `sqlite` | DuckDB `sqlite_scan()` | File mtime + file hash | Yes (if timestamp column exists) |
| `csv` | DuckDB `read_csv()` | File mtime + file hash | No (full refresh only) |
| `excel` | `read_xlsx()` via DuckDB `excel` extension; `.xls` files fall back to `openpyxl` | File mtime + file hash | No (full refresh only) |
| `json` | DuckDB `read_json()` | File mtime + file hash | No (full refresh only) |
| `sqlserver` | pyodbc → PyArrow | CHECKSUM_AGG + COUNT(*) | Yes (timestamp watermark) |

File-based sources use **dual change detection**: first check file modification time (cheap), then compute a content hash only if mtime changed (definitive). Database sources use CHECKSUM_AGG + COUNT(*) for cold/full tables and timestamp watermarks for hot/incremental tables.

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

For `.xlsx` files, use DuckDB's `excel` extension (`read_xlsx()`). For `.xls` files, fall back to `openpyxl` and convert to a PyArrow Table before passing to the loader.

> **Decision: DuckDB native readers rather than pandas or direct Python file reading.**
> DuckDB's built-in readers (`read_csv`, `read_json`, `read_xlsx`, `sqlite_scan`) are columnar, produce zero-copy Arrow output via `fetcharrow()`, and handle type inference reliably for analytical data. Pandas would add a heavy dependency and require an extra conversion step (DataFrame → Arrow). Direct Python file reading (csv module, json module) would require manual type inference and columnar assembly. DuckDB readers give us correct types, efficient memory layout, and a single code path for all file formats.

#### Requirements

```
# FR1.1
THE SYSTEM SHALL support multiple source types (duckdb, sqlite, csv, excel, json, sqlserver),
configured per-table via the source type field.

# FR1.2
WHEN a file-based source is configured
THE SYSTEM SHALL check file mtime (os.path.getmtime()) before computing hash.

# FR1.3
WHEN file mtime is unchanged compared to stored mtime
THE SYSTEM SHALL skip extraction without computing a hash.

# FR1.4
WHEN file mtime has changed
THE SYSTEM SHALL compute file hash (hashlib.md5) and compare to stored hash.

# FR1.5
WHEN file mtime changed but hash is unchanged
THE SYSTEM SHALL skip extraction (file was touched but content identical).

# FR1.6
WHEN file hash has changed
THE SYSTEM SHALL extract the file (full refresh or incremental depending on strategy).

# FR1.7
THE SYSTEM SHALL use CHECKSUM_AGG + COUNT(*) for change detection
on SQL Server full-strategy tables, and timestamp watermarks for incremental tables.

# FR1.8
THE SYSTEM SHALL return data as PyArrow Tables from all source types to the loader.

# FR1.9
THE SYSTEM SHALL read file-based sources via DuckDB native readers
on a temporary DuckDB connection, then export to PyArrow via fetcharrow().

# FR1.10
WHEN a file path ends in .xls
THE SYSTEM SHALL fall back to openpyxl for reading
AND log a warning that the native DuckDB reader does not support .xls format.

# FR1.11
WHEN a file-based source has strategy: incremental
THE SYSTEM SHALL apply timestamp watermark logic:
WHERE {timestamp_column} >= {effective_watermark} ORDER BY {timestamp_column}.
```

#### Acceptance Criteria

**AC-FR1.a:** Given a CSV source file that was touched (mtime updated) but has identical content, when `feather run` executes, then extraction is skipped and the run is recorded as `skipped` with reason "unchanged".

**AC-FR1.b:** Given an Excel file with `.xls` extension, when extraction runs, then a warning is logged mentioning openpyxl fallback and the data is returned as a PyArrow Table identical in schema to what DuckDB's `read_xlsx` would produce for an equivalent `.xlsx` file.

**AC-FR1.c:** Given a DuckDB file source with `strategy: incremental` and `timestamp_column: modified_date`, when the stored watermark is `2024-01-15T10:00:00`, then the extraction query includes `WHERE modified_date >= '2024-01-15T09:58:00'` (applying the 2-minute overlap window).

### FR2: Configuration

One YAML file (`feather.yaml`) defines everything: source connection, destination, tables, schedules, alerts, and defaults. One file per client project. The goal is reproducible deployments — an operator can copy a `feather.yaml` to a new machine, set environment variables, and run. No Python code, no programmatic configuration, no hidden state.

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

```yaml
destination:
  path: ./feather_data.duckdb        # local DuckDB (always)

sync:                                 # optional remote sync
  type: motherduck
  token: "${MOTHERDUCK_TOKEN}"
  database: "client_analytics"
```

Each table is independently configurable with:
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

Schedule tiers allow shorthand:
```yaml
schedule_tiers:
  hot: "twice daily"
  cold: "weekly"
```

> **Decision: Environment variable substitution via `${VAR_NAME}` syntax.**
> Credentials (connection strings, tokens, SMTP passwords) must never be stored in YAML. The `${VAR_NAME}` syntax is resolved at load time via `os.path.expandvars()`. This is simpler than dotenv files or vault integrations and matches how operators manage secrets in cron and systemd environments.

> **Decision: All relative paths resolve against `feather.yaml` location, not CWD.**
> The config file's directory is the anchor so the state DB and data DB locations are stable regardless of how or where the process is invoked (CLI, cron, APScheduler daemon). Validation resolves and logs the absolute path for each at startup so the operator can confirm locations before a run.

#### Requirements

```
# FR2.1
THE SYSTEM SHALL be configured via a single YAML file (feather.yaml).

# FR2.2
WHEN the config contains ${VAR_NAME} syntax
THE SYSTEM SHALL resolve it at load time via os.path.expandvars().

# FR2.3
THE SYSTEM SHALL support source configuration per source type
(csv, duckdb, sqlite, excel, json, sqlserver) with type-specific connection details.

# FR2.4
THE SYSTEM SHALL support destination configuration with a local DuckDB path
and an optional remote sync section.

# FR2.5
THE SYSTEM SHALL support per-table configuration with name, source_table,
target_table, strategy, schedule, primary_key, timestamp_column,
checksum_columns, filter, quality_checks, and column_map fields.

# FR2.6
IF target_table is omitted THEN THE SYSTEM SHALL default to silver.{name}.

# FR2.7
THE SYSTEM SHALL support human-readable schedule names:
hourly, every 2 hours, twice daily, daily, weekly.

# FR2.8
THE SYSTEM SHALL support schedule tier shortcuts mapping tier names
to schedule names (e.g., hot → twice daily).

# FR2.9
THE SYSTEM SHALL support global defaults for overlap_window_minutes (default: 2)
and batch_size (default: 120,000).

# FR2.10
WHEN strategy is incremental or append AND timestamp_column is not configured
THE SYSTEM SHALL raise a validation error at load time.

# FR2.11
WHEN primary_key is not configured for a table
THE SYSTEM SHALL raise a validation error at load time.

# FR2.12
WHEN source.type is not a recognized source type
THE SYSTEM SHALL raise a validation error at load time.

# FR2.13
WHEN a file-based source type is configured AND source.path does not exist
THE SYSTEM SHALL raise a validation error at load time.

# FR2.14
WHEN target_table schema prefix is not one of bronze, silver, gold
THE SYSTEM SHALL raise a validation error at load time.

# FR2.15
THE SYSTEM SHALL resolve all relative paths (destination.path, state.path, source.path)
relative to the feather.yaml file location, not CWD.

# FR2.16
THE SYSTEM SHALL resolve and log absolute paths at startup
so the operator can confirm locations before a run.
```

#### Acceptance Criteria

**AC-FR2.a:** Given a `feather.yaml` with `strategy: incremental` and no `timestamp_column`, when config is loaded, then a validation error is raised naming the table and the missing field.

**AC-FR2.b:** Given a `feather.yaml` at `/opt/client/feather.yaml` with `destination.path: ./data.duckdb`, when config is loaded, then the resolved path is `/opt/client/data.duckdb` regardless of the process CWD.

**AC-FR2.c:** Given a `feather.yaml` with `${MOTHERDUCK_TOKEN}` in the sync section, when the environment variable is set to `abc123`, then the resolved config contains `abc123` — not the literal string `${MOTHERDUCK_TOKEN}`.

### FR3: Extraction

The extractor is stateless. It receives parameters (table name, watermark value, filter, batch size) and returns a PyArrow Table. It does not read or write watermarks, does not know about run history, and does not decide whether extraction should happen. That decision belongs to the pipeline orchestrator, which reads state, calls `detect_changes()`, and only invokes the extractor if work is needed. This separation means the extractor can be tested in isolation with no state database.

Two extraction paths exist — file-based (DuckDB native readers → PyArrow) and database (pyodbc cursor → chunked fetch → PyArrow) — but both produce the same output format.

**File-based extraction** (DuckDB, SQLite, CSV, Excel, JSON):
- Uses DuckDB's native readers to load file contents
- Returns PyArrow Table
- Change detection via file mtime + hash (stored in `_watermarks`)

**Database extraction** (SQL Server):
- Uses pyodbc with `SET NOCOUNT ON`
- Chunked fetching (`cursor.fetchmany(batch_size)`) → PyArrow
- Change detection via CHECKSUM_AGG + COUNT(*) for full-refresh tables
- Timestamp watermark for incremental tables

#### Requirements

```
# FR3.1
THE SYSTEM SHALL support two extraction paths:
file-based (DuckDB native readers → PyArrow) and
database (pyodbc cursor → chunked fetch → PyArrow).

# FR3.2
WHEN extracting an incremental table (any source type)
THE SYSTEM SHALL apply the filter:
WHERE {timestamp_column} >= {effective_watermark} [AND {filter}]
ORDER BY {timestamp_column}.

# FR3.3
WHEN extracting an incremental table
THE SYSTEM SHALL compute effective_watermark as:
stored watermark minus overlap_window_minutes.

# FR3.4
WHEN extracting a full-strategy file source
THE SYSTEM SHALL check mtime, then hash, and skip if unchanged.

# FR3.5
WHEN extracting a full-strategy SQL Server source
THE SYSTEM SHALL check CHECKSUM_AGG + COUNT(*) and skip if unchanged.

# FR3.6
THE SYSTEM SHALL support extracting source schema metadata:
file sources via DuckDB column metadata, SQL Server via INFORMATION_SCHEMA.COLUMNS.

# FR3.7
THE SYSTEM SHALL keep the extractor stateless —
it receives parameters and returns a PyArrow Table;
all state management is external.
```

#### Acceptance Criteria

**AC-FR3.a:** Given an incremental table with stored watermark `2024-06-01T10:00:00` and `overlap_window_minutes: 2`, when extraction runs, then the query uses effective watermark `2024-06-01T09:58:00` (stored minus 2 minutes).

**AC-FR3.b:** Given a full-strategy CSV source where the file has not changed (same mtime and hash), when `feather run` executes, then no data is read from the file and the run is recorded as `skipped`.

**AC-FR3.c:** Given an incremental table with both a watermark filter and a user-defined `filter: "STATUS <> 1"`, when extraction runs, then both conditions appear in the WHERE clause.

### FR4: Loading

The loader writes PyArrow Tables into a local DuckDB file. Three strategies exist because tables have different update semantics: reference tables that can be swapped atomically (`full`), transactional tables where only recent partitions change (`incremental`), and audit/compliance tables where no row is ever deleted (`append`). Each strategy is idempotent — running twice produces the same result as running once, which is essential for safe retries after partial failures.

All four schemas (`bronze`, `silver`, `gold`, `_quarantine`) are created on `feather setup` regardless of whether they are used — consistent structure across deployments.

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

> **Decision: append strategy for audit trail / compliance.**
> The `append` strategy uses insert-only — no deletes, rows accumulate forever. The watermark advances so only new rows are fetched each run. If the target table does not yet exist, it is created on first run. This was added specifically for regulated clients who need point-in-time reconstruction and full history preservation. The alternative — using `incremental` with soft deletes — would require additional logic and still lose the original row values on update.

> **Decision: schema drift handling — auto-adapt, don't block.**
> Added columns are absorbed (ALTER TABLE + NULL backfill). Removed columns are loaded as NULL (target retains the column). Type changes attempt a DuckDB cast; failures quarantine the affected rows. The alternative — blocking the pipeline on any drift — would cause unnecessary downtime for benign changes (e.g., a new column added to the source). Critical failures (type cast failures) still alert the operator.

#### Requirements

```
# FR4.1
THE SYSTEM SHALL load data into a local DuckDB file configured in destination.path.

# FR4.2
WHEN feather setup runs
THE SYSTEM SHALL create schemas: bronze, silver, gold, _quarantine.

# FR4.3
WHEN loading a full-strategy table
THE SYSTEM SHALL use the swap pattern within a single transaction:
CREATE TABLE {target}_new, DROP TABLE IF EXISTS {target},
ALTER TABLE {target}_new RENAME TO {final_name}.

# FR4.4
WHEN loading an incremental-strategy table
THE SYSTEM SHALL use partition-based overwrite:
DELETE FROM {target} WHERE {timestamp_column} >= {min_timestamp_in_batch},
then INSERT with _etl_loaded_at and _etl_run_id metadata columns.

# FR4.5
WHEN loading an append-strategy table
THE SYSTEM SHALL INSERT only, with _etl_loaded_at and _etl_run_id metadata columns.
No deletes shall occur.

# FR4.6
IF the target table does not exist for an append-strategy table
THEN THE SYSTEM SHALL create it on first run.

# FR4.7
THE SYSTEM SHALL add _etl_loaded_at (TIMESTAMP) and _etl_run_id (VARCHAR)
metadata columns to every loaded row regardless of target schema.

# FR4.8
WHEN column_map is configured
THE SYSTEM SHALL apply renaming at the PyArrow level (zero-copy) before loading.

# FR4.9
THE SYSTEM SHALL wrap loading and state update in a single DuckDB transaction.

# FR4.10
WHEN retrying an append-strategy table after partial failure
THE SYSTEM SHALL first delete rows where _etl_run_id matches the failed run's ID,
then re-insert, preventing duplicate rows.

# FR4.11
WHEN schema drift type is "added" (new column in source)
THE SYSTEM SHALL ALTER TABLE to add the column to the target
AND load normally with historical rows having NULL for the new column.

# FR4.12
WHEN schema drift type is "removed" (column missing from source)
THE SYSTEM SHALL load the batch with the missing column as NULL.
Target table retains the column definition.

# FR4.13
WHEN schema drift type is "type_changed" AND DuckDB cast succeeds
THE SYSTEM SHALL load the data normally.

# FR4.14
WHEN schema drift type is "type_changed" AND DuckDB cast fails
THE SYSTEM SHALL route affected rows to _quarantine.{table_name},
send a [CRITICAL] email alert, and mark the run as partial_success.
```

#### Acceptance Criteria

**AC-FR4.a:** Given an append-strategy table where a previous run partially wrote 50 of 100 rows (same `_etl_run_id`), when the pipeline retries, then the 50 partial rows are deleted before re-inserting all 100 rows — resulting in exactly 100 rows with that run ID.

**AC-FR4.b:** Given an incremental table where a source column `phone_number` was added since the last run, when data is loaded, then the target table has a new `phone_number` column with NULL for all historical rows and values populated for newly loaded rows.

**AC-FR4.c:** Given a full-strategy table, when `feather run --table X` executes twice with no source changes, then the target table contains the same rows after both runs (swap pattern idempotency).

### FR5: Transforms

Transforms follow a two-layer model. Silver is the canonical mapping layer — it normalises source-system-specific naming into a standard shape that client analysts and LLM agents work against. Gold is the materialisation layer — client-specific KPIs, aggregations, and denormalized tables shaped for dashboards. Silver transforms are views (cheap, always reflect current data), gold transforms are materialised tables (rebuilt after each extraction so dashboards see fresh data).

> **Decision: silver is the canonical mapping layer.**
> Silver is where source-system-specific naming, column selection, and light cleaning happen. A SAP B1 table `ORDR` and a custom ERP table `SalesHeader` both become `silver.sales_order` with the same 10-15 columns in the same shape. Client analysts and LLM agents work against silver, not bronze. This is also the layer where the planned connector/transform library (v2) will provide reusable canonical mappings per source system — so the operator deploying a second SAP B1 client can reuse the silver transforms from the first.

#### Requirements

```
# FR5.1
THE SYSTEM SHALL store transforms as plain .sql files
in transforms/silver/ and transforms/gold/ directories.

# FR5.2
THE SYSTEM SHALL execute silver transforms as DuckDB views
(CREATE OR REPLACE VIEW).

# FR5.3
THE SYSTEM SHALL execute gold transforms as DuckDB materialized tables
(CREATE OR REPLACE TABLE ... AS SELECT ...).

# FR5.4
THE SYSTEM SHALL support string.Template variable substitution (${variable})
in SQL files.

# FR5.5
THE SYSTEM SHALL determine transform execution order via
graphlib.TopologicalSorter (stdlib) based on declared dependencies.

# FR5.6
WHEN feather setup runs
THE SYSTEM SHALL execute all transform SQL files.

# FR5.7
WHEN an extraction run completes successfully
THE SYSTEM SHALL rebuild all gold tables (rematerialize).
```

#### Acceptance Criteria

**AC-FR5.a:** Given silver and gold transforms with a dependency chain (gold depends on silver), when `feather setup` runs, then silver views are created before gold tables — verified by gold table containing correct data from silver.

**AC-FR5.b:** Given a gold transform referencing `${schema}` and a template variable `schema=silver`, when the transform executes, then the substitution resolves correctly in the generated SQL.

### FR6: Remote Sync (Optional)

Remote sync pushes gold tables — and only gold tables — to MotherDuck via DuckDB's native ATTACH mechanism. It is the last step in the pipeline, running only after gold tables are rebuilt. If the `sync` section is not configured in YAML, the pipeline stops at local DuckDB with no error and no no-op log noise.

#### Requirements

```
# FR6.1
IF the sync section is not configured in feather.yaml
THEN THE SYSTEM SHALL skip remote sync entirely (no-op, no error).

# FR6.2
THE SYSTEM SHALL sync only gold-schema tables to the remote destination.

# FR6.3
THE SYSTEM SHALL connect to MotherDuck via DuckDB ATTACH:
ATTACH 'md:{database}?motherduck_token={token}'.

# FR6.4
WHEN syncing a gold table
THE SYSTEM SHALL execute:
CREATE OR REPLACE TABLE md.{database}.gold.{table}
AS SELECT * FROM local.gold.{table}.

# FR6.5
THE SYSTEM SHALL reuse a single remote connection across all gold table syncs.

# FR6.6
THE SYSTEM SHALL execute the sync step last in the pipeline,
after gold tables are rebuilt.
```

#### Acceptance Criteria

**AC-FR6.a:** Given a `feather.yaml` with no `sync` section, when the pipeline completes, then no MotherDuck connection is attempted and no sync-related entries appear in `_run_steps`.

**AC-FR6.b:** Given 3 gold tables to sync, when remote sync runs, then a single ATTACH is executed (not 3 separate connections).

### FR7: State Management

State lives in a separate DuckDB file from the data — `feather_state.duckdb` by default. This separation means the data DuckDB can be deleted and rebuilt from source without losing watermarks, run history, or retry state. Watermarks are the single source of truth for extraction progress: they advance only when the entire pipeline step (load + optional sync) succeeds. This invariant guarantees that no data window is silently skipped.

State path resolution: use `state.path` from `feather.yaml` if configured (absolute or relative to `feather.yaml` location), otherwise default to `{feather.yaml directory}/feather_state.duckdb`. The config file's directory is the anchor — not CWD — so the state DB location is stable regardless of invocation method.

> **Decision: sync failure rolls back the watermark, not the load.**
> The load is committed to local DuckDB (it's in a transaction that already closed). Only the watermark advancement is withheld. This means on re-run, some rows may be re-loaded into local DuckDB (handled by idempotency, FR4.10) and re-synced to MotherDuck. This trades a small amount of redundant work for a hard guarantee: the watermark only reflects data that is fully visible in MotherDuck. The alternative — advancing the watermark after load regardless of sync — risks MotherDuck silently falling behind with no automated recovery path.

> **Decision: boundary hash for deduplication at watermark edge.**
> Incremental extraction with overlap windows means boundary rows (rows at the exact max watermark timestamp) may be re-fetched. Hashing the PKs of these rows and storing them in `boundary_hashes` allows the next run to skip already-loaded rows at the boundary. The alternative — relying solely on the overlap window — would re-insert boundary rows on every run, violating idempotency for incremental tables.

#### Requirements

```
# FR7.1
THE SYSTEM SHALL store state in a local DuckDB file, separate from the data DuckDB.

# FR7.2
IF state.path is configured in feather.yaml
THEN THE SYSTEM SHALL use that path (absolute or relative to feather.yaml location).

# FR7.3
IF state.path is not configured
THEN THE SYSTEM SHALL default to {feather.yaml directory}/feather_state.duckdb.

# FR7.4
THE SYSTEM SHALL maintain five state tables:
_watermarks, _runs, _run_steps, _dq_results, _schema_snapshots.

# FR7.5
WHEN the entire pipeline step succeeds for a table AND sync is not configured
THE SYSTEM SHALL advance the watermark after successful load.

# FR7.6
WHEN the entire pipeline step succeeds for a table AND sync is configured
THE SYSTEM SHALL advance the watermark only after both load AND sync succeed.

# FR7.7
WHEN load fails
THE SYSTEM SHALL leave the watermark unchanged
AND roll back the data load transaction.

# FR7.8
WHEN load succeeds but sync fails
THE SYSTEM SHALL leave the watermark unchanged.
Loaded data remains in local DuckDB.
Next run re-extracts the same window and re-syncs.

# FR7.9
WHEN extracting an incremental table
THE SYSTEM SHALL hash PKs of rows at the max watermark timestamp
AND store them in boundary_hashes.

# FR7.10
WHEN the next incremental run encounters rows whose PK hash
matches stored boundary_hashes
THE SYSTEM SHALL skip those rows (boundary deduplication).
```

#### Acceptance Criteria

**AC-FR7.a:** Given an incremental table where load succeeds but MotherDuck sync fails, when the pipeline re-runs, then the watermark is unchanged from the previous successful value, the same data window is re-extracted, and sync is re-attempted.

**AC-FR7.b:** Given a boundary row (PK=100) at watermark timestamp `2024-06-01T10:00:00` that was loaded in run N, when run N+1 extracts with overlap and the source row is unchanged, then the row is skipped via boundary hash match. But if the source row was updated (different non-PK column values), then the row is NOT excluded (the PK hash still matches, but the system should re-load it — this AC verifies the boundary hash is PK-only, not full-row).

**AC-FR7.c:** Given a table with `retry_count: 2`, when the pipeline runs successfully, then `retry_count` is reset to 0 and `retry_after` is cleared.

### State Schema Reference

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

### FR8: Data Quality

Data quality checks are declarative — the operator specifies what to validate in YAML, not how. This keeps checks maintainable across dozens of client deployments without per-client Python code. Checks run after each load against the local DuckDB table and log results to `_dq_results`. Failures trigger alerts but do not block the pipeline — the data is still loaded, and the operator investigates at their convenience.

```yaml
quality_checks:
  not_null: [invoice_no, customer_code]
  unique: [invoice_no]
```

#### Requirements

```
# FR8.1
THE SYSTEM SHALL support declarative DQ checks configured per-table in YAML:
not_null, unique, and row_count.

# FR8.2
WHEN not_null check is configured for a column
THE SYSTEM SHALL fail if any NULLs exist in that column after load.

# FR8.3
WHEN unique check is configured for a column
THE SYSTEM SHALL fail if any duplicate values exist in that column after load.

# FR8.4
WHEN a table is loaded
THE SYSTEM SHALL always run the row_count check
AND warn if 0 rows were loaded.

# FR8.5
THE SYSTEM SHALL run DQ checks after each load,
against the local DuckDB table (not the source).

# FR8.6
THE SYSTEM SHALL log all DQ results to the _dq_results state table.

# FR8.7
WHEN a DQ check fails
THE SYSTEM SHALL trigger an email alert at [WARNING] severity (if configured)
AND continue pipeline execution (do NOT block).
```

#### Acceptance Criteria

**AC-FR8.a:** Given a table with `not_null: [invoice_no]` and 3 rows with NULL `invoice_no`, when DQ checks run, then a `fail` result is recorded in `_dq_results` with details indicating the NULL count, and the pipeline continues to the next step.

**AC-FR8.b:** Given a table with no `quality_checks` configured, when loading completes with 0 rows, then a `row_count` warn result is still recorded in `_dq_results` (row_count always runs).

### FR9: Schema Drift Detection

Schema drift detection is observability, not a gate. The pipeline always continues — but the operator is informed when source schemas change. This matters because ERP vendors and client IT teams modify schemas without notice, and silent drift causes subtle data quality issues that surface days later in dashboards. By detecting and classifying drift at extraction time, the operator can respond proactively. Load-time handling of each drift type is defined in FR4.11–FR4.14.

#### Requirements

```
# FR9.1
WHEN extracting a table
THE SYSTEM SHALL compare source schema against stored snapshot in _schema_snapshots.

# FR9.2
THE SYSTEM SHALL classify drift as: added (new column),
removed (column gone), or type_changed (data type changed).

# FR9.3
WHEN schema drift is detected
THE SYSTEM SHALL log changes in the _runs.schema_changes field (JSON)
AND trigger an email alert.

# FR9.4
WHEN drift type is "added" or "removed"
THE SYSTEM SHALL send the alert at [INFO] severity.

# FR9.5
WHEN drift type is "type_changed"
THE SYSTEM SHALL send the alert at [CRITICAL] severity
(because type changes may cause cast failures and quarantined rows per FR4.14).

# FR9.6
WHEN a table is extracted for the first time
THE SYSTEM SHALL save the schema as baseline with no drift reported.

# FR9.7
THE SYSTEM SHALL infer file source schemas from DuckDB column metadata
AND SQL Server schemas from INFORMATION_SCHEMA.COLUMNS.
```

#### Acceptance Criteria

**AC-FR9.a:** Given a table extracted for the first time, when the run completes, then `_schema_snapshots` contains the baseline schema and `_runs.schema_changes` is NULL (no drift reported).

**AC-FR9.b:** Given a source table where column `phone` was added since the last snapshot, when extraction runs, then `_runs.schema_changes` contains `{"added": ["phone"]}` and an `[INFO]` email is sent (if configured).

### FR10: Scheduling

Two scheduling modes because operators have different preferences. Some prefer OS cron — simpler, no daemon, works with existing monitoring. Others prefer APScheduler — richer scheduling (e.g., "twice daily"), no crontab editing, single process. Both modes use the same `feather run` logic underneath; the difference is only in how runs are triggered.

#### Requirements

```
# FR10.1
THE SYSTEM SHALL support two scheduling modes:
built-in (APScheduler v3.x with SQLite job store, feather schedule command)
and external (one-shot CLI via feather run --tier for OS cron).

# FR10.2
WHEN using APScheduler
THE SYSTEM SHALL configure coalesce=True, max_instances=1, replace_existing=True.

# FR10.3
WHEN resolving a table's schedule
THE SYSTEM SHALL follow the resolution chain:
table schedule → tier lookup → named schedule → cron kwargs.

# FR10.4
WHEN feather run --tier is invoked
THE SYSTEM SHALL execute a one-shot run of all tables matching that tier,
then exit (suitable for OS cron).
```

#### Acceptance Criteria

**AC-FR10.a:** Given APScheduler running with `max_instances=1` and `coalesce=True`, when a scheduled run is still in progress and the next trigger fires, then the second run is coalesced (not started concurrently).

**AC-FR10.b:** Given a table with `schedule: hot` and tier definition `hot: "twice daily"`, when schedule resolution runs, then the table resolves to the cron kwargs for "twice daily".

### FR11: CLI

Single entry point `feather`, built with typer. Every operational action is a CLI command — no Python API required for normal operation.

| Command | Purpose |
|---------|---------|
| `feather setup` | Init state DB, create schemas, apply transforms |
| `feather run` | Run all tables |
| `feather run --table X` | Run single table |
| `feather run --tier hot` | Run tables matching a tier |
| `feather status` | Show watermarks and last run status |
| `feather history [--table X]` | Show run history |
| `feather schedule` | Start APScheduler daemon |
| `feather validate` | Parse and validate config, resolve paths, print summary — no execution |

#### Requirements

```
# FR11.1
THE SYSTEM SHALL provide a CLI built with typer, entry point: feather.

# FR11.2
WHEN feather setup is invoked
THE SYSTEM SHALL initialize the state DB, create schemas, and apply all transforms.

# FR11.3
WHEN feather run is invoked with no flags
THE SYSTEM SHALL run all configured tables.

# FR11.4
WHEN feather run --table X is invoked
THE SYSTEM SHALL run only the named table.

# FR11.5
WHEN feather run --tier T is invoked
THE SYSTEM SHALL run all tables matching the named tier.

# FR11.6
WHEN feather status is invoked
THE SYSTEM SHALL display watermarks and last run status for all tables.

# FR11.7
WHEN feather history is invoked
THE SYSTEM SHALL display run history, optionally filtered by --table.

# FR11.8
WHEN feather schedule is invoked
THE SYSTEM SHALL start the APScheduler daemon.

# FR11.9
THE SYSTEM SHALL accept --config PATH on all commands (default: feather.yaml).
```

#### Acceptance Criteria

**AC-FR11.a:** Given a valid `feather.yaml` at a non-default path `/opt/client/config.yaml`, when `feather run --config /opt/client/config.yaml` is invoked, then the pipeline runs using that config file.

**AC-FR11.b:** Given 5 tables configured with 2 in tier `hot`, when `feather run --tier hot` is invoked, then only the 2 hot-tier tables are executed.

### FR12: Alerting

Alerts use SMTP email via Python stdlib `smtplib` + `email` — zero additional dependencies. This works with Gmail, any corporate SMTP relay, or transactional email services. The operator configures credentials in YAML (with env var substitution for secrets). If the `alerts` section is not configured, the pipeline runs silently with no error — alerting is fully optional.

```yaml
alerts:
  smtp_host: "smtp.gmail.com"
  smtp_port: 587
  smtp_user: "${ALERT_EMAIL_USER}"
  smtp_password: "${ALERT_EMAIL_PASSWORD}"
  alert_to: "operator@example.com"
  alert_from: "feather-etl@example.com"   # optional, defaults to smtp_user
```

> **Decision: SMTP email rather than Slack, PagerDuty, or webhook.**
> SMTP uses Python stdlib — zero dependency. Every client has email. Slack/PagerDuty require account setup, API tokens, and a dependency. Webhooks are generic but require the operator to build a receiver. Email is the lowest-friction option for the target audience (Indian SMB operators who may not use Slack). If a future client needs Slack, a webhook-to-Slack relay is trivial to add outside feather-etl.

#### Requirements

```
# FR12.1
THE SYSTEM SHALL send alerts via SMTP email
using Python stdlib smtplib + email (no additional dependency).

# FR12.2
THE SYSTEM SHALL support email configuration via the alerts section in feather.yaml:
smtp_host, smtp_port, smtp_user, smtp_password, alert_to, alert_from (optional).

# FR12.3
IF alert_from is not configured
THEN THE SYSTEM SHALL default to smtp_user as the sender address.

# FR12.4
WHEN a pipeline failure or load error occurs
THE SYSTEM SHALL send an email with [CRITICAL] subject prefix.

# FR12.5
WHEN a DQ check fails
THE SYSTEM SHALL send an email with [WARNING] subject prefix.

# FR12.6
WHEN schema drift is detected
THE SYSTEM SHALL send an email with [INFO] subject prefix
(or [CRITICAL] for type_changed per FR9.5).

# FR12.7
IF the alerts section is not configured in feather.yaml
THEN THE SYSTEM SHALL skip alerting entirely (no-op, no error).
```

#### Acceptance Criteria

**AC-FR12.a:** Given a `feather.yaml` with no `alerts` section, when a pipeline failure occurs, then no SMTP connection is attempted and the failure is only logged locally.

**AC-FR12.b:** Given a configured `alerts` section, when a DQ check fails for table `sales_invoice`, then an email is sent with subject `[WARNING] feather-etl: DQ failure — sales_invoice` (or similar format containing severity, tool name, and table).

### FR13: Retry and Error Handling

Retry state lives in `_watermarks` — the table that is already read at every run start — so there is no extra join or lookup. When a table fails, the pipeline records the failure, increments the retry counter, computes a backoff window, and moves on to the next table. Other tables are never affected by one table's failure. The backoff window prevents a persistently failing table from hammering the source system on every scheduled run.

> **Decision: linear backoff, base 15 minutes, cap 2 hours.**
> Formula: `retry_after = now + min(retry_count × 15 minutes, 120 minutes)`
> After 1 failure: wait 15 min. After 2: 30 min. After 3: 45 min. Capped at 120 min from failure 8 onwards.
> Exponential backoff was rejected — a backoff that reaches 8–16 hours defeats the purpose of a scheduled pipeline. Linear keeps the retry window predictable and within the operator's scheduling expectations. At cap, a persistent failure alerts every ~4 scheduled runs rather than going silent.

#### Requirements

```
# FR13.1
WHEN a table extraction or load fails
THE SYSTEM SHALL record the run as failure, increment retry_count,
compute retry_after using linear backoff, leave watermark unchanged,
and send a [CRITICAL] email alert (if configured).

# FR13.2
THE SYSTEM SHALL compute retry_after as:
now + min(retry_count × 15 minutes, 120 minutes).

# FR13.3
WHILE current_time < retry_after for a table
THE SYSTEM SHALL skip that table
AND record status "skipped" in _runs with error_message referencing the original failure.

# FR13.4
WHEN a table runs successfully after previous failures
THE SYSTEM SHALL reset retry_count to 0 and clear retry_after.

# FR13.5
WHEN one table fails
THE SYSTEM SHALL continue running all other configured tables
(per-table isolation).
```

#### Acceptance Criteria

**AC-FR13.a:** Given a table with 2 consecutive failures (`retry_count: 2`), when the backoff is computed, then `retry_after = now + 30 minutes` (2 × 15 min).

**AC-FR13.b:** Given a table in backoff (`retry_after` is 20 minutes from now), when `feather run` executes, then the table is skipped with status `skipped` and the error message references the original failure — while all other tables run normally.

**AC-FR13.c:** Given a table with `retry_count: 10` (past cap), when backoff is computed, then `retry_after = now + 120 minutes` (capped, not 150).

---

## 5. Non-Functional Requirements

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

**NFR3: Performance.** Targets measured on a mid-range developer laptop (Apple M-series or equivalent Intel, SSD, 16 GB RAM):
- SQL Server full extraction: ~700K rows within 5 minutes over a local network connection
- SQL Server incremental extraction: ~1,000 rows within 10 seconds
- File-based extraction (CSV/DuckDB/SQLite): files up to 500 MB within 30 seconds (DuckDB native columnar readers)
- File-based extraction (Excel via read_xlsx): files up to 50 MB within 30 seconds
- Gold table rebuild: all gold transforms complete within 60 seconds for a typical client deployment (≤20 tables, ≤5M total rows in silver)

**NFR4: Project Tooling.** Use `uv` for dependency management.

**NFR5: Python Version.** Python 3.10+.

**NFR6: Testing.** End-to-end tests use file-based sources (CSV, DuckDB files) → local DuckDB. No mocking needed for core pipeline tests. Only SQL Server and MotherDuck connectivity require mocking. Target: 80% coverage.

**NFR7: Logging.** Python stdlib `logging` — console (human-readable) + JSONL file (structured, queryable). Structured JSONL fields per log entry: `timestamp`, `level`, `table_name`, `run_id`, `step`, `message`. Log file rotates at 10 MB, retains last 5 files.

**NFR8: Security.**
- **Credentials never in plain text.** Connection strings, SMTP passwords, and MotherDuck tokens must use `${ENV_VAR}` substitution in `feather.yaml`. If a config value for a known secret field (e.g. `smtp_password`, `token`, `connection_string`) is not an env var reference, the system shall log a `[WARNING]` at startup and proceed — it does not fail, as some dev environments use plain-text values intentionally.
- **Credential redaction in logs.** Connection strings and tokens shall never appear in log output or in `_runs.error_message`. If an exception message contains a connection string (pyodbc errors often do), it shall be redacted to `[REDACTED]` before logging.
- **State DB file permissions.** On creation, `feather_state.duckdb` and `feather_data.duckdb` shall be created with `600` permissions (owner read/write only) on Unix systems. No enforcement on Windows.
- **No network listening.** feather-etl does not open any network ports. The APScheduler daemon (`feather schedule`) is a local process only.

**NFR9: Reliability.**
- A single table failure shall never crash the process — all other tables continue running (enforced by FR13.4).
- If `feather_state.duckdb` is corrupted or missing, `feather setup` shall recreate it from scratch. Watermarks are lost; the next run performs a full re-extraction for all tables. The operator is informed via a `[WARNING]` log at startup.
- `feather schedule` (APScheduler daemon) shall handle unexpected exceptions in individual job executions without terminating the scheduler process.

---

## 6. Connector Interface Design

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

## 7. Architecture

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

## 8. Feature List for Implementation

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
| F10 | CLI (basic) | `feather setup`, `feather run`, `feather status`, `feather validate` | F9 |

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

## 9. Testing Strategy

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
- Silver tables populated correctly in output DuckDB (silver-direct config, no bronze)
- Bronze tables populated correctly when configured (bronze-cache config)
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

## 10. Open Questions

1. **MotherDuck region** — Where is the instance hosted? Latency from India matters.
2. **ROWVERSION columns** — Do Icube ERP tables have SQL Server's ROWVERSION column?
3. **Excel reader** — ~~DuckDB's `st_read()` requires the `spatial` extension.~~ **Resolved:** Use DuckDB's `excel` extension (`read_xlsx()`) for `.xlsx` files — it is a core extension, ships with DuckDB, and is more efficient than the spatial extension's incidental xlsx support (which may be removed). For `.xls` files, fall back to `openpyxl` (added as 8th dependency). Constraint: `.xls` fallback produces a warning in logs; `.xlsx` is the recommended format for all new source files.

---

## 11. Deferred Ideas

See `docs/research.md` section "Ideas Evaluated and Deferred to Later Stages" for 13 deferred ideas with revisit triggers.

---

## 12. Reference

- Research: `docs/research.md` (7 agents synthesized)
- Existing POC: `~/Desktop/NonDropBoxProjects/afans-reporting-dev/`
- Source DB snapshot: `afans-reporting-dev/discovery/discovery.duckdb`
