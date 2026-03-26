# feather-etl Research Report

**Date:** 2026-03-25
**Purpose:** Research existing ETL tools and patterns to inform the design of feather-etl — a lightweight, config-driven ETL framework replacing dlt + sqlmesh + Dagster.

---

## Research Prompt

The following prompt was used across multiple research agents. It can be reused with any LLM for further research.

> I'm building a custom, lightweight Python ETL tool called **feather-etl** to replace a stack of dlt + sqlmesh + Dagster, which is overkill for my use case.
>
> **Current setup being replaced:**
> - Python dlt extracts from SQL Server → MotherDuck (DuckDB cloud)
> - SQLMesh transforms bronze → silver (column renames, filters) → gold (header+detail joins)
> - Dagster orchestrates on a schedule
> - Rill Data connects to MotherDuck for dashboards
>
> **Source system constraints:**
> - Microsoft SQL Server (ERP database, read-only — owned by ERP vendor)
> - Cannot enable CDC, Change Tracking, or modify schema
> - Can create views on the source DB
> - ~20-25 tables of interest out of ~296 total
> - Largest table ~700K rows, most tables under 50K
> - Indian client — cost-sensitive, full loads multiple times/day not acceptable
>
> **Two table categories with different strategies:**
>
> | Category | Examples | Change detection | Schedule |
> |----------|----------|-----------------|----------|
> | **Hot (transactional)** | SALESINVOICE, POSMaster, POSDetail | Incremental via timestamp column (ModifiedDate, TimeStamp) | Configurable: hourly to twice daily |
> | **Cold (master/dimension)** | CUSTOMERMASTER, EmployeeForm, InventoryGroup, INVITEM | SQL Server `CHECKSUM_AGG(BINARY_CHECKSUM(*))` — single integer check, full refresh only if changed | Configurable: daily to weekly |
>
> **Transformation requirements (simple):**
> - Column renaming (ERP column names → clean business names)
> - Filtering (e.g., `WHERE STATUS <> 1`, `WHERE extinct = 0`)
> - Flattening: JOIN header + detail + dimension tables into wide gold views
> - No complex incremental models, snapshots, or SCD logic
> - Currently expressed as plain SQL
>
> **Configuration requirements:**
> - YAML-driven, human-readable
> - Per-table: source table name, strategy (incremental/checksum), schedule, timestamp column
> - Schedules in plain English: `daily`, `hourly`, `every 2 hours`, `twice daily`, `weekly` — NOT cron syntax
> - Tier shortcuts: define `hot: twice daily`, `cold: weekly` globally, then tables just say `schedule: hot`
> - Transforms defined as SQL with column mappings
>
> **Logging & observability requirements:**
> - Know what happened in each run (rows extracted, loaded, skipped, errors)
> - Detect data quality issues (nulls, duplicates, freshness)
> - Human-readable logs + queryable run history
> - dlt does this well — want to replicate the useful parts without the framework
>
> **Target architecture:**
> - Single Python package, minimal dependencies
> - Extract: pyodbc/SQLAlchemy → SQL Server
> - Transform: plain SQL executed in DuckDB
> - Load: DuckDB Python client → MotherDuck
> - Schedule: lightweight Python scheduler (APScheduler or similar), per-table cron
> - State: watermarks, checksums, run history stored locally (DuckDB or SQLite)
>
> **What to research:**
> 1. How Singer taps, dlt, Bruin, and other ETL tools handle incremental state, watermarks, and logging — what ideas to borrow
> 2. Most efficient Python patterns for SQL Server bulk reads (pyodbc vs SQLAlchemy vs turbodbc)
> 3. DuckDB Python client patterns for bulk loading (from Arrow, pandas, or direct INSERT)
> 4. `CHECKSUM_AGG(BINARY_CHECKSUM(*))` behavior and edge cases on SQL Server
> 5. Lightweight Python scheduling (APScheduler, schedule, rocketry) — which is simplest for per-table cron with persistence
> 6. ETL run metadata and data quality: what's the minimum viable set of metrics to track per run
> 7. How to structure SQL transforms without a framework — plain `.sql` files with variable substitution?
> 8. Any other lightweight ETL tools or patterns worth borrowing from
>
> **NOT in scope:** CDC, streaming, complex DAGs, plugin systems, multi-destination support, enterprise observability.
>
> **Goal:** Identify the best patterns from existing tools and combine them into the simplest possible architecture that handles: configurable per-table extraction, simple SQL transforms, MotherDuck loading, and enough logging to debug issues.

---

## Research Areas

Four parallel research tracks were executed:

1. Singer/Meltano taps — protocol design, incremental patterns, state management
2. dlt internals — watermark tracking, `_dlt_load_id`, logging/observability, schema evolution
3. Lightweight ETL frameworks — Bruin, APScheduler, DuckDB client, pyodbc, petl, bonobo
4. ETL logging/DQ patterns — run metadata, structured logging, data quality checks, alerting

---

## Detailed Findings

### 1. Singer/Meltano Ecosystem

#### The Singer Protocol

Singer is a specification (v0.3.0) for data movement, not a framework. Taps (extractors) and targets (loaders) communicate via stdout/stdin using newline-delimited JSON:

```
tap --config config.json --state state.json | target --config target_config.json >> state.json
```

**Message types:**

| Message | Purpose |
|---------|---------|
| `SCHEMA` | Declares stream name, JSON Schema, primary key, bookmark properties |
| `RECORD` | A single data row with stream name and optional `time_extracted` |
| `STATE` | Opaque JSON checkpoint — the target passes it through after persisting data |
| `METRIC` | (Optional, stderr) Structured counters and timers for observability |

**Replication methods:**

| Method | How | State stores |
|--------|-----|-------------|
| `FULL_TABLE` | Re-extract everything | Nothing |
| `INCREMENTAL` | `WHERE key >= last_value` | Last seen value of replication key per stream |
| `LOG_BASED` | Database binary logs (CDC) | Log file + position |

**Incremental/watermark pattern:**
1. Tap receives previous state: `{"bookmarks": {"stream_name": {"replication_key_value": "2023-01-15T12:00:00"}}}`
2. Queries source with `WHERE updated_at >= last_value`
3. Emits STATE messages with highest seen bookmark
4. Target writes data, passes STATE through to stdout
5. Runner captures last STATE line

**`is_sorted` concept (from Meltano SDK):** If records come sorted by replication key, state can be emitted mid-stream (resumable). If NOT sorted, tap must complete fully before emitting final state — crash means restart from scratch.

#### What Meltano Adds

Meltano wraps Singer with: plugin management (isolated virtualenvs), YAML config, automatic catalog/stream selection, state management (system database), and orchestration integration.

#### tap-mssql

The official Singer tap for SQL Server is written in **Clojure** (not Python), requiring OpenJDK. Python alternatives exist (PipelineWise variant, BuzzCutNorman's Meltano SDK variant) but quality varies.

#### Strengths

- Dead-simple protocol — JSON over stdout/stdin, any language can implement
- Unix philosophy — standalone processes, compose with pipes
- Large ecosystem — 200+ extractors, 50+ loaders
- Elegant bookmark/watermark pattern — state semantics opaque to framework
- Schema-aware — JSON Schema per stream for automatic DDL

#### Weaknesses

- **JSON serialization bottleneck** — every record serialized/deserialized. PipelineWise created FastSync to bypass this for bulk loads.
- **Single-threaded** — no parallelism within a tap-target pair
- **Inconsistent tap quality** — community-maintained, variable quality, multiple competing variants
- **All-or-nothing state** — if crash mid-stream and not sorted, lose all progress
- **No backpressure** — tap produces as fast as it can
- **Logging is an afterthought** — METRIC messages optional and inconsistently implemented

#### Patterns Worth Borrowing

1. **Bookmark/watermark as opaque state** — each extractor owns its bookmark format, framework just stores/retrieves it
2. **Schema-first messaging** — emit schema before records for automatic DDL
3. **Discovery mode** — extractors self-describe available tables/schemas
4. **METRIC messages** — counter/timer with tags for lightweight observability
5. **PipelineWise FastSync** — bypass row-by-row for bulk/initial loads

---

### 2. dlt Internals

#### Incremental State / Watermark Tracking

The cursor value lives in the pipeline state dict at:
```
sources.<schema_name>.resources.<resource_name>.incremental.<cursor_path>
```

State entry (`IncrementalColumnState`) contains:
- `initial_value` — preserved across runs
- `last_value` — updated after each successful extraction
- `unique_hashes` — row hashes at the `last_value` boundary for deduplication

**Per-run flow:**
1. Read `last_value` from state
2. Pass to resource function for server-side filtering (`WHERE col > last_value`)
3. Client-side filter as safety net
4. Deduplicate boundary rows using stored hashes
5. Update `last_value` in state

**Boundary deduplication detail:** Uses `digest128(json.dumps(primary_key_values, sort_keys=True))`. On next run, rows whose hash matches `start_unique_hashes` are dropped. Critical for timestamp-based incrementals where multiple rows share the same timestamp.

**`lag` parameter:** Subtracts N seconds from `last_value` to re-fetch late-arriving data. Stored `last_value` is always un-lagged.

#### `sql_table` Source Under the Hood

- `sql_database()` uses `sqlalchemy.MetaData.reflect()` to discover tables
- `sql_table()` calls `table_rows()` which uses `sa.select(table)` → `connection.execute()` → `fetchmany(50000)`
- Backend system supports dict output (default), PyArrow, Pandas, or ConnectorX
- Incremental adds a WHERE clause via `query_adapter_callback`

#### `_dlt_load_id`

Generated by `create_load_id()` — returns **current unix timestamp as a string** (e.g., `"1234562350.98417"`). Created at start of extract phase.

**Where it appears:**
- Every root data table row as `_dlt_load_id` column
- `_dlt_loads` system table with `status=0` when load fully completes

**The `_dlt_loads` table:**

| Column | Type | Description |
|--------|------|-------------|
| `load_id` | text | Unix timestamp string |
| `schema_name` | text | Schema loaded |
| `status` | bigint | 0 = completed |
| `inserted_at` | timestamp | When load completed |
| `schema_version_hash` | text | Hash of schema at load time |

**Atomicity mechanism:** Rows with a `_dlt_load_id` NOT in `_dlt_loads` are from incomplete/failed loads — can be filtered or deleted. This is how dlt achieves atomicity without distributed transactions.

#### Schema Evolution

- Schema is a versioned, content-hashed artifact (SHA3-256)
- Stored in `_dlt_version` table at the destination
- New columns auto-added, type changes create variant columns, removed columns become null
- Schema contracts allow controlling evolution: `freeze`, `evolve`, `discard_value`, `discard_row`

#### State Management

**Dual-write:** State written to both local filesystem (`~/.dlt/pipelines/<name>/state.json`) AND destination (`_dlt_pipeline_state` table).

**Atomic commit:** State is written as part of the load package — committed atomically with the data (same `_dlt_load_id`).

**Conflict resolution:** Remote state applied only if `_state_version >= local_version`.

**On failure:** Backed-up state is restored (rollback).

#### Logging & Observability

**`load_info`** (returned by `pipeline.run()`):
- Load packages with job statuses, file sizes, error messages
- `schema_update` — new tables/columns created
- `has_failed_jobs` boolean

**`pipeline.last_trace`:**
- Timing for extract, normalize, load steps
- Row counts per table (`last_normalize_info.row_counts`)
- Config values with provenance

**Self-loading trace:** `pipeline.run([pipeline.last_trace], table_name="_trace")` loads trace data into destination — queryable for dashboards.

**Logging config:** Python `logging` under `"dlt"` logger. Supports JSON format.

#### Patterns Worth Borrowing

1. **Load ID on every row** — simple lineage + atomicity mechanism
2. **`_dlt_loads` as commit log** — rows without matching load are incomplete
3. **Atomic state commits** — state saved with data, not separately
4. **Boundary deduplication** — hash primary keys at cursor boundary
5. **Schema content hashing** — detect drift automatically
6. **Self-loading trace** — ETL metadata queryable alongside data

---

### 3. Lightweight ETL Frameworks

#### Bruin

Go-based CLI combining ingestion, SQL/Python transforms, and DQ checks. Assets are SQL files with YAML config in comments. Uses **ingestr** for ingestion — a single CLI command for database-to-database copy:

```bash
ingestr ingest \
    --source-uri 'mssql://user:pass@host/db' \
    --source-table 'dbo.customers' \
    --dest-uri 'duckdb:///local.db' \
    --dest-table 'raw.customers'
```

ingestr supports SQL Server → DuckDB/MotherDuck with append, merge, and delete+insert modes. Uses dlt internally.

#### SQLMesh vs Plain SQL

| Feature | Plain SQL | SQLMesh |
|---------|-----------|---------|
| Incremental models | Manual `WHERE >= MAX()` | Auto-tracks state, fills gaps |
| Change detection | Run everything | Parses SQL, runs only changed models |
| Backfill | Manual scripting | Built-in batched backfill |
| Column lineage | None | Automatic via SQL parsing |
| Unit testing | External | Built-in, no database needed |

**Verdict:** SQLMesh is worthwhile for 5+ SQL models with dependencies. For <5 simple transforms (renames + joins), plain SQL with manual watermarks is less machinery.

#### APScheduler v3.x

- **Triggers:** `date`, `interval`, `cron`
- **Job stores:** Memory (volatile) or SQLAlchemy (persistent across restarts)
- **Key settings:** `coalesce=True` (missed runs → run once), `max_instances=1` (no overlap)
- **Persistence:** SQLite-backed via SQLAlchemy job store

```python
scheduler = BackgroundScheduler(
    jobstores={'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')},
    job_defaults={'coalesce': True, 'max_instances': 1}
)
```

APScheduler v4.0 exists but is pre-release. Stick with v3.x.

#### DuckDB Python Client + MotherDuck

**Bulk loading speed ranking:**

1. **PyArrow tables (fastest — zero-copy):**
   ```python
   arrow_table = pa.table({'id': [1,2,3], 'name': ['a','b','c']})
   con.sql('CREATE TABLE my_table AS SELECT * FROM arrow_table')
   ```

2. **Pandas with Arrow backend:**
   ```python
   df = pd.DataFrame(data).convert_dtypes(dtype_backend='pyarrow')
   con.sql('CREATE TABLE t AS SELECT * FROM df')
   ```

3. **Parquet intermediate file:**
   ```python
   con.execute("COPY my_table FROM 'data.parquet' (FORMAT 'parquet')")
   ```

4. **executemany (avoid for >500 rows)**

**ATTACH for hybrid local/cloud:** `duckdb.connect('md:')` connects to MotherDuck. Can query across local and remote databases in the same SQL.

**Batching:** Load in ~100K row chunks.

#### pyodbc — SQL Server Reading

**Optimal pattern:**
```python
cursor.execute("SELECT * FROM dbo.table WHERE updated_at > ?", [watermark])
cursor.arraysize = 10000
while True:
    rows = cursor.fetchmany(10000)
    if not rows:
        break
    # Convert to PyArrow batch
```

**CHECKSUM_AGG for change detection:**
```sql
SELECT CHECKSUM_AGG(CAST(BINARY_CHECKSUM(*) AS INT)) FROM dbo.customers
```

- Order-independent (good for comparing sets)
- **Collision caveat:** Small but real probability of false negatives (two different datasets → same checksum). Microsoft recommends HASHBYTES for guaranteed detection.
- For our use case (deciding whether to trigger a full refresh of a small table), a rare false positive (unnecessary reload) is acceptable. A false negative (missed change) is the risk, but extremely unlikely.

#### Other Tools Evaluated

| Tool | Verdict |
|------|---------|
| **petl** | Pure Python table transforms. No parallelism, no scheduling. Too limited. |
| **Bonobo** | Abandoned (last commit 2020). Skip. |
| **Hamilton** | Function-based DAGs. Interesting for ML, overkill for SQL ETL. |
| **ingestr** | Uses dlt internally. Could be useful as a CLI but adds a dependency we're trying to avoid. |

---

### 4. ETL Logging & Data Quality

#### What dlt Tracks Per Run

**Internal tables at destination:**

| Table | Purpose |
|-------|---------|
| `_dlt_loads` | Every completed load with load_id, status, timestamp, schema hash |
| `_dlt_version` | Schema evolution history |
| `_dlt_pipeline_state` | Serialized pipeline state including cursors |

Every data row gets `_dlt_load_id` linking to `_dlt_loads` — built-in lineage.

**Runtime trace captures:** timing per step (extract/normalize/load), row counts per table, config provenance, schema changes, job statuses with error messages.

#### Minimum Viable Run Metadata

| Field | Purpose |
|-------|---------|
| `run_id` | `{pipeline}_{table}_{timestamp}` — correlates across logs |
| `started_at`, `ended_at`, `duration_sec` | Timing |
| `status` | success / failure / skipped |
| `rows_extracted`, `rows_loaded` | Volume |
| `error_message` | Null on success |
| `watermark_before`, `watermark_after` | State diff |
| `checksum_changed` | Boolean for cold tables |
| `schema_changes` | New/modified columns |

#### Data Quality Checks (Tiered)

**Tier 1 — Always implement (catches 80% of issues):**

| Check | What it catches | Implementation |
|-------|----------------|----------------|
| Row count validation | Empty loads, duplicates, source outages | Compare vs historical baseline |
| Null checks on required columns | Missing critical data | `SELECT COUNT(*) WHERE col IS NULL` |
| Freshness checks | Stale data, failed upstream | `MAX(timestamp_col)` vs expected cadence |

**Tier 2 — Implement next:**

| Check | What it catches |
|-------|----------------|
| Uniqueness / duplicate detection | Duplicate loads, merge key issues |
| Volume anomaly detection | Unusual spikes or drops |

**Tier 3 — Add when needed:** Referential integrity, numeric distribution, string pattern validation.

#### Structured Logging

**Two complementary approaches:**

1. **JSON-structured text logs** — Python `logging` with JSON formatter → NDJSON file. Can be loaded into DuckDB for ad-hoc analysis: `SELECT * FROM read_ndjson_auto('pipeline.log')`

2. **Run history table in DuckDB** — structured metadata per run, queryable with SQL.

#### Run History Schema

```sql
CREATE TABLE IF NOT EXISTS _runs (
    run_id          VARCHAR PRIMARY KEY,
    table_name      VARCHAR NOT NULL,
    started_at      TIMESTAMP NOT NULL,
    ended_at        TIMESTAMP,
    duration_sec    DOUBLE,
    status          VARCHAR,       -- success, failure, skipped
    rows_extracted  INTEGER,
    rows_loaded     INTEGER,
    error_message   VARCHAR,
    watermark       JSON,          -- {"before": "...", "after": "..."}
    schema_changes  JSON
);

-- Granular per-step logging within a run (extract, load, transform, dq)
CREATE TABLE IF NOT EXISTS _run_steps (
    run_id          VARCHAR NOT NULL,
    step            VARCHAR NOT NULL,  -- extract, load, transform, dq_check
    message         VARCHAR,
    started_at      TIMESTAMP,
    ended_at        TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES _runs(run_id)
);
```

#### Alerting

Simple three-tier approach:

| Severity | Trigger | Channel |
|----------|---------|---------|
| Critical | Pipeline failure, zero rows | Slack webhook |
| Warning | Row count anomaly, DQ failure, schema change | Slack or log |
| Info | Successful completion | Log only |

Slack webhook is the simplest — one HTTP POST, no extra infra.

---

## Summary: Design Decisions for feather-etl

### Architecture

```
SQL Server (pyodbc)
    │
    ├── Hot tables ──→ incremental extract (timestamp watermark) ──→ PyArrow
    │                                                                   │
    └── Cold tables ──→ CHECKSUM_AGG check ──→ full refresh if changed ─┘
                                                                        │
                                                              DuckDB (zero-copy)
                                                                        │
                                                         Transform (plain SQL)
                                                                        │
                                                              MotherDuck (ATTACH)
                                                                        │
                                                                   Rill Data
```

### Extraction — pyodbc + PyArrow

Direct pyodbc reads with `fetchmany(10000)` → PyArrow RecordBatch → zero-copy into DuckDB. No framework overhead.

### Change Detection — Two Strategies

- **Timestamp watermark** for hot tables: `WHERE modified_at > last_value ORDER BY modified_at` (ordering ensures deterministic extraction and simpler boundary dedup), with boundary deduplication via primary key hashing (borrowed from dlt)
- **CHECKSUM + rowcount** for cold tables: `SELECT COUNT(*), CHECKSUM_AGG(BINARY_CHECKSUM(*))` — compare both values against last known state. Using both is safer than checksum alone (catches edge cases where different data produces same checksum but different row count). Full refresh only if either changed.

### Transforms — Plain SQL

`.sql` files with variable substitution, executed via DuckDB Python client. No SQLMesh.

### Scheduling — APScheduler v3.x

Per-table cron with human-readable schedule presets. SQLite-backed job store for persistence. `coalesce=True`, `max_instances=1`.

### State — Local DuckDB

Tables for watermarks, run history, and DQ results. Load ID on every data row for lineage.

### Logging — Structured JSON + DuckDB

Python logging with JSON formatter to NDJSON file. Run history in DuckDB table. Slack webhook for critical alerts.

### Config — YAML

Human-readable, per-table configuration with schedule presets (`hot`, `cold`, `daily`, `weekly`).

### Dependencies (6 total)

| Package | Purpose |
|---------|---------|
| `pyodbc` | SQL Server connectivity |
| `pyarrow` | Zero-copy data transfer |
| `duckdb` | Local processing + MotherDuck |
| `apscheduler` | Per-table scheduling |
| `pyyaml` | Config parsing |
| `requests` | Slack alerts |

### Patterns Borrowed

| From | Pattern |
|------|---------|
| dlt | Load ID on every row, `_loads` commit log, atomic state, boundary dedup |
| Singer | Opaque bookmarks per stream, discovery mode, METRIC counters |
| PipelineWise | FastSync (bulk Arrow path for full refreshes) |
| Bruin | Config-in-comments inspiration for SQL files |

### Explicitly NOT Included

CDC, streaming, complex DAGs, plugin systems, multi-destination support, enterprise observability, schema evolution with variant columns, nested data flattening.

---

## Addendum: Cross-Agent Refinements

Additional research from a second agent surfaced these refinements (incorporated into decisions above):

1. **CHECKSUM + rowcount together** — Using `COUNT(*)` alongside `CHECKSUM_AGG` is safer than checksum alone. Different data can produce the same checksum but rarely the same count too.

2. **ORDER BY on incremental queries** — `ORDER BY ModifiedDate` ensures deterministic extraction order, making boundary deduplication simpler and state more predictable.

3. **`_run_steps` sub-table** — Per-step logging within a run (extract started, load completed, etc.) gives granular debugging without cluttering the main `_runs` table.

4. **turbodbc as future optimization** — Can fetch Arrow directly via `cursor.fetchallarrow()`, eliminating manual conversion. Start with pyodbc for reliability, upgrade path exists.

5. **Single state file** — Both agents agreed on DuckDB for state, but differed on file count. Decision: single `feather_state.duckdb` file containing all state tables (`_watermarks`, `_runs`, `_run_steps`, `_dq_results`). Fewer moving parts, can join across tables in one query.

---

## Addendum 2: Third Agent Refinements

A third research agent provided additional depth. Key new ideas incorporated:

### Overlap Window for Incremental Extraction

Configurable lookback (e.g., 2 minutes) subtracted from the watermark to catch late-committing transactions. dlt calls this `lag`. Critical for SQL Server where a transaction may commit with a `ModifiedDate` slightly behind the extraction cursor.

```yaml
defaults:
  overlap_window_minutes: 2  # look back 2 min from watermark
```

Effective query becomes: `WHERE ModifiedDate > (watermark - 2 minutes)`. The stored watermark is always the true max value — the overlap is applied dynamically at query time. Combined with boundary deduplication, this prevents both missed rows and duplicate rows.

### Views for Transforms (Major Simplification)

If silver/gold transforms are DuckDB **views** (not materialized tables), we eliminate the need to "run" transforms entirely. The flow becomes:

1. Extract → Load bronze tables (the only active step)
2. Silver/gold views are passive — always reflect latest bronze data
3. Rill Data queries views → views read fresh bronze → always up to date

Reserve `CREATE TABLE AS` only if Rill dashboard performance requires materialization (unlikely at current volumes). This removes an entire pipeline step and the need for transform orchestration/ordering.

### Source-Side Filtering

`filter` field in table config, applied at extraction time:

```yaml
tables:
  - source_table: dbo.CUSTOMERMASTER
    strategy: checksum
    filter: "WHERE extinct = 0"
```

Reduces data pulled from source, applied in the SELECT query alongside any incremental WHERE clause.

### Declarative Data Quality in YAML

Per-table quality checks declared alongside table config:

```yaml
quality_checks:
  not_null: [InvoiceNo, CustomerCode, InvoiceDate]
  unique: [InvoiceNo]
```

Translates to SQL queries at runtime (`SELECT COUNT(*) WHERE col IS NULL`, `SELECT pk, COUNT(*) ... HAVING COUNT(*) > 1`). Failures log warnings, don't block the pipeline.

### Swap Pattern for Cold Table Loads

More atomic than truncate + insert:

```sql
CREATE OR REPLACE TABLE bronze.customer_master_new AS SELECT * FROM staging_data;
DROP TABLE IF EXISTS bronze.customer_master;
ALTER TABLE bronze.customer_master_new RENAME TO customer_master;
```

Ensures the table is never empty during a refresh — queries against the old table continue to work until the swap completes.

### Additional Run Metrics

Added `rows_skipped` (filtered/deduplicated rows) and split timing into `extract_duration_ms` and `load_duration_ms` for finer diagnostics.

### mssql-python (Future Watch)

Microsoft's new first-party Python driver for SQL Server, GA since November 2025. Built-in connection pooling, DB-API 2.0 compliance, ~8.6x faster inserts vs pyodbc. BCP support on roadmap. Lacks Arrow integration and ecosystem maturity currently. Revisit in 6 months as potential pyodbc replacement.

### XOR Collision Risk Quantified

Lab analysis showed ~25% of changes undetected by CHECKSUM_AGG alone (for consecutive primes). Real ERP data would have lower collision rates, but this reinforces the decision to combine CHECKSUM + rowcount, plus periodic forced full refreshes as a safety net.

### Quality Checks as SQL Audits (from SQLMesh)

Express data quality rules as SQL queries that should return zero rows:

```sql
-- audits/no_null_invoice_numbers.sql
SELECT * FROM silver.sales_invoice WHERE invoice_number IS NULL
```

If the query returns rows, the audit fails. Simple, declarative, testable.

---

## Addendum 3: Fourth Agent Refinements

### Local DuckDB Staging Before MotherDuck (Architectural Choice)

A fourth agent proposed a different loading topology:

```
SQL Server → pyodbc → Arrow → Local DuckDB (bronze) → Transform locally → ATTACH + INSERT to MotherDuck
```

vs our original design:

```
SQL Server → pyodbc → Arrow → MotherDuck directly
```

**Tradeoffs:**

| Aspect | Direct to MotherDuck | Local staging first |
|--------|---------------------|-------------------|
| **Transform cost** | Runs on MotherDuck (billable compute) | Runs locally (free) |
| **Network** | Raw data goes over wire | Only transformed results go over wire |
| **Latency** | One hop | Two hops but transforms are local |
| **Complexity** | Simpler | Extra local DB to manage |
| **Resilience** | Load failure = re-extract from source | Local copy survives MotherDuck outages |

**Recommendation:** Given cost sensitivity and the fact that transforms are just views/renames, local staging may not save much — views are lazy and don't consume compute until queried. However, if transforms become materialized tables or if raw data volume grows, local staging becomes the better pattern. **Design the code to support both** — the DuckDB ATTACH pattern makes switching trivial.

### CLI Interface

Add `typer` (or `click`) for interactive use:

```bash
feather-etl run                          # Run all due tables
feather-etl run --table SALESINVOICE     # Run one table manually
feather-etl history                      # Show recent run history
feather-etl history --table CUSTOMERMASTER  # Filter to one table
feather-etl status                       # Show watermarks and next scheduled runs
feather-etl check                        # Validate config
```

Adds one dependency (`typer`) but provides clean UX for debugging and manual triggers. Worth including from v1.

### Column Map in YAML (Alternative to SQL for Simple Renames)

For tables that only need column renames (no joins, no filters), a YAML column map can auto-generate SQL:

```yaml
tables:
  - source_table: dbo.SALESINVOICE
    column_map:
      InvoiceID: invoice_id
      InvoiceDate: invoice_date
      CustomerCode: customer_code
```

Generates: `SELECT InvoiceID AS invoice_id, InvoiceDate AS invoice_date, ...`

For tables needing JOINs or complex logic, fall back to `.sql` files. Both approaches coexist — column_map for simple cases, transform_sql for complex ones.

### `freshness_max_ts` in Run Metadata

Store `MAX(timestamp_column)` from loaded data in each run record. Enables freshness alerts: "SALESINVOICE data is 6 hours old, expected freshness is 2 hours." Different from the watermark (which tracks extraction cursor) — this tracks the actual business data recency.

### Pre-Compute CHECKSUM in SQL Server View

Since views can be created on the source DB, use **explicit column lists** (not `*`):

```sql
CREATE VIEW dbo.vw_CustomerMaster_Checksum AS
SELECT
  COUNT(*) AS row_count,
  CHECKSUM_AGG(BINARY_CHECKSUM(CustomerCode, CustomerName, SalesType, City, State)) AS tbl_checksum
FROM dbo.CUSTOMERMASTER
```

**Why explicit columns:** `BINARY_CHECKSUM(*)` depends on column ordinal position. If the ERP vendor adds a column at the end (common in ERP upgrades), the checksum changes even when actual business data is identical — triggering unnecessary full refreshes. Listing columns explicitly makes checksums stable across schema changes.

### Updated Dependency List (7 total)

| Package | Purpose |
|---------|---------|
| `pyodbc` | SQL Server connectivity |
| `pyarrow` | Zero-copy data transfer |
| `duckdb` | Local processing + MotherDuck |
| `apscheduler` | Per-table scheduling |
| `pyyaml` | Config parsing |
| `requests` | Slack alerts |
| `typer` | CLI interface |

---

## Addendum 4: Fifth Agent Refinements

### Transaction Safety (Atomicity)

Wrap Load + State Update in a single DuckDB transaction:

```python
con.begin()
try:
    con.execute("INSERT INTO bronze.sales_invoice SELECT * FROM staging")
    con.execute("UPDATE _watermarks SET last_value = ? WHERE table_name = ?", [new_watermark, table_name])
    con.execute("INSERT INTO _runs VALUES (...)")
    con.commit()
except:
    con.rollback()  # watermark stays unchanged, next run retries
    raise
```

If the load fails, watermark is never updated — next run retries from the same point. This is how dlt achieves atomicity without distributed transactions.

### Retry Logic with Backoff

Skip a table for N minutes after X consecutive failures. Prevents a broken table from dominating scheduler cycles:

```python
# In state table
retry_count INTEGER DEFAULT 0,
retry_after TIMESTAMP  -- NULL means "run whenever scheduled"
```

Logic: if `retry_count >= 3`, set `retry_after = now + 1 hour`. Reset on success. Other tables continue running normally.

### Timezone Normalization

SQL Server ODBC driver can return datetimes without timezone info. ERP data is likely IST. Normalize immediately on extraction:

```python
# After fetching, ensure timestamps are timezone-aware
# Store as UTC in DuckDB, convert for display
```

This avoids subtle bugs where watermark comparisons fail because one side is naive and the other is tz-aware.

### `extracts/` Directory (Optional Source Queries)

For tables where you want to control exactly what's pulled from SQL Server (specific columns, source-side JOINs, pre-filtering):

```
extracts/
  sales_invoice.sql    # Custom SELECT with specific columns
  customer_master.sql  # May include JOINs to lookup tables at source
```

If no extract SQL exists for a table, default to `SELECT * FROM source_table`. This separates "what to pull" from "how to transform."

### Explicit Column Lists in Checksum Views

`BINARY_CHECKSUM(*)` depends on column ordinal position — if the ERP adds a column, the checksum changes even when data hasn't. Use explicit column lists in views (already incorporated into Pre-Compute CHECKSUM section above).

### Dissenting View: SQLite for State

This agent argued for SQLite state (decouples control plane from data plane). **Decision: keep DuckDB for state.** Since we're already using local DuckDB for data staging, state in the same file is simpler (one less file, can join state with data). SQLite is built-in but adds file management overhead. If state needs to be independent in the future, migration is trivial.

### Dissenting View: pandas as Dependency

Agent suggested `pandas.read_sql` for extraction convenience. **Decision: skip pandas.** pyodbc → Arrow directly avoids pandas' large dependency tree. Keeps the package lightweight per project goals.

---

## Addendum 5: Sixth Agent — Deep Architectural Research

A final deep-dive research agent provided academic-level analysis. Key new findings:

### ADBC (Arrow Database Connectivity)

A protocol standard for Arrow-native database extraction — zero-copy from SQL Server directly to Arrow buffers. This is the ideal extraction path since DuckDB uses Arrow as its internal data interchange format.

**Driver maturity status (as of March 2026):** ADBC drivers exist for PostgreSQL and SQLite with production readiness. SQL Server ADBC driver status needs verification — if mature, it would be the preferred extraction path over pyodbc + manual Arrow conversion.

**Decision:** Start with pyodbc (proven). Investigate ADBC SQL Server driver maturity as a potential upgrade path alongside mssql-python and turbodbc.

### ROWVERSION — SQL Server's Built-in Change Tracker

`ROWVERSION` (formerly `TIMESTAMP` data type) is a monotonically increasing binary value that SQL Server **automatically updates** on every row insert or modify. Unlike application-level `ModifiedDate` columns (which depend on the ERP application to update them correctly), `ROWVERSION` is engine-level and immune to application bugs.

**Open question:** Do any Icube ERP tables have a `ROWVERSION` column? If so, it's the most reliable incremental watermark available — better than `ModifiedDate`.

### Quarantine Pattern for Data Quality

Instead of failing the entire pipeline when rows violate quality constraints, route bad rows to a separate quarantine table:

```sql
-- After loading, quarantine bad rows
INSERT INTO _quarantine.sales_invoice
SELECT * FROM bronze.sales_invoice
WHERE invoice_no IS NULL OR customer_code IS NULL;

DELETE FROM bronze.sales_invoice
WHERE invoice_no IS NULL OR customer_code IS NULL;
```

Valid data continues flowing to Rill dashboards. Engineers investigate quarantined rows separately. Better than all-or-nothing failure.

### BINARY_CHECKSUM Deeper Edge Cases

More specific limitations than previously documented:

| Limitation | Impact |
|-----------|--------|
| `nvarchar(max)` — only first 255 characters checksummed | Changes beyond char 255 invisible |
| Strings > 25,999 characters — function stops providing unique values | Effectively useless for large text |
| `text`, `ntext`, `image`, `xml`, `cursor` columns silently ignored | Modifications to these types undetected |
| `BINARY_CHECKSUM(2.1) = BINARY_CHECKSUM(-2.1)` | Sign changes invisible |

**Impact on feather-etl:** ERP master tables (CUSTOMERMASTER, EmployeeForm) are unlikely to have nvarchar(max) or xml columns, so these edge cases are mostly academic. But worth documenting for when feather-etl is used with other source systems.

### Two-Tier Checksum Strategy

Use cheap BINARY_CHECKSUM for frequent polling. If it detects a change, optionally escalate to HASHBYTES (SHA-256) for row-level diffing to identify exactly which rows changed. For feather-etl's current use case (full refresh on change), the escalation isn't needed — but it's a valid optimization for large tables where you want to load only changed rows.

### mssql-python 1.4.0 — BCP Support (March 2026)

The March 2026 release added Bulk Copy (BCP) support for native-speed bulk extraction and loading. This makes mssql-python a serious contender to replace pyodbc:

| Feature | pyodbc | mssql-python 1.4.0 |
|---------|--------|-------------------|
| Batch fetch speed | Baseline | 3.6x faster |
| Connection scaling | Baseline | 16.5x faster |
| Bulk copy (BCP) | Not available | Native speed |
| External driver needed | Yes (ODBC Driver) | No (bundled) |
| Arrow support | Manual conversion | Not yet (coming) |

**Decision:** Monitor mssql-python. Once Arrow support lands, it becomes the clear winner. For now, pyodbc remains the safer choice.

### DuckDB Row Group Alignment

DuckDB operates in row groups of ~122,800 records. Aligning batch sizes to multiples of this improves parallel insert performance. For extraction chunks, use ~120,000 rows per batch instead of arbitrary 10,000 or 100,000.

### MotherDuck Connection Caching

MotherDuck caches database-global context for 15 minutes after connection close. Reuse a single connection across all table loads in a run — avoid reconnecting per table. Use `.cursor()` to create copies that reference the existing database instance.

### Partition-Based Overwrites for Idempotency

For incremental tables, clear the target date partition within a transaction before loading:

```python
con.begin()
con.execute("DELETE FROM bronze.sales_invoice WHERE load_date = ?", [today])
con.execute("INSERT INTO bronze.sales_invoice SELECT * FROM staging")
con.commit()
```

Ensures reruns of the same date produce identical results — no duplicates.

### Ideas Evaluated and Deferred to Later Stages

The following ideas were evaluated during research and intentionally deferred for v1. Each includes the reasoning and a trigger condition for when to revisit.

#### 1. Ibis Query Compiler

**What:** Python DataFrame expressions compiled to SQL for any backend (DuckDB, Postgres, BigQuery). Write once, run anywhere.

**Why deferred:** Transforms are 5-10 line SQL statements (renames, filters, JOINs). Ibis adds a translation layer that makes debugging harder — you'd debug Ibis's generated SQL, not yours. Adds a dependency for a problem we don't have.

**Revisit when:** feather-etl needs to support multiple warehouse backends beyond DuckDB/MotherDuck, or transforms become complex enough that Python expressions are cleaner than SQL.

#### 2. Hamilton Framework

**What:** Each Python function is a transformation step with dependencies declared via parameter names. Automatic lineage graphs, easy unit testing, modular design.

**Why deferred:** Shines with 20+ Python transformation functions with complex dependencies. We have ~5 SQL transforms that are `CREATE VIEW` statements. Would wrap SQL execution in Python functions — indirection without value.

**Revisit when:** Transforms grow beyond SQL into Python-based data enrichment, ML feature engineering, or the transform count exceeds ~15 with complex inter-dependencies.

#### 3. Rocketry Scheduler

**What:** Condition-based scheduling. Tasks run when a logical condition evaluates to True, composable with AND/OR/NOT. Example: "Run this sync daily, but only if row count has changed." Natural language DSL: `@app.task("daily after 09:00")`.

**Why deferred:** APScheduler is simpler, more mature, battle-tested. Time-based triggers are sufficient. Condition-based logic (e.g., "only run if checksum changed") is handled inside the extraction code instead of the scheduling layer.

**Revisit when:** Scheduling logic becomes more sophisticated — e.g., conditional dependencies between tables ("run gold transforms only after all silver tables have loaded"), or when non-technical operators need to understand schedules.

#### 4. SCD Type 2 (Slowly Changing Dimensions)

**What:** Tracks historical changes to dimension tables. Instead of overwriting a customer's old address, creates a new row with `valid_from`/`valid_to` dates, preserving full history.

**Why deferred:** Cold tables do full refreshes by design — current state only, history overwritten. SCD2 adds significant complexity: surrogate keys, effective dating, current-record flags, merge logic.

**Revisit when:** Client asks "what was this customer's address when this invoice was created 6 months ago?" Flag this early — once they realize historical dimension tracking is possible, they may want it. Easier to add from the start than retrofit missing history.

#### 5. Jinja Macro-Dispatcher Model

**What:** Define reusable SQL blocks as Jinja macros and dispatch from a central file. Claims 70% line reduction vs hard-coded scripts.

**Why deferred:** ~10 SQL transforms, each 5-15 lines. A macro system adds abstraction for something that doesn't need abstracting. Standard Jinja2 variable substitution (`{{ source_table }}`) is enough.

**Revisit when:** 3+ clients with similar ERP structures. Macros for common patterns (rename columns, join header+detail, filter deleted records) would reduce duplication across client configs.

#### 6. mssql-python as Primary Driver (instead of pyodbc)

**What:** Microsoft's new first-party Python driver for SQL Server. GA since late 2025, BCP support added March 2026. 3.6x faster fetching, 16.5x faster connection scaling, zero external driver dependencies.

**Why deferred:** Newer, less community documentation, fewer battle-tested edge cases. pyodbc has decades of production usage. At current data volumes (max 700K rows), extraction is network-bound, not driver-bound.

**Revisit when:** Extraction speed becomes a bottleneck, deploying to environments where ODBC driver installation is painful, or once mssql-python adds Arrow support (then it becomes the clear winner). Design the extractor behind an interface so the driver is swappable.

#### 7. ADBC (Arrow Database Connectivity) as Primary Wire Transport

**What:** Protocol standard for Arrow-native database extraction. Zero-copy from SQL Server directly to Arrow buffers. DuckDB uses Arrow internally, so this gives zero-copy end to end.

**Why deferred:** ADBC driver maturity for SQL Server needs verification. PostgreSQL and SQLite ADBC drivers are production-ready, but SQL Server support status is unclear. If mature, it would eliminate the pyodbc → manual Arrow conversion step entirely.

**Revisit when:** ADBC SQL Server driver reaches production readiness. At that point, the extraction pipeline simplifies to: ADBC → Arrow → DuckDB (zero-copy, no pyodbc, no manual conversion).

#### 8. Schema Drift Detection (Fifth Observability Pillar)

**What:** Proactively compare source column count/types against a stored snapshot. Alert when the ERP vendor adds, removes, or changes columns during an upgrade.

**Why deferred:** Not skipped — this should be included in v1. Adding here as a reminder to implement it. One SQL query against SQL Server's `INFORMATION_SCHEMA.COLUMNS` compared to stored expectations. Low effort, high value.

**Status:** **INCLUDE in v1** — promoted from deferred to active.

#### 9. @@DBTS System Function

**What:** SQL Server's `@@DBTS` returns the current database-wide last-used ROWVERSION value. Quick "has anything changed in the entire database?" check.

**Why deferred:** Only useful if tables have ROWVERSION columns. Unknown whether Icube ERP tables have ROWVERSION. Narrow use case.

**Revisit when:** Confirmed that Icube ERP tables have ROWVERSION columns. If they do, ROWVERSION is the most reliable incremental watermark — better than application-level ModifiedDate.

#### 10. HASHBYTES Escalation (Two-Tier Checksum)

**What:** Use cheap BINARY_CHECKSUM for frequent polling. If it detects a change, escalate to expensive HASHBYTES (SHA-256) for row-level diffing to find exactly which rows changed.

**Why deferred:** Current design does CHECKSUM change → full refresh. HASHBYTES escalation only matters for large cold tables where you want to load only changed rows instead of full refresh.

**Revisit when:** Cold tables grow to hundreds of thousands of rows and full refresh becomes costly. At that point, row-level HASHBYTES diffing saves transfer and load time.

#### 11. Polars as Alternative to PyArrow

**What:** Fast DataFrame library built on Arrow. Could replace PyArrow for data handling.

**Why deferred:** Already using PyArrow directly. Polars adds a dependency for something PyArrow handles. Polars shines when you need DataFrame operations (group by, filter, transform in Python) — but our transforms are SQL.

**Revisit when:** Python-side data manipulation becomes necessary before loading (e.g., complex cleaning, enrichment, or reshaping that's awkward in SQL).

#### 12. Regional Alignment for MotherDuck

**What:** MotherDuck is hosted in specific AWS regions (us-east-1, eu-central-1). Network latency from India to Virginia can negate performance gains.

**Why deferred:** Not skipped — needs investigation. Where is the MotherDuck instance hosted? Where does the ETL process run? If there's a region mismatch, this directly impacts load performance.

**Status:** **OPEN QUESTION** — needs answer from user before planning.

#### 13. uv as Project Manager

**What:** Astral's fast Python package manager. Replaces pip/venv/poetry. Fast, handles virtual environments and dependencies in one tool.

**Why deferred:** Not skipped — this should be used. Aligns with "lightweight" philosophy. No reason to use heavier alternatives.

**Status:** **INCLUDE in v1** — use uv for project setup and dependency management.

---

## Final Consolidated Architecture

After synthesizing research from 6 agents, the final architecture:

```
SQL Server (read-only ERP)
    │
    ├── pyodbc (extraction)
    │   ├── Hot tables: WHERE timestamp > watermark - overlap_window ORDER BY timestamp
    │   └── Cold tables: CHECKSUM_AGG + COUNT(*) check → full refresh if changed
    │
    ▼
PyArrow (in-memory, zero-copy)
    │
    ▼
Local DuckDB (staging + state + transforms)
    ├── bronze schema (raw data)
    ├── silver views (column renames, filters)
    ├── gold views (header+detail joins)
    ├── _watermarks table (per-table state)
    ├── _runs table (run history)
    ├── _run_steps table (granular step logging)
    ├── _dq_results table (quality check results)
    └── _quarantine schema (bad rows)
    │
    ▼
MotherDuck (via ATTACH, final destination)
    │
    ▼
Rill Data (dashboards)
```

**Scheduling:** APScheduler v3.x + SQLite job store
**Config:** YAML with human-readable schedules and tier shortcuts
**CLI:** typer for manual runs and debugging
**Alerts:** Slack webhook for failures
**Dependencies:** pyodbc, pyarrow, duckdb, apscheduler, pyyaml, requests, typer (7 total)
**Future upgrade path:** mssql-python (when Arrow support lands), ADBC, turbodbc

---

## Addendum 6: Seventh Agent Refinements

### `_etl_loaded_at` and `_etl_run_id` on Every Target Row

Add two metadata columns to every bronze table row during loading:

```python
conn.execute("""
    INSERT INTO bronze.salesinvoice
    SELECT *, current_timestamp AS _etl_loaded_at, ? AS _etl_run_id
    FROM __staging
""", [run_id])
```

Costs nothing, invaluable for debugging: "which rows came from which run?" "when was this row loaded?" This replaces dlt's `_dlt_load_id` pattern with a simpler, more explicit version. The `_etl_run_id` links to the `_runs` table for full traceability.

### Only Push Gold to MotherDuck

Refinement of the local staging decision: bronze and silver stay local-only. Only materialized gold tables go to MotherDuck. Rill Data queries gold tables on MotherDuck.

**Implication:** Gold transforms must be `CREATE TABLE AS` (materialized), not views. Views would need bronze tables in MotherDuck too. But materializing gold is fine — gold tables are the final denormalized output, and at current volumes a full gold rebuild takes seconds.

**Cost impact:** Reduces MotherDuck storage and compute significantly — only final business-ready tables are stored in the cloud.

**Decision:** Adopt this. Push only gold to MotherDuck. Bronze + silver are local DuckDB only.

### OS Cron as Alternative to APScheduler

Write feather-etl as a pure CLI tool (`feather run --tier hot`), let OS cron handle timing:

```cron
0 */6 * * *  cd /path/to/project && uv run feather run --tier hot
0 3 * * 0    cd /path/to/project && uv run feather run --tier cold
```

**Advantages:** Zero scheduler failure modes, more debuggable, no APScheduler dependency, works on any OS.
**Disadvantages:** No per-table scheduling granularity within a tier, no dynamic job modification, no built-in missed-job recovery.

**Decision:** Keep APScheduler as default but design CLI to work standalone with OS cron as a valid deployment option. This means `feather run` must work as a one-shot command (not just within the scheduler).

### `SET NOCOUNT ON` for SQL Server Queries

Add `SET NOCOUNT ON` before extraction queries. Prevents SQL Server from sending row count messages after each statement, reducing network chatter:

```python
cursor.execute("SET NOCOUNT ON")
cursor.execute("SELECT * FROM ...")
```

Small optimization, zero cost to implement.

### `string.Template` Over Jinja2

Python stdlib's `string.Template` handles `${source_table}` substitution without adding Jinja2 as a dependency. Sufficient for our transform variables. If conditional blocks are ever needed, upgrade to Jinja2 then.

**Decision:** Use `string.Template`. Drop Jinja2 from consideration. Dependencies stay at 7 (no Jinja2 was in the list anyway).

### `graphlib.TopologicalSorter` for Transform Ordering

Python stdlib since 3.9. If transforms need execution ordering (for materialized gold tables that depend on silver views):

```python
from graphlib import TopologicalSorter
ts = TopologicalSorter({"gold.sales": {"silver.invoice", "silver.customer"}})
for transform in ts.static_order():
    run_transform(conn, transform)
```

Zero dependencies, stdlib only.

### Updated Final Architecture (post 7th agent)

```
SQL Server (read-only ERP)
    │
    ├── pyodbc (extraction, SET NOCOUNT ON)
    │   ├── Hot tables: WHERE timestamp > watermark - overlap ORDER BY timestamp
    │   └── Cold tables: CHECKSUM_AGG + COUNT(*) → full refresh if changed
    │
    ▼
PyArrow (in-memory, zero-copy)
    │
    ▼
Local DuckDB (staging + state + transforms)
    ├── bronze schema (raw data + _etl_loaded_at + _etl_run_id)
    ├── silver views (column renames, filters) — LOCAL ONLY
    ├── gold materialized tables (header+detail joins) — PUSHED TO MOTHERDUCK
    ├── _watermarks, _runs, _run_steps, _dq_results, _schema_snapshots
    └── _quarantine schema (bad rows)
    │
    ▼
MotherDuck (via ATTACH — gold tables only)
    │
    ▼
Rill Data (dashboards — queries gold tables)

---

## Addendum 7: Connector Interface Design Research (8th Agent)

### Comparative Analysis of Connector Abstractions

| Tool | Abstraction | Min Lines (CSV) | Min Lines (SQL) | Data Format |
|------|------------|----------------|----------------|-------------|
| **Airbyte CDK** | ABC classes (Source + Stream) | ~100+ | ~150+ | dict / AirbyteMessage |
| **dlt** | Decorated generator functions | ~5 | ~20 | dict / DataFrame / Arrow |
| **Singer SDK** | ABC hierarchy (Stream → RESTStream → SQLStream) | ~25 | ~30 | dict |
| **Sling** | Go interface (70+ methods, internal only) | N/A | N/A | iop.Datastream |
| **ingestr** | Python Protocol (2 methods), delegates to dlt | ~15 | ~30 | dlt-compatible |

### What Was Borrowed

- **From ingestr:** Protocol-based interface (structural typing, no forced inheritance)
- **From Singer SDK:** Specialization hierarchy (FileSource, DatabaseSource) to eliminate boilerplate
- **From dlt:** Arrow as first-class output, state passed TO connector not managed BY it, minimal ceremony
- **From Airbyte:** `discover()` and `check()` as explicit interface methods

### Decision: Reimplement dlt Patterns (Option B)

**Evaluated three options:**

| Option | Approach | Deps Impact | Risk |
|--------|----------|-------------|------|
| A. Use dlt as dependency | Import dlt sources, skip dlt loading | +30 transitive deps | Framework lock-in |
| **B. Reimplement patterns** | **Study dlt code, rewrite ~300 LOC** | **No extra deps** | **Edge cases** |
| C. Hybrid (dlt now, replace later) | Ship fast with dlt, swap later | +30 deps initially | "Temporary" becomes permanent |

**Chose Option B because:**
- Core extraction logic is ~100 lines (pyodbc + fetchmany + Arrow)
- Incremental/watermark logic is ~80 lines
- File sources don't benefit from dlt at all (dlt lacks native file readers)
- dlt adds ~30 transitive dependencies (SQLAlchemy, jsonpath-ng, pendulum, etc.)
- Protocol-based interface remains dlt-compatible for future adapter wrapping

### Connector Interface (as implemented in PRD)

See PRD section 5 for full details. Summary:

**Source Protocol:** `check()`, `discover()`, `extract()`, `detect_changes()`, `get_schema()` — 5 methods
**Destination Protocol:** `load_full()`, `load_incremental()`, `execute_sql()`, `sync_to_remote()` — 4 methods
**Base classes:** `FileSource` (mtime+hash, DuckDB readers), `DatabaseSource` (cursor→Arrow, checksum)
**Registry:** `SOURCE_REGISTRY` maps type names to classes, `create_source()` factory
