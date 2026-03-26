# Slice 1 Foundation Implementation Plan

Created: 2026-03-26
Status: VERIFIED
Approved: Yes
Iterations: 1
Worktree: Yes
Type: Feature
Review: [docs/reviews/2026-03-26-slice1-review.md](../reviews/2026-03-26-slice1-review.md)

## Summary

**Goal:** Build feather-etl Slice 1 — the minimum viable pipeline where an operator runs `feather init` → edits `feather.yaml` → `feather validate` → `feather discover` → `feather setup` → `feather run` → `feather status` and sees real client data in a local bronze DuckDB. Full strategy only, DuckDB source only.

**Architecture:** Python package at `src/feather/` using `uv`, with Source Protocol + DuckDBFileSource, DuckDB destination with swap pattern loading, state management in separate DuckDB, and a thin Typer CLI. Config driven by `feather.yaml` with typed dataclasses.

**Tech Stack:** Python 3.10+, uv, duckdb, pyarrow, pyyaml, typer (+ pyodbc, apscheduler, openpyxl declared but unused in Slice 1)

## Scope

### In Scope

- Project scaffold: `pyproject.toml`, `src/feather/` package structure, all 7 deps
- Config: parse `feather.yaml` + `tables/` directory, env var substitution, path resolution, typed dataclasses, validation (file sources, full strategy only)
- Validation guard: FR2.9 rules for file sources, writes `feather_validation.json`, runs at start of every command
- Source Protocol + `StreamSchema` + `ChangeResult` dataclasses + source registry
- `DuckDBFileSource`: ATTACH-based discover() and extract(), full strategy only
- DuckDB destination: schema creation (bronze, silver, gold, _quarantine), full strategy swap pattern, `_etl_loaded_at` + `_etl_run_id` metadata columns
- State DB: all 5 tables from FR7.4 (`_state_meta`, `_watermarks`, `_runs`, `_run_steps`, `_dq_results`, `_schema_snapshots`). Only `_watermarks` and `_runs` populated in Slice 1; others created empty for schema stability.
- Pipeline: `run_table()` (validate → extract → load → update state), `run_all()`
- CLI: `feather init` (scaffold only), `feather validate`, `feather discover`, `feather setup`, `feather run`, `feather status`
- Test fixture: subset of real afans client data (6 tables from `source_data.duckdb`)

### Out of Scope

- Change detection (mtime + hash) — Slice 2
- Incremental / append strategy — Slice 3
- CSV, SQLite, JSON, Excel sources — excluded per user clarification (2026-03-26) despite appearing in PRD Slice 1 component table. Will be added in a follow-on task using the FileSource base class scaffolded here.
- column_map, column selection — Slice 4
- primary_key validation — Slice 3
- Transforms (silver/gold SQL) — Slice 8
- DQ checks — Slice 9
- Schema drift detection — Slice 10
- SMTP alerting — Slice 11
- SQL Server source — Slice 7
- MotherDuck sync — Slice 13
- Scheduling — Slice 15
- `feather init` wizard (interactive prompts, discovery) — Slice 17
- `--json` output — Slice 18
- `feather history`, `feather run --table`, `feather run --tier`

## Approach

**Chosen:** Bottom-up build following PRD module breakdown — config → source protocol → DuckDB source → destination → state → pipeline → CLI. Each layer is independently testable before composition.

**Why:** The PRD defines clear module boundaries and protocols. Building bottom-up means each task produces working, tested code before the next layer depends on it. The Source Protocol enables future sources without changing downstream code.

**Alternatives considered:**
- *Top-down (CLI first, stub internals):* Would produce a "working" CLI faster but with untested internals. Rejected because the PRD emphasizes testability at each layer.
- *Single monolithic module:* Fewer files but violates PRD module breakdown and makes future slices harder. Rejected.

## Context for Implementer

> Write for an implementer who has never seen the codebase.

- **Patterns to follow:** PRD Section 6 (Connector Interface Design) defines the Source Protocol, FileSource base class, and registry pattern. PRD Section 7 defines the module breakdown with LOC targets per file.
- **Conventions:** Package uses `src/feather/` layout. All paths resolve relative to `feather.yaml` location, not CWD. Config uses typed dataclasses, not raw dicts. CLI is a thin wrapper — no pipeline logic in `cli.py`.
- **Key files:**
  - `docs/prd.md` — the authoritative spec for all behavior
  - `tests/fixtures/client.duckdb` — real afans ERP data subset (6 tables in `icube` schema)
- **Gotchas:**
  - DuckDB ATTACH gives the attached DB an alias. Tables are accessed as `{alias}.{schema}.{table}`. The source_table config uses `schema.table` format (e.g., `icube.SALESINVOICE`).
  - `feather_validation.json` must be written by every command, not just `feather validate`.
  - State DB is separate from data DB — two different DuckDB files.
  - The swap pattern for full strategy is: CREATE TABLE new → DROP old → RENAME new. Must be in a transaction.
- **Domain context:** feather-etl extracts data from client ERP systems into local DuckDB for analytics. "Bronze" is the raw landing layer with all columns plus ETL metadata (`_etl_loaded_at`, `_etl_run_id`). The test data comes from an Indian ERP system called Icube.

## Autonomous Decisions

- **DuckDB only for Slice 1** — user confirmed. CSV and SQLite deferred.
- **primary_key validation deferred to Slice 3** — user confirmed. Config accepts primary_key but doesn't validate it.
- **Test fixture uses date-based split** — `client.duckdb` has data **before** 2025-10-01 (initial batch). `client_update.duckdb` has data **Oct-Dec 2025** (new data for future incremental tests). Reference tables taken in full.
- **source_table uses `schema.table` format** — user confirmed. DuckDBFileSource queries `{alias}.{source_table}`.
- **`feather_validation.json` written to `feather.yaml` directory** — no FR specifies this path explicitly, but follows FR2.15 principle that config-relative paths anchor to feather.yaml location.
- **`target_table` default is `silver.{name}` per FR2.6** — Slice 1 E2E tests explicitly set `target_table: bronze.*` rather than changing the default.

## Test Fixture Design

Extract from `/Users/siraj/Desktop/NonDropBoxProjects/afans-reporting-dev/discovery/source_data.duckdb`.

**`tests/fixtures/client.duckdb`** — initial data (used by Slice 1):

| Source Table | Filter | Expected Rows | Purpose |
|---|---|---|---|
| `icube.SALESINVOICE` | `ModifiedDate >= '2025-08-01' AND ModifiedDate < '2025-10-01'` | ~11,676 | Main transactional (timestamp: ModifiedDate) |
| `icube.SALESINVOICEMASTER` | `ModifyByDate >= '2025-08-01' AND ModifyByDate < '2025-10-01'` | ~335 | Master records (timestamp: ModifyByDate) |
| `icube.INVITEM` | `Timestamp >= '2025-08-01' AND Timestamp < '2025-10-01'` | ~1,058 | Wide table, 189 cols (timestamp: Timestamp) |
| `icube.CUSTOMERMASTER` | (all rows) | 1,339 | Reference table, full refresh |
| `icube.EmployeeForm` | (all rows) | 55 | Small reference table |
| `icube.InventoryGroup` | (all rows) | 66 | Small reference table |

**`tests/fixtures/client_update.duckdb`** — new data (used by Slice 3 incremental tests):

| Source Table | Filter | Expected Rows | Purpose |
|---|---|---|---|
| `icube.SALESINVOICE` | `ModifiedDate >= '2025-10-01'` | ~7,870 | New transactional rows |
| `icube.SALESINVOICEMASTER` | `ModifyByDate >= '2025-10-01'` | ~219 | New master rows |
| `icube.INVITEM` | `Timestamp >= '2025-10-01'` | ~3,932 | New inventory rows |

**Usage:** Slice 1 tests use `client.duckdb` only (full strategy). Slice 3 tests copy both files, ATTACH `client_update.duckdb`, and INSERT newer rows into the test copy to simulate incremental data arrival.

**Note:** All three timestamp tables use the Aug-Sep 2025 window (`>= 2025-08-01 AND < 2025-10-01`) to keep the fixture under ~15K total rows while preserving realistic data distribution.

## Assumptions

- DuckDB's ATTACH works with schema-qualified table names (`attached_db.icube.SALESINVOICE`) — supported by DuckDB docs — Tasks 3, 4 depend on this
- `feather_validation.json` is written alongside `feather.yaml` (same directory) — supported by PRD Slice 1 component table (Validation guard row) and FR11.10; follows FR2.15 principle that config-relative paths anchor to feather.yaml location — Tasks 2, 6 depend on this
- `_etl_loaded_at` uses `CURRENT_TIMESTAMP` at insert time, `_etl_run_id` is `{table_name}_{iso_timestamp}` — supported by PRD FR4.7, FR7 state schema — Task 4 depends on this
- State schema version starts at 1, with `_state_meta` tracking version — supported by PRD FR7 State Schema Reference — Task 5 depends on this
- `feather init` in Slice 1 is scaffold-only (no wizard, no prompts) — supported by PRD v1.8 changelog — Task 7 depends on this

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| DuckDB ATTACH with schema-qualified names fails | Low | High | Test early in Task 3. If fails, use `SELECT * FROM` with explicit schema prefix |
| Test fixture too large for fast CI | Low | Medium | Date cutoff keeps total under ~14K rows. If slow, tighten to Nov 1 |
| Config dataclass design doesn't accommodate future slices | Medium | Medium | Follow PRD FR2.5 field list exactly. Fields for future features (column_map, filter, etc.) exist in dataclass but aren't validated in Slice 1 |

## Goal Verification

### Truths

1. `feather init` in a temp directory creates a valid project scaffold (feather.yaml template, pyproject.toml, .gitignore, .env.example, transforms/, tables/, extracts/)
2. `feather validate` on a valid config exits 0 and writes `feather_validation.json`; on invalid config exits non-zero with specific error message
3. `feather discover` lists all tables in the DuckDB source with column names and types
4. `feather setup` creates `feather_state.duckdb` with all 6 state tables (`_state_meta`, `_watermarks`, `_runs`, `_run_steps`, `_dq_results`, `_schema_snapshots`), and creates bronze/silver/gold/_quarantine schemas in `feather_data.duckdb`
5. `feather run` extracts configured tables into bronze.* with `_etl_loaded_at` and `_etl_run_id` columns, records run in `_runs`
6. `feather status` shows last run per table with status, row count, and timestamp
7. Second `feather run` re-extracts everything (no change detection in Slice 1)

### Artifacts

| Truth | Supporting Artifacts |
|-------|---------------------|
| 1 | `src/feather/init_wizard.py` — scaffold logic |
| 2 | `src/feather/config.py` — validation + JSON output |
| 3 | `src/feather/sources/duckdb_file.py` — discover() |
| 4 | `src/feather/state.py` — state init, `src/feather/destinations/duckdb.py` — schema creation |
| 5 | `src/feather/pipeline.py` — run_table(), `src/feather/destinations/duckdb.py` — load_full() |
| 6 | `src/feather/state.py` — status query |
| 7 | `src/feather/pipeline.py` — no change detection call |

## Progress Tracking

- [x] Task 1: Project scaffold + test fixture
- [x] Task 2: Config parsing + validation
- [x] Task 3: Source Protocol + DuckDBFileSource
- [x] Task 4: DuckDB destination
- [x] Task 5: State management
- [x] Task 6: Pipeline orchestrator
- [x] Task 7: CLI (thin wrapper)
- [x] Task 8: End-to-end integration test

**Total Tasks:** 8 | **Completed:** 8 | **Remaining:** 0

## Implementation Tasks

### Task 1: Project scaffold + test fixture

**Objective:** Set up the Python package with uv, create directory structure, install dependencies, and extract the test fixture from afans source data.

**Dependencies:** None

**Files:**

- Create: `pyproject.toml`
- Create: `src/feather/__init__.py`
- Create: `src/feather/sources/__init__.py`
- Create: `src/feather/destinations/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/fixtures/client.duckdb` (generated via script)
- Create: `scripts/create_test_fixture.py`

**Key Decisions / Notes:**

- Package name: `feather-etl`, import as `feather`
- Use `uv` for project management (`uv init`, `uv add`)
- All 7 PRD dependencies declared: duckdb, pyarrow, pyyaml, typer, pyodbc, apscheduler, openpyxl
- Dev dependencies: pytest, pytest-cov
- Test fixture script extracts 6 tables from afans `source_data.duckdb` into `tests/fixtures/client.duckdb` (data before 2025-10-01) and `tests/fixtures/client_update.duckdb` (data Oct-Dec 2025), both in `icube` schema
- `conftest.py` provides `client_db` fixture (copies client.duckdb to tmp_path) and `runner` fixture (Typer CliRunner)

**Definition of Done:**

- [ ] `uv run python -c "import feather"` succeeds
- [ ] `tests/fixtures/client.duckdb` exists with 6 tables in icube schema
- [ ] `uv run pytest tests/ -q` runs (even if 0 tests)

**Verify:**

- `uv run python -c "import feather; print(feather.__version__)"`
- `uv run python -c "import duckdb; print(duckdb.connect('tests/fixtures/client.duckdb').execute('SHOW ALL TABLES').fetchall())"`

---

### Task 2: Config parsing + validation

**Objective:** Parse `feather.yaml` into typed dataclasses, resolve env vars and paths, validate for Slice 1 scope (file sources, full strategy), and write `feather_validation.json`.

**Dependencies:** Task 1

**Files:**

- Create: `src/feather/config.py`
- Test: `tests/test_config.py`

**Key Decisions / Notes:**

- Dataclasses: `FeatherConfig`, `SourceConfig`, `DestinationConfig`, `TableConfig`, `DefaultsConfig`
- `SourceConfig.type` validated against known types (duckdb, sqlite, csv, excel, json, sqlserver) per FR2.12
- All relative paths resolve against `feather.yaml` parent directory per FR2.15
- `${VAR_NAME}` resolved via `os.path.expandvars()` per FR2.2
- File source validation: `source.path` must exist per FR2.13
- `strategy` validated to be one of: full, incremental, append per FR2.5
- `target_table` defaults to `silver.{name}` if omitted per FR2.6. Slice 1 E2E tests explicitly set `target_table: bronze.*` — the default is not changed.
- `target_table` schema prefix validated against bronze/silver/gold per FR2.14
- Tables from `tables/` directory merged with inline tables per FR2.1b
- `feather_validation.json` written with: valid (bool), errors (list), resolved_paths (dict), tables_count (int), timestamp
- Config fields for future slices (column_map, filter, quality_checks, schedule, etc.) exist in dataclass but are not validated in Slice 1

**Definition of Done:**

- [ ] Valid config parses into typed dataclasses
- [ ] Invalid config (bad source type, missing path, bad strategy) raises specific errors
- [ ] Env var substitution works (e.g., `${HOME}` resolves)
- [ ] Path resolution relative to config file, not CWD
- [ ] `feather_validation.json` written on both success and failure
- [ ] Tables from `tables/` directory merged correctly
- [ ] Config with no `primary_key` on a table does NOT raise a validation error (deferred to Slice 3 per user decision)

**Verify:**

- `uv run pytest tests/test_config.py -q`

---

### Task 3: Source Protocol + DuckDBFileSource

**Objective:** Define the Source Protocol, StreamSchema, ChangeResult dataclasses, source registry, and implement DuckDBFileSource with discover() and extract() for full strategy.

**Dependencies:** Task 1

**Files:**

- Create: `src/feather/sources/__init__.py` (Protocol + dataclasses)
- Create: `src/feather/sources/base.py` (FileSource base class stub — change detection deferred)
- Create: `src/feather/sources/duckdb_file.py` (DuckDBFileSource)
- Create: `src/feather/sources/registry.py` (SOURCE_REGISTRY + create_source())
- Test: `tests/test_sources.py`

**Key Decisions / Notes:**

- Source Protocol per PRD Section 6: `check()`, `discover()`, `extract()`, `detect_changes()`, `get_schema()`
- DuckDBFileSource uses `ATTACH '{path}' AS source_db (READ_ONLY)` then queries `source_db.{source_table}`
- `source_table` in config is `schema.table` format (e.g., `icube.SALESINVOICE`)
- `discover()` queries `information_schema.tables` and `information_schema.columns` on attached DB
- `extract()` runs `SELECT * FROM source_db.{source_table}` via DuckDB, returns PyArrow Table via `fetcharrow()`
- `detect_changes()` returns `ChangeResult(changed=True, reason="first_run")` always in Slice 1 (no change detection)
- `get_schema()` queries column metadata from attached DB
- Registry maps `"duckdb"` → `DuckDBFileSource` (other sources registered as they're built)

**Definition of Done:**

- [ ] `discover()` returns list of `StreamSchema` for all tables in client.duckdb
- [ ] `extract()` returns PyArrow Table with correct row count for a known table
- [ ] `check()` returns True for valid DuckDB file, False for nonexistent path
- [ ] `get_schema()` returns column names and types
- [ ] Registry resolves `"duckdb"` to `DuckDBFileSource`

**Verify:**

- `uv run pytest tests/test_sources.py -q`

---

### Task 4: DuckDB destination

**Objective:** Implement DuckDBDestination with schema creation, full strategy swap pattern loading, and ETL metadata columns.

**Dependencies:** Task 1, Task 3 (for PyArrow Table format)

**Files:**

- Create: `src/feather/destinations/__init__.py` (Destination Protocol)
- Create: `src/feather/destinations/duckdb.py` (DuckDBDestination)
- Test: `tests/test_destinations.py`

**Key Decisions / Notes:**

- Destination Protocol per PRD Section 6: `load_full()`, `load_incremental()`, `execute_sql()`, `sync_to_remote()`
- Only `load_full()` and schema creation implemented in Slice 1
- `setup_schemas()` creates bronze, silver, gold, _quarantine schemas via `CREATE SCHEMA IF NOT EXISTS`
- `load_full()` swap pattern per FR4.3: `CREATE TABLE {target}_new AS SELECT *, CURRENT_TIMESTAMP AS _etl_loaded_at, '{run_id}' AS _etl_run_id FROM arrow_table` → `DROP TABLE IF EXISTS {target}` → `ALTER TABLE {target}_new RENAME TO {final_name}` — all in one transaction
- DuckDB can directly query PyArrow Tables registered via `con.register('arrow_data', arrow_table)` then `CREATE TABLE ... AS SELECT * FROM arrow_data`
- Returns row count loaded
- File permissions: create DuckDB files with 600 permissions per NFR8

**Definition of Done:**

- [ ] `setup_schemas()` creates all four schemas
- [ ] `load_full()` loads PyArrow Table into bronze.{table} with `_etl_loaded_at` and `_etl_run_id`
- [ ] Second `load_full()` replaces data (swap pattern idempotency)
- [ ] Row count matches source data

**Verify:**

- `uv run pytest tests/test_destinations.py -q`

---

### Task 5: State management

**Objective:** Implement state DB initialization, watermark tracking, and run recording.

**Dependencies:** Task 1

**Files:**

- Create: `src/feather/state.py`
- Test: `tests/test_state.py`

**Key Decisions / Notes:**

- State DB is separate DuckDB file (`feather_state.duckdb` by default)
- `init_state()` creates all 5 FR7.4 tables: `_state_meta`, `_watermarks`, `_runs`, `_run_steps`, `_dq_results`, `_schema_snapshots`
- `_state_meta` per PRD: `schema_version` (INTEGER PK), `created_at` (TIMESTAMP), `feather_version` (VARCHAR). Insert row with version=1 on first init.
- `_watermarks` per PRD schema — all columns created, only `table_name`, `strategy`, `last_run_at` populated in Slice 1
- `_runs` per PRD schema — all 14 columns: `run_id`, `table_name`, `started_at`, `ended_at`, `duration_sec`, `status`, `rows_extracted`, `rows_loaded`, `rows_skipped`, `error_message`, `watermark_before`, `watermark_after`, `freshness_max_ts`, `schema_changes`. Columns unused in Slice 1 (`rows_skipped`, `watermark_before`, `watermark_after`, `freshness_max_ts`, `schema_changes`) inserted as NULL.
- `_run_steps`, `_dq_results`, `_schema_snapshots` created with full PRD schemas but remain empty in Slice 1
- `read_watermark(table_name)` returns watermark dict or None
- `write_watermark(table_name, strategy, ...)` upserts watermark
- `record_run(run_id, table_name, ...)` inserts run record
- `get_status()` returns last run per table (for `feather status`)
- Downgrade protection: if state DB version > package version, raise error

**Definition of Done:**

- [ ] `init_state()` creates all 5 FR7.4 tables plus `_state_meta` (6 total) with correct schemas
- [ ] `_state_meta` has version=1 after init
- [ ] `write_watermark()` + `read_watermark()` roundtrip works
- [ ] `record_run()` writes run record, `get_status()` retrieves it
- [ ] Re-running `init_state()` on existing DB is idempotent (no errors, no data loss)

**Verify:**

- `uv run pytest tests/test_state.py -q`

---

### Task 6: Pipeline orchestrator

**Objective:** Implement `run_table()` and `run_all()` — the core pipeline that ties source, destination, and state together.

**Dependencies:** Task 2, Task 3, Task 4, Task 5

**Files:**

- Create: `src/feather/pipeline.py`
- Test: `tests/test_pipeline.py`

**Key Decisions / Notes:**

- `run_table()` per PRD Section 7 Core Pipeline Flow (Slice 1 subset):
  1. Extract data via source.extract()
  2. Load to DuckDB via destination.load_full()
  3. Update state (watermark + run record)
  4. Return `RunResult` dataclass (status, rows_loaded, etc.)
- `run_all()` iterates configured tables, calls `run_table()` for each
- Per-table error isolation: one table's failure doesn't stop others (FR13.4 / NFR9)
- `run_id` format: `{table_name}_{iso_timestamp}` per PRD
- No change detection call in Slice 1 — always extracts
- Validation guard: every pipeline entry point calls config validation first and writes `feather_validation.json`

**Definition of Done:**

- [ ] `run_table()` extracts from DuckDB source → loads to bronze → records state
- [ ] `run_all()` runs all configured tables
- [ ] Failed table doesn't stop other tables
- [ ] `RunResult` contains status, rows_loaded, table_name, run_id
- [ ] Second run re-extracts (change detection not implemented)
- [ ] `feather_validation.json` written before pipeline starts

**Verify:**

- `uv run pytest tests/test_pipeline.py -q`

---

### Task 7: CLI (thin wrapper)

**Objective:** Build the Typer CLI with all Slice 1 commands: init, validate, discover, setup, run, status. CLI calls into config/pipeline/state — no business logic.

**Dependencies:** Task 2, Task 3, Task 5, Task 6

**Files:**

- Create: `src/feather/cli.py`
- Create: `src/feather/init_wizard.py`
- Test: `tests/test_cli.py`

**Key Decisions / Notes:**

- Entry point: `feather` via `[project.scripts]` in pyproject.toml
- All commands accept `--config PATH` (default: `feather.yaml`) per FR11.9
- `feather init [project_name]` — scaffold only per v1.8: create directory with feather.yaml template, pyproject.toml, .gitignore, .env.example, transforms/silver/, transforms/gold/, tables/, extracts/
- `feather validate` — parse config, validate, write `feather_validation.json`, print summary
- `feather discover` — call source.discover(), print tables/columns/types
- `feather setup` — init state DB, create schemas in data DB
- `feather run` — call pipeline.run_all(), print results
- `feather status` — call state.get_status(), print table with last run info
- Every command (except init) auto-validates config and writes `feather_validation.json` before executing
- CLI is thin: each command is ~5-10 lines calling into the appropriate module
- init_wizard.py contains the scaffold logic (~50 LOC for Slice 1, expandable to ~120 LOC for Slice 17 wizard)

**Definition of Done:**

- [ ] `feather init test-project` creates scaffolded directory
- [ ] `feather validate --config path/to/feather.yaml` exits 0 on valid, non-zero on invalid
- [ ] `feather discover --config ...` prints tables and columns
- [ ] `feather setup --config ...` creates state DB and schemas
- [ ] `feather run --config ...` extracts tables into bronze
- [ ] `feather status --config ...` shows run results
- [ ] Every command writes `feather_validation.json`

**Verify:**

- `uv run pytest tests/test_cli.py -q`

---

### Task 8: End-to-end integration test

**Objective:** Single test that runs the full onboarding flow: init → validate → discover → setup → run → status → run again. Verifies the complete Slice 1 pipeline against real client data.

**Dependencies:** All previous tasks

**Files:**

- Create: `tests/test_e2e.py`

**Key Decisions / Notes:**

- Uses `client_db` fixture (real afans data, copied to tmp_path)
- Generates a `feather.yaml` pointing at the client DuckDB with 3 tables: SALESINVOICE, CUSTOMERMASTER, InventoryGroup
- Runs CLI commands via Typer's CliRunner (in-process, no subprocess)
- Verifies each step per the PRD's end-to-end test specification (Section 8, Slice 1)
- After `feather run`: query bronze.sales_invoice in feather_data.duckdb, verify row count matches, verify `_etl_loaded_at` and `_etl_run_id` columns present
- After second `feather run`: verify re-extraction happened (change detection not yet implemented)
- Verify `feather_validation.json` exists after each command

**Definition of Done:**

- [ ] Full onboarding flow completes without errors
- [ ] bronze.* tables contain expected row counts
- [ ] `_etl_loaded_at` and `_etl_run_id` present on all rows
- [ ] `feather status` shows success for all tables
- [ ] Second run re-extracts successfully
- [ ] `feather_validation.json` written

**Verify:**

- `uv run pytest tests/test_e2e.py -v`
- `uv run pytest tests/ -q` (full suite passes)

## Open Questions

None — all questions resolved during planning.

## Deferred Ideas

- **CSV/SQLite sources:** Narrowed out of Slice 1 per user decision. FileSource base class is ready for them.
- **`--json` output on CLI commands:** PRD FR11.15, deferred to Slice 18.
- **Structured JSONL logging:** PRD NFR7, deferred to Slice 18.
