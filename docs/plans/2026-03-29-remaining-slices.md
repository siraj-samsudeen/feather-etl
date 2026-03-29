# Remaining Slices Implementation Plan

Created: 2026-03-29
Status: VERIFIED
Approved: Yes
Iterations: 0
Worktree: Yes
Type: Feature

## Summary

**Goal:** Complete the feather-etl PRD by implementing the 5 remaining features: DQ checks (V9), schema drift detection (V10), duplicate row handling, multi-file CSV glob, and the interactive init wizard (V17). Also fix the partial-failure exit code bug.

**Architecture:** Six independent features, each adding a new module or extending existing ones. DQ checks and schema drift hook into the existing pipeline post-load. Dedup adds a config flag + extraction-time filtering. Multi-file CSV extends `CsvSource` with glob pattern support and per-file change detection. Init wizard extends the existing scaffold with interactive prompts and `--non-interactive` mode.

**Tech Stack:** Python stdlib only (no new dependencies). DuckDB queries for DQ checks and schema comparison. Typer prompts for init wizard.

## Scope

### In Scope

- **V9: DQ checks** — `not_null`, `unique`, `row_count`, `duplicate` checks. `_dq_results` table (exists). Alert integration via existing `alert_on_dq_failure()`.
- **V10: Schema drift** — Schema snapshots in `_schema_snapshots` (exists). Classify added/removed/type_changed. Alert via existing `alert_on_schema_drift()`. Load-time handling per FR4.11-14.
- **Dedup** — Detect-then-opt-in UX. V9 `duplicate` check warns. Operator adds `dedup: true` or `dedup_columns: [...]` to config. Extraction-time `SELECT DISTINCT` or `GROUP BY`.
- **Multi-file CSV glob** — `source_table: "sales_*.csv"` support. Per-file mtime+hash change detection. DuckDB `read_csv()` with file list.
- **V17: Init wizard** — Interactive prompts, `discover()` table selection, YAML + silver stub generation, `--non-interactive` agent mode.
- **Bugfix** — Partial failure exit code: backoff-skipped tables count toward non-zero exit.

### Out of Scope

- V13: MotherDuck sync (separate plan)
- V15: Scheduling / APScheduler (separate plan)
- Quarantine schema for type_changed cast failures (logged + alerted, rows excluded — full quarantine table is V10+)
- DQ `row_count` range checks (only 0-row warning in v1)
- Multi-file glob for Excel/JSON (CSV only in this plan)

## Approach

**Chosen:** Sequential tasks, one feature at a time, each independently testable.

**Why:** Each feature touches different files with minimal overlap. Sequential execution avoids merge conflicts while keeping each task small (2-4 files). The order is: bugfix first (tiny), then DQ (needed by dedup), then schema drift, then dedup (depends on DQ), then CSV glob, then wizard (largest, most independent).

**Alternatives considered:**
- Parallel agent groups: Low benefit — most features touch pipeline.py, so conflicts are likely. Rejected.
- Single monolithic task: Too large to review incrementally. Rejected.

## Context for Implementer

> Write for an implementer who has never seen the codebase.

### Patterns to Follow

- **New module pattern:** See `src/feather/alerts.py` (80 lines) — standalone function-based module, no classes, imported by pipeline.py.
- **State table pattern:** See `state.py:90-99` — `_dq_results` table already exists with columns `(run_id, table_name, check_type, column_name, result, details, checked_at)`. `_schema_snapshots` table at `state.py:102-108`.
- **Pipeline hook pattern:** See `pipeline.py:364` — `alert_on_failure()` called in the except block. DQ and schema drift hooks follow the same pattern: import, call, pass config.
- **Config parsing pattern:** See `config.py:100-124` — `_parse_tables()` reads dict fields into `TableConfig` dataclass.
- **Source extension pattern:** See `sources/csv.py` (81 lines) — extends `FileSource`, implements `extract()`, `discover()`, `get_schema()`.

### Conventions

- `from __future__ import annotations` at top of every module
- Modern type hints: `list[str]`, `str | None`
- Real DuckDB fixtures in tests, no mocking (except pyodbc/psycopg2)
- Config validation in `config.py:_validate()`

### Key Files

| File | Purpose | Size |
|------|---------|------|
| `src/feather/pipeline.py` | `run_table()` orchestration — DQ and schema drift hook here | 439 lines |
| `src/feather/state.py` | StateManager — add DQ recording + schema snapshot methods | 445 lines |
| `src/feather/config.py` | Config parsing — add `dedup`/`dedup_columns` fields | 391 lines |
| `src/feather/cli.py` | CLI — fix exit code, extend `feather init` | 374 lines |
| `src/feather/sources/csv.py` | CsvSource — extend for glob patterns | 81 lines |
| `src/feather/sources/file_source.py` | FileSource base — extend change detection for globs | 100 lines |
| `src/feather/init_wizard.py` | Scaffold + wizard — extend for interactive mode | 100 lines |
| `src/feather/alerts.py` | Alert hooks — `alert_on_dq_failure()` and `alert_on_schema_drift()` already exist | 80 lines |

### Gotchas

- `_dq_results` and `_schema_snapshots` tables already exist in `state.py:init_state()` — do NOT recreate them.
- `alert_on_dq_failure()` and `alert_on_schema_drift()` already exist in `alerts.py` — wire them, don't rewrite.
- `quality_checks` field already exists on `TableConfig` — just need to implement the logic.
- `_runs.schema_changes` JSON column already exists — populate it.
- CSV glob with per-file tracking: watermark metadata needs to store a dict of `{filename: {mtime, hash}}`, not a single mtime/hash.
- Exit code fix: `RunResult.error_message` is set for backoff-skipped tables ("In backoff...") — use this to distinguish from unchanged-skipped.

### Domain Context

- **DQ checks run post-load against the local DuckDB table**, not the source. This is per FR8.5.
- **Schema drift detection happens at extraction time** by comparing `source.get_schema()` against `_schema_snapshots`. First run saves baseline, no drift reported.
- **Dedup is an opt-in extraction filter**, not a load-time operation. The operator must explicitly enable it after seeing a DQ warning.
- **Multi-file CSV glob** treats all matching files as one logical table. `read_csv(['file1.csv', 'file2.csv'])` is native DuckDB.

## Assumptions

- `_dq_results` table schema (`run_id, table_name, check_type, column_name, result, details, checked_at`) is sufficient for V9 — supported by `state.py:90-99`. Tasks 2-3 depend on this.
- `_schema_snapshots` table schema (`table_name, column_name, data_type, snapshot_at`) is sufficient for V10 — supported by `state.py:102-108`. Tasks 4-5 depend on this.
- All sources implement `get_schema()` returning `list[tuple[str, str]]` — supported by `sources/__init__.py:50`. Task 4 depends on this.
- DuckDB `read_csv()` supports file list natively — supported by DuckDB docs. Task 6 depends on this.
- Typer supports `typer.prompt()` and `typer.confirm()` for interactive input — supported by Typer docs. Task 7 depends on this.

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Schema drift load handling (FR4.11-14) adds complexity to pipeline.py | Medium | Medium | Implement only added/removed in v1; type_changed logs + alerts but skips quarantine table creation |
| Per-file change detection for CSV globs may not fit cleanly in watermark metadata | Low | Medium | Store as JSON string in `last_file_hash` field (already VARCHAR) — parse on read |
| Init wizard interactive prompts hard to test | Medium | Low | Test non-interactive mode with CLI flags; interactive mode tested via mocked input |

## Goal Verification

### Truths

1. `feather run` with partial failure exits with code 1 on the second run (backoff-skipped tables count)
2. DQ checks run automatically after each load and results appear in `_dq_results`
3. `not_null`, `unique`, and `row_count` checks produce correct pass/fail results
4. Schema drift is detected on second run when source columns change, logged in `_runs.schema_changes`
5. Schema drift alerts fire with correct severity (INFO for added/removed, CRITICAL for type_changed)
6. `dedup: true` in config causes `SELECT DISTINCT` at extraction time
7. `source_table: "sales_*.csv"` extracts all matching files as one table
8. Per-file change detection correctly skips extraction when no files changed
9. `feather init` interactive mode prompts for source type, runs discover, generates YAML
10. `feather init --non-interactive` generates a valid project with CLI flags only

### Artifacts

- `src/feather/dq.py` — DQ check execution logic
- `src/feather/schema_drift.py` — Schema drift detection + classification
- `tests/test_dq.py` — DQ check tests
- `tests/test_schema_drift.py` — Schema drift tests
- `tests/test_csv_glob.py` — Multi-file CSV glob tests
- `tests/test_init_wizard.py` — Init wizard tests (interactive + non-interactive)

## Progress Tracking

- [x] Task 1: Fix partial failure exit code
- [x] Task 2: DQ checks module + pipeline integration
- [x] Task 3: Schema drift detection + pipeline integration
- [x] Task 4: Dedup config + extraction filtering
- [x] Task 5: Multi-file CSV glob with per-file change detection
- [x] Task 6: Init wizard interactive + non-interactive mode
      **Total Tasks:** 6 | **Completed:** 6 | **Remaining:** 0

## Implementation Tasks

### Task 1: Fix Partial Failure Exit Code

**Objective:** When `feather run` encounters tables that are backoff-skipped (due to a previous failure), the exit code should be non-zero — same as if the table had failed in the current run.

**Dependencies:** None

**Files:**

- Modify: `src/feather/cli.py`
- Test: `tests/test_cli.py` (or relevant test file)

**Key Decisions / Notes:**

- Root cause: `cli.py:264-266` counts only `status == "failure"` for exit code. Backoff-skipped tables have `status == "skipped"` with a non-None `error_message` ("In backoff...").
- Fix: Count results where `status == "failure"` OR (`status == "skipped"` AND `error_message is not None`) as failures for exit code.
- This distinguishes "skipped because unchanged" (no error_message) from "skipped because in backoff" (has error_message).

**Definition of Done:**

- [ ] `feather run` exits with code 1 when any table is backoff-skipped
- [ ] `feather run` still exits with code 0 when all tables succeed or are skipped due to unchanged data
- [ ] `hands_on_test.sh` S6 "exit code non-zero on partial failure" passes (existing test at lines 294-331)
- [ ] Full test suite passes

**Verify:**

- `bash scripts/hands_on_test.sh`
- `uv run pytest -q`

---

### Task 2: DQ Checks Module + Pipeline Integration

**Objective:** Implement declarative data quality checks (not_null, unique, row_count, duplicate) that run after each successful table load. Results are stored in `_dq_results` and failures trigger alerts.

**Dependencies:** None

**Files:**

- Create: `src/feather/dq.py`
- Modify: `src/feather/state.py` (add `record_dq_result()` method)
- Modify: `src/feather/pipeline.py` (wire DQ checks after load)
- Create: `tests/test_dq.py`

**Key Decisions / Notes:**

- `dq.py` exposes `run_dq_checks(con, table_name, target_table, quality_checks, run_id) -> list[DQResult]`
- Each check runs a SQL query against the loaded DuckDB table:
  - `not_null`: `SELECT COUNT(*) FROM {target} WHERE {col} IS NULL` — fail if > 0
  - `unique`: `SELECT {col}, COUNT(*) FROM {target} GROUP BY {col} HAVING COUNT(*) > 1` — fail if any
  - `row_count`: Always runs, `SELECT COUNT(*) FROM {target}` — warn if 0
  - `duplicate`: `SELECT COUNT(*) - COUNT(DISTINCT *) FROM {target}` — fail if > 0 (exact row duplicates). If `primary_key` is configured, also check `SELECT {pk}, COUNT(*) ... GROUP BY {pk} HAVING COUNT(*) > 1`
- DQ checks run inside `pipeline.py:run_table()` after successful load, before watermark update
- `state.record_dq_result()` writes to existing `_dq_results` table
- `alert_on_dq_failure()` already exists in `alerts.py` — call it on any `fail` result
- DQ failures do NOT block the pipeline (FR8.7) — data is loaded, alert sent, pipeline continues

**Definition of Done:**

- [ ] `not_null` check fails when NULLs exist in specified column
- [ ] `unique` check fails when duplicates exist in specified column
- [ ] `row_count` check warns when 0 rows loaded (runs always, no config needed)
- [ ] `duplicate` check detects exact duplicate rows
- [ ] Results stored in `_dq_results` with correct run_id, table_name, check_type, result, details
- [ ] Alert email triggered on DQ failure (mocked SMTP in tests)
- [ ] Pipeline continues after DQ failure (data still loaded)
- [ ] Full test suite passes

**Verify:**

- `uv run pytest tests/test_dq.py -q`
- `uv run pytest -q`

---

### Task 3: Schema Drift Detection + Pipeline Integration

**Objective:** Detect schema changes between source and stored snapshot, classify as added/removed/type_changed, log to `_runs.schema_changes`, trigger alerts, and handle at load time per FR4.11-14.

**Dependencies:** None

**Files:**

- Create: `src/feather/schema_drift.py`
- Modify: `src/feather/state.py` (add `save_schema_snapshot()`, `get_schema_snapshot()` methods)
- Modify: `src/feather/pipeline.py` (wire schema drift before load, handle at load time)
- Modify: `src/feather/destinations/duckdb.py` (add `alter_table_add_column()` for FR4.11)
- Create: `tests/test_schema_drift.py`

**Key Decisions / Notes:**

- `schema_drift.py` exposes:
  - `detect_drift(current_schema, stored_schema) -> DriftReport` with fields: `added: list[str]`, `removed: list[str]`, `type_changed: list[tuple[str, str, str]]` (col, old_type, new_type)
  - `classify_severity(drift) -> str` — returns "INFO" for added/removed, "CRITICAL" for type_changed
- Schema drift detection happens in `pipeline.py:run_table()` after `source.extract()` but before `dest.load_*()`:
  1. Call `source.get_schema(table.source_table)` to get current schema
  2. Call `state.get_schema_snapshot(table.name)` to get stored schema
  3. If no stored schema → first run, save baseline, no drift
  4. If drift detected → log to `_runs.schema_changes`, alert, handle:
     - **Added column (FR4.11):** `ALTER TABLE target ADD COLUMN {col} {type}` — called on the destination BEFORE `dest.load_*()` so the table schema matches incoming data. Existing rows get NULL.
     - **Removed column (FR4.12):** Add NULL column to PyArrow data before load — target retains column
     - **Type changed (FR4.13-14):** Attempt load; if DuckDB cast succeeds → OK. If cast fails → log CRITICAL alert, skip those rows (no quarantine table in v1)
  - **Ordering:** Schema drift detection and ALTER TABLE happen BEFORE `dest.load_*()`. The sequence is: detect drift → ALTER TABLE if needed → adjust PyArrow columns → load.
  5. Save updated schema snapshot
- `_schema_snapshots` uses UPSERT: `INSERT OR REPLACE` keyed on `(table_name, column_name)`

**Definition of Done:**

- [ ] First extraction saves schema baseline to `_schema_snapshots`
- [ ] Added column detected, `_runs.schema_changes` contains `{"added": ["new_col"]}`
- [ ] Removed column detected, target table retains column with NULLs for new rows
- [ ] Type changed detected, CRITICAL alert fires
- [ ] `[INFO]` alert for added/removed, `[CRITICAL]` for type_changed
- [ ] `ALTER TABLE` adds new column to existing target on added-column drift
- [ ] Full test suite passes

**Verify:**

- `uv run pytest tests/test_schema_drift.py -q`
- `uv run pytest -q`

---

### Task 4: Dedup Config + Extraction Filtering

**Objective:** Add `dedup` and `dedup_columns` config options. When enabled, file-based sources apply `SELECT DISTINCT` or `GROUP BY` at extraction time.

**Dependencies:** Task 2 (V9 DQ `duplicate` check provides detection — operator sees warning, then adds dedup config)

**Files:**

- Modify: `src/feather/config.py` (add `dedup`, `dedup_columns` to `TableConfig` + validation)
- Modify: `src/feather/sources/csv.py` (apply DISTINCT/GROUP BY in extract)
- Modify: `src/feather/sources/excel.py` (apply DISTINCT/GROUP BY in extract)
- Modify: `src/feather/sources/json_source.py` (apply DISTINCT/GROUP BY in extract)
- Modify: `src/feather/sources/file_source.py` (add `_build_dedup_clause()` helper)
- Create: `tests/test_dedup.py`

**Key Decisions / Notes:**

- `TableConfig` gets two new fields:
  - `dedup: bool = False` — when True, `SELECT DISTINCT *` at extraction
  - `dedup_columns: list[str] | None = None` — when set, `GROUP BY` these columns (keeps first occurrence)
- Validation: `dedup` and `dedup_columns` are mutually exclusive. `dedup_columns` must not be empty if set.
- Implementation in file sources: `FileSource._build_dedup_sql()` returns SQL fragment. Each source's `extract()` uses it.
  - `dedup: true` → `SELECT DISTINCT {cols} FROM read_csv(...) WHERE ...`
  - `dedup_columns: [id]` → wrap in `SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {dedup_cols} ORDER BY 1) AS _rn FROM ...) WHERE _rn = 1`

**Definition of Done:**

- [ ] `dedup: true` removes exact duplicate rows at extraction
- [ ] `dedup_columns: [id]` deduplicates by specified columns (keeps first)
- [ ] Validation rejects both `dedup` and `dedup_columns` set simultaneously
- [ ] Works for CSV, Excel, JSON sources
- [ ] Full test suite passes

**Verify:**

- `uv run pytest tests/test_dedup.py -q`
- `uv run pytest -q`

---

### Task 5: Multi-file CSV Glob with Per-file Change Detection

**Objective:** Support `source_table: "sales_*.csv"` in config. CsvSource detects glob patterns, extracts all matching files as one table, and tracks per-file mtime+hash for change detection.

**Dependencies:** None

**Files:**

- Modify: `src/feather/sources/csv.py` (glob detection in extract, discover, get_schema)
- Modify: `src/feather/sources/file_source.py` (per-file change detection for glob patterns)
- Modify: `src/feather/config.py` (validation: allow glob characters in source_table for CSV)
- Create: `tests/test_csv_glob.py`
- Create: `tests/fixtures/csv_glob/` (test CSV files: `sales_jan.csv`, `sales_feb.csv`)

**Key Decisions / Notes:**

- **Glob detection:** If `source_table` contains `*` or `?`, treat as glob pattern. Otherwise, treat as single file (existing behavior).
- **CsvSource changes:**
  - `_source_path_for_table(table)` → for glob, returns the directory (not a single file)
  - `_resolve_glob_files(table)` → new helper, returns sorted list of matching `Path` objects
  - `extract()` → for glob: `read_csv(['file1.csv', 'file2.csv', ...])` — DuckDB UNION ALL natively
  - `discover()` → for glob: show as single logical table with schema from first matching file
  - `get_schema()` → use first matching file's schema
- **Per-file change detection:**
  - Override `detect_changes()` in CsvSource for glob patterns
  - Store per-file state as JSON in watermark metadata: `{"files": {"sales_jan.csv": {"mtime": 1234, "hash": "abc"}, ...}}`
  - Algorithm: for each matching file, check mtime. Only hash files with changed mtime. If any file new/changed/removed → `changed=True`.
  - Store combined state in `last_file_hash` as JSON string (existing `last_file_hash VARCHAR` field at `state.py:52`).
- **Config validation:** Skip SQL identifier validation for CSV source_table when it contains glob characters.

**Definition of Done:**

- [ ] `source_table: "sales_*.csv"` extracts all matching files as one table
- [ ] Change detection skips when no files have changed
- [ ] Change detection triggers when a file is added, modified, or removed
- [ ] Only changed files are re-hashed (unchanged files skip hashing)
- [ ] `feather discover` shows glob table as a single logical entry
- [ ] Full test suite passes

**Verify:**

- `uv run pytest tests/test_csv_glob.py -q`
- `uv run pytest -q`

---

### Task 6: Init Wizard Interactive + Non-Interactive Mode

**Objective:** Extend `feather init` with an interactive wizard that prompts for source type, connection details, discovers tables, lets the user select which tables to extract, and generates `feather.yaml` + silver transform stubs. Also add `--non-interactive` mode for agent/CI use.

**Dependencies:** None (uses existing `discover()` from source implementations)

**Files:**

- Modify: `src/feather/init_wizard.py` (add interactive wizard logic)
- Modify: `src/feather/cli.py` (extend `init` command with flags)
- Create: `tests/test_init_wizard.py`

**Key Decisions / Notes:**

- **Interactive flow (FR11.13):**
  1. Prompt: source type (duckdb, sqlite, csv, excel, json, sqlserver, postgres)
  2. Prompt: connection details (path for file-based, connection_string for DB)
  3. Create temporary source, call `source.check()` to verify connectivity
  4. Call `source.discover()` to list available tables
  5. Display tables with column counts. Prompt user to select (space-separated numbers or "all")
  6. For each selected table: infer strategy (incremental if timestamp column found, else full), generate table entry
  7. Generate `feather.yaml` with selected tables
  8. Generate `transforms/silver/{table_name}.sql` stubs with `-- depends_on` header and `SELECT * FROM bronze.{table_name}` body
  9. Run `feather validate` on generated config to confirm validity
  10. Print summary: files created, tables configured, next steps
- **Non-interactive mode (FR11.14):**
  - `feather init --non-interactive --source-type sqlserver --connection-string "..." --name client-abc --tables "table1,table2"`
  - Accept all inputs as CLI flags. No prompts.
  - Exit with JSON summary if `--json` is set.
- **Silver stub generation:** Each stub is a `.sql` file:
  ```sql
  -- Silver transform for {table_name}
  -- depends_on: bronze.{table_name}
  SELECT * FROM bronze.{table_name}
  ```
- Use `typer.prompt()` for text input, `typer.confirm()` for yes/no, numbered list for table selection.
- Non-interactive mode: if `--tables` is "all" or omitted, select all discovered tables.

**Definition of Done:**

- [ ] Interactive mode prompts for source type and connection details
- [ ] `source.check()` verifies connectivity before discovery
- [ ] `source.discover()` lists tables with columns
- [ ] User can select specific tables from discovery list
- [ ] `feather.yaml` generated with selected tables and correct strategy inference
- [ ] Silver transform stubs generated in `transforms/silver/`
- [ ] Generated config passes `feather validate`
- [ ] `--non-interactive` mode accepts all inputs as CLI flags
- [ ] `--json` mode outputs JSON summary
- [ ] Full test suite passes

**Verify:**

- `uv run pytest tests/test_init_wizard.py -q`
- `uv run pytest -q`

---

## Open Questions

None — all design decisions resolved.

### Deferred Ideas

- **Quarantine table** for type_changed cast failures (V10+) — currently logged + alerted, rows excluded
- **DQ `row_count` range checks** — min/max row count thresholds (currently only warns on 0)
- **Dedup for database sources** (sqlserver, postgres) — same SQL pattern, straightforward follow-up
- **Multi-file glob for Excel/JSON** — same pattern as CSV glob, straightforward follow-up
- **DQ `freshness` check** — warn if max timestamp is older than threshold
