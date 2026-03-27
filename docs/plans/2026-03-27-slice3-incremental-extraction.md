# Slice 3: Incremental Extraction Implementation Plan

Created: 2026-03-27
Status: VERIFIED
Approved: Yes
Iterations: 0
Worktree: No
Type: Feature

## Summary

**Goal:** Tables with `strategy: incremental` and a `timestamp_column` extract only rows newer than the last successful run's watermark. The watermark advances after each successful load. An optional `filter` field applies a source-side WHERE clause on top of the watermark filter. An `overlap_window_minutes` setting (default: 2) prevents missed rows from clock skew.

**Architecture:** FileSource base class gets a shared `_build_where_clause()` helper. Each source's `extract()` uses the existing `watermark_column`/`watermark_value`/`filter` params to construct a filtered query. Pipeline reads the stored watermark, computes the effective watermark (subtracting overlap), and passes it to `extract()`. A new `load_incremental()` method on DuckDBDestination does partition-overwrite (DELETE rows >= min batch timestamp, then INSERT). State's `write_watermark()` is extended to persist `last_value`.

**Tech Stack:** Python, DuckDB SQL, PyArrow, existing test fixtures + new purpose-built incremental fixture.

## Scope

### In Scope

- `_build_where_clause()` helper in FileSource base class
- Watermark-filtered `extract()` in DuckDBFileSource, CsvSource, SqliteSource
- `load_incremental()` on DuckDBDestination (DELETE + INSERT partition overwrite)
- Pipeline branching: incremental vs full strategy
- `write_watermark()` extended to persist `last_value` (MAX of timestamp column from batch)
- `record_run()` extended to record `watermark_before` and `watermark_after`
- Config validation: `incremental` requires `timestamp_column` (already done in config.py:182)
- Purpose-built test fixture with controlled timestamps
- Unit tests for WHERE clause construction, incremental load, watermark advancement
- Integration tests matching PRD Slice 3 test matrix
- E2E hands_on_test.sh scenarios for incremental extraction

### Out of Scope

- Boundary deduplication / PK hashing (Slice 16)
- Append strategy (Slice 12)
- Column_map (Slice 4)
- SQL Server incremental (Slice 7)
- CLI changes (PRD says "No changes to CLI" for this slice)
- Change detection changes (file-level mtime/hash stays as-is)

## Approach

**Chosen:** Shared WHERE clause helper in FileSource base class

**Why:** All three file sources (DuckDB, CSV, SQLite) use DuckDB SQL under the hood, so the WHERE clause syntax is identical. A single `_build_where_clause()` method eliminates duplication. Each source's `extract()` just appends the result to its query.

**First-run behavior:** When no watermark exists yet, extract all rows via the existing `load_full()` swap pattern. Only subsequent runs use `load_incremental()` (DELETE + INSERT). This reuses the battle-tested full-load path for initial extraction.

**Alternatives considered:**
- *Each source builds its own WHERE clause:* Identical SQL duplicated three times. No benefit since all use DuckDB.
- *Pipeline builds the WHERE clause and passes it:* Breaks encapsulation — pipeline shouldn't know SQL details of each source type.
- *Always use load_incremental() even on first run:* Functionally correct (DELETE of empty table is a no-op) but less efficient than swap for initial full load.

## Context for Implementer

> Write for an implementer who has never seen the codebase.

- **Source protocol:** `src/feather/sources/__init__.py` — `Source.extract()` already accepts `watermark_column`, `watermark_value`, and `filter` params. All three file sources have these params but currently ignore them (always `SELECT *`).
- **FileSource base:** `src/feather/sources/file_source.py` — shared base for DuckDB/CSV/SQLite sources. Already has `detect_changes()` and `_compute_file_hash()`. The new `_build_where_clause()` goes here.
- **DuckDB source query pattern:** `src/feather/sources/duckdb_file.py:79` — `SELECT * FROM source_db.{table}`. WHERE clause appends after this.
- **CSV source query pattern:** `src/feather/sources/csv.py:62` — `SELECT * FROM read_csv(?)`. WHERE clause appends after this.
- **SQLite source query pattern:** `src/feather/sources/sqlite.py:79` — `SELECT * FROM sqlite_scan(?, ?)`. WHERE clause appends after this.
- **Destination load_full():** `src/feather/destinations/duckdb.py:37` — swap pattern (CREATE staging, DROP old, RENAME). `load_incremental()` goes in the same file.
- **Pipeline:** `src/feather/pipeline.py` — `run_table()` currently always calls `source.extract(table.source_table)` then `dest.load_full()`. Needs branching by strategy.
- **State watermark:** `src/feather/state.py:139` — `write_watermark()` currently doesn't set `last_value`. The `_watermarks.last_value` column exists in the schema (line 48) but is never populated.
- **Config:** `src/feather/config.py` — `TableConfig` already has `timestamp_column`, `filter`, and `strategy` fields. `DefaultsConfig` already has `overlap_window_minutes` (default 2). Validation at line 182 already enforces `incremental` requires `timestamp_column`.
- **Timestamp format in client.duckdb:** `icube.SALESINVOICE.ModifiedDate` is `TIMESTAMP WITH TIME ZONE` (IST+5:30). Range: 2025-08-01 to 2025-09-30.
- **sample_erp.duckdb:** `erp.orders.created_at` is `TIMESTAMP` (no TZ). 5 rows, range 2025-01-15 to 2025-02-10. Good base for extending with controlled timestamps.
- **Test pattern:** Tests use real DuckDB fixtures, no mocking. Fixtures copied to `tmp_path` so tests don't modify originals. See `tests/conftest.py`.
- **Hands-on test pattern:** `scripts/hands_on_test.sh` — bash script with `check()` helper. Creates temp dirs, writes configs, runs CLI commands, asserts on output. Currently 62 checks.

**Gotchas:**
- DuckDB file source uses ATTACH (`source_db.{table}`) while CSV/SQLite use function-based reads (`read_csv(?)`, `sqlite_scan(?, ?)`). The WHERE clause works the same way for all three since DuckDB parses them uniformly, but the base query construction differs.
- Watermark values are stored as VARCHAR in `_watermarks.last_value`. Timestamp comparison in WHERE clause works because DuckDB auto-casts string timestamps.
- The overlap window subtracts minutes from a timestamp — need `datetime` arithmetic, not string manipulation.
- `detect_changes()` (file-level mtime/hash) still runs before incremental extract. A file that hasn't changed at all can be skipped entirely even for incremental tables.

## Assumptions

- DuckDB auto-casts VARCHAR watermark values to TIMESTAMP in WHERE clauses — supported by DuckDB docs and verified with client.duckdb fixture. Tasks 1, 3 depend on this.
- `read_csv()` and `sqlite_scan()` support WHERE clauses in the outer SELECT — supported by DuckDB SQL semantics (they produce virtual tables). Tasks 1, 2 depend on this.
- `overlap_window_minutes` default of 2 is sufficient for ERP clock skew — supported by PRD specification. Task 3 depends on this.
- First incremental run with no watermark should extract all rows — supported by PRD end-to-end test spec ("first run → all rows extracted"). Task 3 depends on this.

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Timezone mismatch between stored watermark and source data | Medium | High | Store watermark as ISO string from PyArrow MAX(); DuckDB handles TZ-aware comparison |
| Late-arriving rows outside overlap window | Low | Medium | `overlap_window_minutes` is configurable; document that users should set based on their source system's clock skew |
| Large initial incremental load causes memory pressure | Low | Medium | First run uses existing `load_full()` swap pattern which streams through PyArrow |

## Goal Verification

### Truths

1. A table with `strategy: incremental` and `timestamp_column: ModifiedDate` extracts all rows on first run and sets watermark to MAX(ModifiedDate).
2. A second run with no new source rows extracts 0 rows and leaves watermark unchanged.
3. After adding rows with newer timestamps, only new rows (plus overlap window) are extracted, and watermark advances.
4. A `filter` field causes matching rows to never appear in the destination.
5. The overlap window arithmetic is correct: effective_watermark = stored_watermark - overlap_window_minutes.
6. `_watermarks.last_value` is populated after each successful incremental run.

### Artifacts

- `src/feather/sources/file_source.py` — `_build_where_clause()` method
- `src/feather/sources/duckdb_file.py` — watermark-filtered `extract()`
- `src/feather/sources/csv.py` — watermark-filtered `extract()`
- `src/feather/sources/sqlite.py` — watermark-filtered `extract()`
- `src/feather/destinations/duckdb.py` — `load_incremental()` method
- `src/feather/pipeline.py` — incremental strategy branching
- `src/feather/state.py` — `write_watermark()` with `last_value` support
- `tests/test_incremental.py` — unit + integration tests
- `scripts/hands_on_test.sh` — incremental E2E scenarios

## Progress Tracking

- [x] Task 1: FileSource WHERE clause helper + source extract() updates
- [x] Task 2: DuckDBDestination load_incremental()
- [x] Task 3: Pipeline incremental orchestration + state watermark
- [x] Task 4: Test fixture + unit tests
- [x] Task 5: Integration tests (PRD test matrix)
- [x] Task 6: E2E hands_on_test.sh scenarios

**Total Tasks:** 6 | **Completed:** 6 | **Remaining:** 0

## Implementation Tasks

### Task 1: FileSource WHERE clause helper + source extract() updates

**Objective:** Add `_build_where_clause()` to FileSource and make all three file source `extract()` methods use it to apply watermark and filter conditions.

**Dependencies:** None

**Files:**

- Modify: `src/feather/sources/file_source.py`
- Modify: `src/feather/sources/duckdb_file.py`
- Modify: `src/feather/sources/csv.py`
- Modify: `src/feather/sources/sqlite.py`

**Key Decisions / Notes:**

- `_build_where_clause()` returns a string like `" WHERE col >= 'val' AND (filter)"` or empty string `""`. Callers append it to their base query.
- Watermark value is passed as a string (ISO timestamp). DuckDB handles the cast.
- `filter` is wrapped in parentheses to prevent operator precedence issues (e.g., `status = 'A' OR status = 'B'`).
- Add `ORDER BY {watermark_column}` when watermark is present — ensures deterministic ordering for partition overwrite.
- Each source's `extract()` already has the params; just wire them through to `_build_where_clause()`.

**Definition of Done:**

- [ ] `_build_where_clause("col", "2025-01-01", None)` returns `" WHERE col >= '2025-01-01' ORDER BY col"`
- [ ] `_build_where_clause("col", "2025-01-01", "status <> 1")` returns `" WHERE col >= '2025-01-01' AND (status <> 1) ORDER BY col"`
- [ ] `_build_where_clause(None, None, "status <> 1")` returns `" WHERE (status <> 1)"`
- [ ] `_build_where_clause(None, None, None)` returns `""`
- [ ] DuckDBFileSource.extract() with watermark params returns only matching rows
- [ ] CsvSource.extract() with watermark params returns only matching rows
- [ ] SqliteSource.extract() with watermark params returns only matching rows
- [ ] All existing tests still pass (no regression)

**Verify:**

- `uv run pytest tests/test_sources.py -q`

---

### Task 2: DuckDBDestination load_incremental()

**Objective:** Add `load_incremental()` method that does partition-overwrite: DELETE rows where timestamp >= min batch timestamp, then INSERT new rows.

**Dependencies:** None (can be built in parallel with Task 1)

**Files:**

- Modify: `src/feather/destinations/duckdb.py`

**Key Decisions / Notes:**

- Pattern: `DELETE FROM {target} WHERE {timestamp_column} >= {min_ts}` then `INSERT INTO {target} SELECT *, CURRENT_TIMESTAMP AS _etl_loaded_at, '{run_id}' AS _etl_run_id FROM _arrow_data`
- Wrapped in a transaction for atomicity (same pattern as `load_full()`).
- `min_ts` is computed from the incoming PyArrow batch: `pc.min(data.column(timestamp_column))`.
- If the target table doesn't exist yet (first run went through `load_full()`), `load_incremental()` should handle this gracefully — but in practice pipeline routes first run to `load_full()`, so this is a safety check only.
- Must handle the case where the incoming batch has 0 rows (no new data) — return 0, don't execute DELETE.
- The INSERT must match the target table's column order. Use `CREATE TABLE IF NOT EXISTS` for the target, then INSERT with explicit ETL columns appended.

**Definition of Done:**

- [ ] `load_incremental(table, data, run_id, timestamp_column)` deletes overlapping rows and inserts new ones
- [ ] Returns count of rows inserted
- [ ] Transaction wraps the DELETE + INSERT
- [ ] 0-row batch returns 0 without modifying target
- [ ] All existing tests still pass

**Verify:**

- `uv run pytest tests/test_destinations.py -q`

---

### Task 3: Pipeline incremental orchestration + state watermark

**Objective:** Update `run_table()` to branch on `strategy: incremental`. Read watermark, compute effective watermark with overlap, pass to extract, call appropriate load method, advance watermark.

**Dependencies:** Task 1, Task 2

**Files:**

- Modify: `src/feather/pipeline.py`
- Modify: `src/feather/state.py`

**Key Decisions / Notes:**

- Pipeline flow for incremental:
  1. Read watermark → get `last_value`
  2. If `last_value` is None (first run): extract all rows (no watermark params), `load_full()`, set watermark to MAX(ts_col)
  3. If `last_value` exists: compute `effective_watermark = last_value - overlap_window_minutes`, pass to `extract()`, call `load_incremental()`, advance watermark to MAX(ts_col)
- Overlap arithmetic: parse `last_value` (ISO string) → datetime, subtract `timedelta(minutes=overlap_window_minutes)`, format back to ISO string.
- After successful load, compute `MAX(timestamp_column)` from the loaded PyArrow batch using `pc.max()`.
- `write_watermark()` extended: add `last_value` param, include in INSERT and UPDATE SQL.
- `record_run()` already has `watermark_before` and `watermark_after` params — populate them.
- File-level change detection (`detect_changes()`) still runs first. For incremental tables, a file that hasn't changed at all → skip (no extraction needed).
- Access `config.defaults.overlap_window_minutes` — pass config into `run_table()` (already available).

**Definition of Done:**

- [ ] First incremental run: extracts all rows, watermark set to MAX(timestamp_column)
- [ ] Second run (no new rows): 0 rows extracted, watermark unchanged
- [ ] Run after adding rows: only rows >= effective_watermark extracted
- [ ] `_watermarks.last_value` populated correctly after each incremental run
- [ ] `_runs.watermark_before` and `_runs.watermark_after` recorded
- [ ] Full strategy tables still work unchanged
- [ ] All existing tests still pass

**Verify:**

- `uv run pytest tests/test_pipeline.py tests/test_state.py -q`

---

### Task 4: Test fixture + unit tests

**Objective:** Create a purpose-built test fixture with controlled timestamps and write unit tests for the new components.

**Dependencies:** Task 1, Task 2, Task 3

**Files:**

- Modify: `scripts/create_sample_erp_fixture.py` (extend with timestamp-controlled table)
- Modify: `tests/fixtures/sample_erp.duckdb` (regenerated)
- Create: `tests/test_incremental.py`
- Modify: `tests/conftest.py` (add incremental-specific fixtures)

**Key Decisions / Notes:**

- Add a new table `erp.sales` to sample_erp.duckdb with columns: `sale_id INTEGER, customer_id INTEGER, amount DECIMAL(10,2), status VARCHAR, modified_at TIMESTAMP`. ~10 rows with controlled timestamps spanning a known range (e.g., 2025-01-01 to 2025-01-10, one per day).
- Unit tests cover:
  - `_build_where_clause()` — all four combinations (watermark only, watermark+filter, filter only, neither)
  - `load_incremental()` — partition overwrite correctness, 0-row batch handling, transaction atomicity
  - `write_watermark()` with `last_value` — INSERT and UPDATE paths
  - Overlap window arithmetic — `timedelta` subtraction from ISO timestamp string

**Definition of Done:**

- [ ] `erp.sales` table exists in regenerated sample_erp.duckdb
- [ ] Unit tests for `_build_where_clause()` cover all 4 combinations
- [ ] Unit tests for `load_incremental()` cover normal and edge cases
- [ ] Unit tests for watermark `last_value` persistence
- [ ] All tests pass

**Verify:**

- `uv run pytest tests/test_incremental.py -q`

---

### Task 5: Integration tests (PRD test matrix)

**Objective:** Write integration tests that exercise the full pipeline for incremental extraction, matching the PRD Slice 3 test spec.

**Dependencies:** Task 4

**Files:**

- Modify: `tests/test_incremental.py` (add integration test class)

**Key Decisions / Notes:**

- Tests use the purpose-built fixture, configure a `feather.yaml` with `strategy: incremental`, and run through the pipeline.
- PRD test matrix:
  1. First incremental run → all rows extracted, watermark = MAX(modified_at)
  2. Second run, no new rows → 0 rows extracted, watermark unchanged
  3. Add rows with new timestamps → only new rows + overlap window extracted, watermark advances
  4. Another run, no new rows → 0 rows, watermark unchanged
  5. With `filter: "status <> 'cancelled'"` → filtered rows never in destination
- Each test uses `tmp_path`, copies fixture, writes config, calls `run_table()` or `run_all()`.
- For "add rows" scenario: copy fixture to tmp_path, then INSERT rows with `duckdb.connect()` directly.

**Definition of Done:**

- [ ] Integration test covers all 5 PRD scenarios
- [ ] Tests verify row counts, watermark values, and filter exclusion
- [ ] Tests run in < 5 seconds total
- [ ] All tests pass

**Verify:**

- `uv run pytest tests/test_incremental.py -q`

---

### Task 6: E2E hands_on_test.sh scenarios

**Objective:** Add incremental extraction scenarios to the CLI integration test script.

**Dependencies:** Task 5

**Files:**

- Modify: `scripts/hands_on_test.sh`

**Key Decisions / Notes:**

- New section "S-INCR: Incremental extraction" at end of script (before Summary).
- Scenarios:
  1. Create config with `strategy: incremental`, `timestamp_column: modified_at` pointing at sample_erp.duckdb (copied to temp dir). Run → all rows extracted, output shows success.
  2. Run again → 0 new rows (source unchanged, file-level change detection kicks in first), output shows "skipped (unchanged)".
  3. Modify source (add rows with new timestamps). Run → only new rows extracted.
  4. Create config with `filter: "status <> 'cancelled'"`. Run → verify filtered rows not in destination.
- Use same `check()` / `run_ok()` pattern as existing tests.
- Use DuckDB CLI or Python one-liner to verify destination row counts and watermark values.

**Definition of Done:**

- [ ] At least 6 new checks in hands_on_test.sh covering incremental scenarios
- [ ] `bash scripts/hands_on_test.sh` passes with 0 failures
- [ ] Existing 62 checks still pass
- [ ] Check count updated in CLAUDE.md if needed

**Verify:**

- `bash scripts/hands_on_test.sh`

## Open Questions

None — all resolved during planning.
