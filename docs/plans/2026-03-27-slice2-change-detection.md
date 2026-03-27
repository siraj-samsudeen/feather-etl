# Slice 2: Change Detection Implementation Plan

Created: 2026-03-27
Status: VERIFIED
Approved: Yes
Iterations: 0
Worktree: Yes
Type: Feature

## Summary

**Goal:** A second `feather run` on an unchanged source file skips extraction entirely and records `skipped`. Only re-extracts when the file actually changed. Saves time and avoids unnecessary writes during iterative development.

**Architecture:** `FileSource.detect_changes()` uses a two-tier check (mtime first, MD5 hash if mtime differs) against watermark state. Pipeline calls this before `extract()`, records `status: skipped` if unchanged, and persists new mtime/hash via `ChangeResult.metadata` after successful loads.

**Tech Stack:** Python stdlib (`os.path.getmtime`, `hashlib.md5`), existing DuckDB state tables.

## Scope

### In Scope

- Implement real `detect_changes()` in `FileSource` base class (replacing Slice 1 stub)
- Template method `_source_path_for_table()` for per-source file resolution
- CsvSource override: per-file change detection (not whole directory)
- Extend `StateManager.write_watermark()` to persist `last_file_mtime` and `last_file_hash`
- Pipeline integration: call `detect_changes()` before `extract()`, skip if unchanged
- Record `status: skipped` in `_runs` table when file unchanged
- CLI output: show `table_name: skipped (unchanged)` in same line format
- CLI summary: separate count format — `N/M tables extracted, K skipped`
- Unit tests for detect_changes logic, integration tests for pipeline skip behavior
- E2E hands-on test scenarios matching PRD test matrix

### Out of Scope

- Incremental strategy (Slice 3)
- Multi-file CSV tables (glob patterns, day-wise partitions) — current model is 1 file = 1 table
- SQL Server change detection (CHECKSUM_AGG + COUNT — Slice 7)
- column_map, filter, any config schema changes
- Source protocol interface changes (detect_changes signature already exists)

### Deferred Ideas

- **Multi-file CSV tables** (e.g., `sales_*.csv` → single table): would require config schema + Source protocol changes. Candidate for Slice 6.
- **Duplicate CSV detection**: same data placed under different filenames, or overlapping date-range re-exports in the same directory. Would need content-aware deduplication (hash-based or row-level). Candidate for Slice 6 or a DQ check in Slice 9.

## Approach

**Chosen:** Template method in FileSource base class

**Why:** All file-based sources share the same mtime→hash detection logic. The only variation is _which file_ to check per table. A template method (`_source_path_for_table`) lets subclasses customize the path while the base class owns the algorithm. Minimal code, no duplication.

**Alternatives considered:**
- *Accept path parameter in detect_changes()*: Moves file path knowledge out of the source into the pipeline — violates encapsulation. Pipeline shouldn't know how source types map tables to files.
- *Pipeline computes mtime/hash independently*: Duplicates file resolution logic between source and pipeline. Two places to maintain.

## Context for Implementer

> Write for an implementer who has never seen the codebase.

- **Patterns to follow:**
  - `FileSource` base class at `src/feather/sources/file_source.py` — all file-based sources inherit from this. Currently has a stub `detect_changes()` at line 27 that always returns `changed=True`.
  - `ChangeResult` dataclass at `src/feather/sources/__init__.py:22` — already has `changed`, `reason`, and `metadata` fields. `metadata` dict is the vehicle for passing mtime/hash from source to pipeline.
  - `Source` protocol at `src/feather/sources/__init__.py:30` — `detect_changes(table, last_state)` signature already defined.
  - `StateManager.write_watermark()` at `src/feather/state.py:139` — has `**kwargs` but ignores them. `_watermarks` table already has `last_file_mtime DOUBLE` and `last_file_hash VARCHAR` columns (created in Slice 1, unpopulated).

- **Conventions:**
  - All sources inherit `FileSource` → `__init__(path: Path)` stores `self.path`
  - DuckDB/SQLite: `self.path` is the single database file (many tables inside)
  - CSV: `self.path` is the directory, each `.csv` file inside is a table
  - Tests use real DuckDB fixtures in `tests/fixtures/`, no mocking (per CLAUDE.md)
  - Integration test fixtures: `conftest.py` has `client_db`, `csv_data_dir`, `sqlite_db` fixtures

- **Key files:**
  - `src/feather/sources/file_source.py` — base class, main change target
  - `src/feather/sources/csv.py` — needs `_source_path_for_table` override
  - `src/feather/state.py` — `write_watermark()` needs mtime/hash params
  - `src/feather/pipeline.py` — `run_table()` needs detect_changes + skip logic
  - `src/feather/cli.py` — `run` command output needs skipped handling

- **Gotchas:**
  - `write_watermark()` currently has `**kwargs` but doesn't use them — the UPDATE and INSERT SQL only reference `strategy` and `last_run_at`. Must fix both paths.
  - For DuckDB/SQLite sources, one file contains many tables. Changing any data in the file changes the mtime for ALL tables. This is expected — file-level granularity, not table-level.
  - `RunResult.status` is a string enum: `"success"`, `"failure"`, `"skipped"`. The `"skipped"` value is already defined in the dataclass docstring but never produced.
  - `_runs` table has `rows_skipped INTEGER` column — for change detection skips, we record `status: skipped` (the entire table was skipped), which is different from `rows_skipped` (individual rows skipped during load).

- **Domain context:** File-based ERP exports are often re-run during development. Without change detection, every `feather run` re-extracts everything even if the source hasn't changed. The mtime check is O(1), the hash check is O(file_size) but only triggered when mtime actually differs (handles `touch` without content change).

## Assumptions

- `_watermarks` table already has `last_file_mtime` and `last_file_hash` columns — supported by `state.py:51-52` DDL. Tasks 1-4 depend on this.
- `ChangeResult.metadata` dict is the right vehicle to pass computed mtime/hash from source to pipeline — supported by `__init__.py:27` field definition. Tasks 1, 3 depend on this.
- `RunResult` status string `"skipped"` is a valid status — supported by `pipeline.py:19` docstring. Tasks 3, 4 depend on this.
- CSV table name matches the filename (e.g., `orders.csv`) — supported by `csv.py:37` in `discover()`. Task 1 depends on this.
- All file-based sources have `self.path` set by `FileSource.__init__` — supported by `file_source.py:21`. Task 1 depends on this.

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Large files slow down MD5 hashing | Low | Medium | First run always hashes (unavoidable to establish baseline). Subsequent runs only hash when mtime changes. Typical ERP exports are <100MB; first-run hash of 100MB takes ~0.3s on modern hardware. |
| File modified between detect_changes() and extract() (TOCTOU) | Very Low | Low | Acceptable for local ETL. Worst case: next run detects the change. |
| DuckDB file mtime changes from internal compaction (no data change) | Low | Low | Hash check catches this — content unchanged means skip. This is exactly the `touch` scenario the PRD tests for. |

## Goal Verification

### Truths

1. Running `feather run` twice on an unchanged source produces `status: skipped` on the second run
2. Modifying the source file and running again produces `status: success` with rows re-loaded
3. Touching a file (mtime changes, content identical) produces `status: skipped` on re-run (hash unchanged)
4. First run always extracts (no prior watermark)
5. `_watermarks` table contains populated `last_file_mtime` and `last_file_hash` after a successful run
6. CLI output shows skipped tables in `table_name: skipped (unchanged)` format
7. CLI summary uses separate count: `N/M tables extracted, K skipped`

### Artifacts

| Truth | Supporting Artifact |
|-------|-------------------|
| 1, 2, 3, 4 | `tests/test_pipeline.py` — integration tests for skip behavior |
| 1, 2, 3, 4 | `scripts/hands_on_test.sh` — E2E CLI scenarios |
| 5 | `tests/test_state.py` — watermark persistence tests |
| 1, 2, 3 | `tests/test_sources.py` — detect_changes unit tests |
| 6, 7 | `tests/test_cli.py` or `scripts/hands_on_test.sh` — CLI output verification |

## Progress Tracking

- [x] Task 1: FileSource.detect_changes() + _source_path_for_table()
- [x] Task 2: StateManager.write_watermark() — persist mtime/hash
- [x] Task 3: Pipeline skip logic
- [x] Task 4: CLI output + hands-on tests

**Total Tasks:** 4 | **Completed:** 4 | **Remaining:** 0

## Implementation Tasks

### Task 1: FileSource.detect_changes() + _source_path_for_table()

**Objective:** Replace the Slice 1 stub with real two-tier change detection (mtime → MD5 hash) and add the template method for file path resolution.

**Dependencies:** None

**Files:**

- Modify: `src/feather/sources/file_source.py`
- Modify: `src/feather/sources/csv.py`
- Test: `tests/test_sources.py`

**Key Decisions / Notes:**

- Add `_source_path_for_table(self, table: str) -> Path` to `FileSource`, returning `self.path`. CsvSource overrides to return `self.path / table`.
- `detect_changes()` algorithm:
  1. Resolve file path via `_source_path_for_table(table)`
  2. Get current mtime via `os.path.getmtime()`
  3. If `last_state` is None → compute hash, return `ChangeResult(changed=True, reason="first_run", metadata={file_mtime, file_hash})`
  4. If mtime == last_state["last_file_mtime"] → return `ChangeResult(changed=False, reason="unchanged")` — no metadata, no watermark write needed
  5. Compute MD5 hash of file contents
  6. If hash == last_state["last_file_hash"] → return `ChangeResult(changed=False, reason="unchanged", metadata={file_mtime, file_hash})` — metadata IS populated (mtime changed but hash same = touch scenario). Pipeline must update watermark with new mtime so next run skips re-hashing.
  7. If hash differs → return `ChangeResult(changed=True, reason="hash_changed", metadata={file_mtime, file_hash})`
- Metadata rules: step 4 = empty metadata (nothing changed). Steps 3, 6, 7 = metadata populated with computed values.
- Update `ChangeResult` reason docstring at `__init__.py:26` to list only the three values this implementation produces: `"first_run"`, `"unchanged"`, `"hash_changed"`. Remove `"mtime_changed"` (never emitted).
- Import `hashlib` and `os` in file_source.py
- MD5 hash computed by reading file in 8KB chunks (`hashlib.md5()` with `update()` loop)

**Definition of Done:**

- [ ] `detect_changes()` returns `changed=True` with `reason="first_run"` when no prior state
- [ ] `detect_changes()` returns `changed=False` with `reason="unchanged"` when file untouched
- [ ] `detect_changes()` returns `changed=False` when mtime changed but hash identical (touch scenario)
- [ ] `detect_changes()` returns `changed=True` with `reason="hash_changed"` when content changed
- [ ] CsvSource resolves per-file path (e.g., `orders.csv` → `self.path / "orders.csv"`)
- [ ] DuckDB/SQLite use base class path (whole file)
- [ ] `metadata` dict contains `file_mtime` and `file_hash` when computed
- [ ] All tests pass

**Verify:**

- `uv run pytest tests/test_sources.py -q`

---

### Task 2: StateManager.write_watermark() — persist mtime/hash

**Objective:** Extend `write_watermark()` to accept and persist `last_file_mtime` and `last_file_hash` in the `_watermarks` table.

**Dependencies:** None (can be done in parallel with Task 1)

**Files:**

- Modify: `src/feather/state.py`
- Test: `tests/test_state.py`

**Key Decisions / Notes:**

- Add explicit keyword args `last_file_mtime: float | None = None` and `last_file_hash: str | None = None` to `write_watermark()`
- UPDATE path: add `last_file_mtime = ?, last_file_hash = ?` to the SET clause
- INSERT path: add the two columns to the INSERT statement
- Keep `**kwargs` for forward compatibility but still don't use it
- Follow existing pattern at `state.py:139-164`

**Definition of Done:**

- [ ] `write_watermark(name, strategy, last_file_mtime=1234.5, last_file_hash="abc")` persists both values
- [ ] `read_watermark(name)` returns dict with `last_file_mtime` and `last_file_hash` populated
- [ ] Calling without mtime/hash still works (backwards compatible — values stay NULL)
- [ ] Existing watermark tests still pass
- [ ] All tests pass

**Verify:**

- `uv run pytest tests/test_state.py -q`

---

### Task 3: Pipeline skip logic

**Objective:** Integrate change detection into the pipeline so unchanged files are skipped with `status: skipped` in the run record.

**Dependencies:** Task 1, Task 2

**Files:**

- Modify: `src/feather/pipeline.py`
- Test: `tests/test_pipeline.py`

**Key Decisions / Notes:**

- In `run_table()`, after creating the source but before `extract()`:
  1. `wm = state.read_watermark(table.name)` — get last state
  2. `change = source.detect_changes(table.source_table, last_state=wm)` — check for changes
  3. If `not change.changed`:
     - Record run with `status="skipped"` and `rows_extracted=0, rows_loaded=0`
     - If `change.metadata` is non-empty (touch scenario — mtime changed but hash identical, algorithm step 6), call `write_watermark()` with new mtime AND existing hash so next run skips re-hashing. If `change.metadata` is empty (simple unchanged, algorithm step 4), no watermark write needed.
     - Return `RunResult(status="skipped")`
  4. If `change.changed`: proceed with existing extract→load flow
  5. After successful load: call `write_watermark()` with `last_file_mtime` and `last_file_hash` from `change.metadata`
- The `source.detect_changes()` is called with `table.source_table` (not `table.name`) — this is the source-side table identifier that `_source_path_for_table()` uses
- Integration tests: use `client_db` fixture, run pipeline twice, assert second run is skipped. Then modify source file, run again, assert success.

**Definition of Done:**

- [ ] Second `run_table()` on unchanged source returns `status="skipped"`
- [ ] After modifying source file, `run_table()` returns `status="success"` with rows re-loaded
- [ ] `_runs` table records `status: skipped` entries
- [ ] `_watermarks` table has populated `last_file_mtime` and `last_file_hash` after first successful run
- [ ] Touch scenario: mtime changes but hash identical → skipped
- [ ] First run always extracts (no prior watermark → changed=True)
- [ ] Failed extractions don't update mtime/hash watermarks
- [ ] All tests pass

**Verify:**

- `uv run pytest tests/test_pipeline.py -q`
- `uv run pytest tests/ -q`

---

### Task 4: CLI output + hands-on tests

**Objective:** Update CLI display for skipped tables and add E2E test scenarios to hands_on_test.sh matching the PRD test matrix.

**Dependencies:** Task 3

**Files:**

- Modify: `src/feather/cli.py`
- Modify: `scripts/hands_on_test.sh`

**Key Decisions / Notes:**

- CLI `run` command output changes:
  - Skipped tables: `  table_name: skipped (unchanged)`
  - Summary line: `N/M tables extracted, K skipped.` (separate count as user requested)
  - **Exit-code fix (CRITICAL):** Replace the exit-code guard at `cli.py:118` (`if successes < len(results)`) with `failures = sum(1 for r in results if r.status == "failure")` / `if failures > 0: raise typer.Exit(code=1)`. The current condition conflates skipped with failed — without this change, any skipped table triggers exit code 1.
- hands_on_test.sh scenarios to add (matching PRD E2E test):
  ```
  S19: feather run         → status: success, rows loaded
  S20: feather run again   → status: skipped (file unchanged)
  S21: modify source file, feather run again → status: success, rows re-loaded
  S22: touch source file, feather run again  → status: skipped (hash unchanged)
  ```
- Use the existing `sample_erp.duckdb` fixture (fast, 12 rows) for the E2E test
- Number the new scenarios after the last existing scenario in hands_on_test.sh

**Definition of Done:**

- [ ] CLI shows `table_name: skipped (unchanged)` for skipped tables
- [ ] CLI summary: `N/M tables extracted, K skipped.`
- [ ] Skipped tables don't trigger non-zero exit code
- [ ] All 4 hands-on test scenarios pass
- [ ] `uv run pytest tests/ -q` passes
- [ ] `bash scripts/hands_on_test.sh` passes

**Verify:**

- `bash scripts/hands_on_test.sh`
- `uv run pytest tests/ -q`

## Open Questions

None — all design decisions resolved.
