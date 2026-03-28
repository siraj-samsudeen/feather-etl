# MVP Test Pass Bugfixes

Created: 2026-03-28
Status: VERIFIED
Approved: Yes
Iterations: 0
Worktree: No
Type: Bugfix

## Summary

**Symptom:** MVP test pass has 5 failing checks across 3 issues:
1. Gold VIEW not rematerialized as TABLE on dev→prod mode switch (check 5.2)
2. `hands_on_test.sh` fails 6 checks with `ModuleNotFoundError: No module named 'duckdb'`
3. Test guide doesn't account for PRD-mandated `_etl_loaded_at`/`_etl_run_id` metadata columns (checks 1.1, 4.1, 4.3)

**Trigger:** Running full MVP test matrix after mode feature merge

**Root Cause:**

| # | File | Root Cause | Type |
|---|------|-----------|------|
| 1 | `pipeline.py:250` | `any(r.status == "success")` gate excludes skipped tables — transforms never run on mode switch when data unchanged | Code bug |
| 2 | `scripts/hands_on_test.sh:777,829,847,859,871,907` | `.venv/bin/python -c` fails in environments where `.venv` doesn't have duckdb (worktrees, fresh checkouts) | Script bug |
| 3 | `docs/testing-feather-etl.md` | Guide says "only the renamed columns" / "all source columns" without mentioning metadata columns that PRD line 544 mandates on every row | Guide mismatch |

**Not bugs:** Metadata columns are PRD-mandated (`_etl_loaded_at`, `_etl_run_id` on every loaded row regardless of target schema). DuckDB fixture schema qualification (`erp.erp.customers`) is a test harness issue, not a code issue.

## Investigation

- `pipeline.py:250-251`: `any_success = any(r.status == "success" for r in results)` — when all tables are skipped (unchanged data), this is `False`. The entire transform block (lines 252-278) is skipped. Gold VIEWs created in dev mode persist even when mode switches to prod.
- `transforms.py:169-170`: `execute_transforms()` correctly handles `force_views` parameter. `rebuild_materialized_gold()` correctly creates gold TABLEs. The bug is that these functions are never called when extraction is skipped.
- `duckdb.py:52-55, 97-100`: Both `load_full()` and `load_incremental()` add `_etl_loaded_at` and `_etl_run_id` — matching PRD requirement FR4.5 line 544.
- `test_transforms.py:799`: `test_run_no_rebuild_when_all_skipped` asserts gold TABLE persists across skipped runs, but doesn't verify the transform block was skipped vs re-ran. Test will still pass with the fix.

## Fix Approach

**Chosen:** Expand gate condition to include skipped tables

Change `any(r.status == "success")` → `any(r.status in ("success", "skipped"))`. Transforms run whenever extraction wasn't a total failure. Slightly wasteful (rebuilds gold TABLE even when nothing changed in same-mode runs), but correct and simple.

**Why:** 1-line change with clear semantics. Covers mode-switch and normal scenarios. Only skips transforms when ALL tables fail (protecting against corrupt data).

**Alternatives considered:**
- Always run transforms (remove gate entirely) — rejected: runs against potentially corrupt data when all extractions fail
- Mode-aware gate (track mode in state) — rejected: adds state tracking complexity for a narrow edge case

**Files:** `src/feather/pipeline.py`, `scripts/hands_on_test.sh`, `docs/testing-feather-etl.md`
**Tests:** `tests/test_transforms.py` (update existing test, add mode-switch regression test)

## Progress

- [x] Task 1: Fix gold rematerialization gate and add regression test
- [x] Task 2: Fix hands_on_test.sh python invocations
- [x] Task 3: Update test guide for metadata columns
- [x] Task 4: Verify all suites pass
      **Tasks:** 4 | **Done:** 4

## Tasks

### Task 1: Fix gold rematerialization gate and add regression test

**Objective:** Change transform gate in `run_all()` so transforms run when tables are skipped, and add test for dev→prod mode switch scenario.
**Files:** `src/feather/pipeline.py`, `tests/test_transforms.py`
**TDD:** Write mode-switch regression test → verify FAILS → change gate condition → verify all PASS
**Details:**
- In `pipeline.py:250`, change `any(r.status == "success" for r in results)` to `any(r.status in ("success", "skipped") for r in results)`
- Rename variable from `any_success` to `any_ok` for clarity
- New test: set up source + gold transform, run in dev mode (gold = VIEW), switch to prod mode and run again (all skipped), verify gold is now a TABLE
- Update `test_run_no_rebuild_when_all_skipped` docstring — behavior changes (gold IS rebuilt on skip, but test assertion still passes since it only checks existence)
**Verify:** `uv run pytest tests/test_transforms.py -q`

### Task 2: Fix hands_on_test.sh python invocations

**Objective:** Replace all `.venv/bin/python -c` with `uv run python -c` for portability.
**Files:** `scripts/hands_on_test.sh`
**Details:** Replace 6 occurrences at lines 777, 829, 847, 859, 871, 907
**Verify:** `bash scripts/hands_on_test.sh` — all 70 checks should pass

### Task 3: Update test guide for metadata columns

**Objective:** Add note to test guide that `_etl_loaded_at` and `_etl_run_id` are always present per PRD.
**Files:** `docs/testing-feather-etl.md`
**Details:**
- Section 1 (Source Types): add note that all loaded tables include `_etl_loaded_at` and `_etl_run_id` metadata columns
- Section 5 (Pipeline Modes, prod with column_map): clarify that mapped columns + metadata columns are present (not ONLY mapped columns)
**Verify:** Read the updated guide

### Task 4: Verify all suites pass

**Objective:** Full regression check
**Verify:** `uv run pytest -q && bash scripts/hands_on_test.sh && ruff check .`
