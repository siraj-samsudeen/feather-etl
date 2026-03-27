# Slice 2-3 Review

Created: 2026-03-27
Slice: 2-3 (change detection + incremental extraction)
Plan: [docs/plans/2026-03-27-slice2-change-detection.md](../plans/2026-03-27-slice2-change-detection.md), [docs/plans/2026-03-27-slice3-incremental-extraction.md](../plans/2026-03-27-slice3-incremental-extraction.md)
Commit: 239429e + uncommitted Slice 3 worktree
Status: OPEN
Prior review: [docs/reviews/2026-03-26-slice1-review-2.md](2026-03-26-slice1-review-2.md)

---

## Review method

1. **Spec pass** — reviewed PRD Slice 1-3 sections (`docs/prd.md:1674-1793`, `docs/prd.md:1848-1855`) against current implementation and test matrix coverage.
2. **Code pass** — reviewed orchestrator, state, source, destination, config, CLI, and incremental test files listed in the review request.
3. **Execution pass** — ran both required suites before writing findings.

---

## Test results before review

```bash
uv run pytest -q
  180 passed, 0 failed

bash scripts/hands_on_test.sh
  64 passed, 6 failed
```

The 6 failing checks are environment-related in this run (`ModuleNotFoundError: No module named 'duckdb'` in shell `python -c` checks), not logic regressions in Slice 2/3 paths. Incremental checks S-INCR-1..8 are green.

---

## PRD compliance snapshot

- **Slice 1:** Implemented behavior remains consistent with current expectations.
- **Slice 2:** Two-tier mtime/hash detection is implemented and exercised.
- **Slice 3:** Incremental branch, overlap arithmetic, watermark persistence, and filter behavior are implemented and largely aligned with PRD matrix.
- **Gap:** One Slice 3 correctness issue causes watermark regression in a specific Slice 2 touch-file path (H-1 below).

---

## Findings summary

### HIGH

| ID | File | Issue |
|---|---|---|
| H-1 | `src/feather/pipeline.py` + `src/feather/state.py` | Touch-file skip path clears incremental watermark `last_value` to `NULL` |
| H-2 | `src/feather/config.py` + `src/feather/pipeline.py` | `overlap_window_minutes` is unvalidated; negative values can cause missed rows |

### MEDIUM

| ID | File | Issue |
|---|---|---|
| M-1 | `src/feather/sources/file_source.py`, `src/feather/sources/*.py` | Raw SQL composition from config fields leaves wide SQL-injection / malformed-query surface |
| M-2 | `src/feather/state.py` | DB connections still leak on exceptions in state methods |

### LOW

| ID | File | Issue |
|---|---|---|
| L-1 | `src/feather/pipeline.py`, `src/feather/cli.py` | `write_validation_json()` is called twice on `feather run` success path |

---

## Detailed findings

### H-1 — Touch-file skip path clears incremental watermark

- **Severity:** HIGH
- **Location:** `src/feather/pipeline.py:61-68`, `src/feather/state.py:157-161`
- **What happens:** In the skip path for touch events (mtime changed, hash unchanged), `run_table()` calls `write_watermark()` without `last_value`. `write_watermark()` always updates `last_value = ?`, so incremental tables lose stored watermark (`NULL` overwrite).
- **Why this matters:** Next incremental run behaves like first run (or otherwise bypasses incremental filtering), causing unnecessary large reloads and violating Slice 3 watermark continuity intent.
- **Suggested fix:** Preserve existing `last_value` on touch-skip updates (pass through previous watermark), or make `write_watermark()` perform partial updates (`COALESCE`/conditional field update) rather than unconditional overwrite.
- **Test additions (Known bug):**
  - `tests/test_integration.py::TestKnownBugs::test_H1_touch_skip_preserves_incremental_last_value`
  - `scripts/hands_on_test.sh` new check after S22/S-INCR: touch unchanged incremental source and assert `_watermarks.last_value` unchanged.

### H-2 — `overlap_window_minutes` missing validation can miss data

- **Severity:** HIGH
- **Location:** `src/feather/config.py:235-239`, `src/feather/pipeline.py:82-85`
- **What happens:** `overlap_window_minutes` accepts any value from YAML. If negative, effective watermark moves forward instead of backward (`T - (-N)`), shrinking extraction window and potentially missing late-arriving rows.
- **Why this matters:** This is a data-correctness risk (silent under-extraction) with no validation error.
- **Suggested fix:** Validate `overlap_window_minutes` as integer `>= 0` during config validation; reject invalid values with a clear message.
- **Test additions (Known bug):**
  - `tests/test_integration.py::TestKnownBugs::test_H2_negative_overlap_window_rejected`
  - Positive test in `tests/test_incremental.py` for `0` and standard positive values.

### M-1 — Raw SQL composition from config creates unsafe/malformed query surface

- **Severity:** MEDIUM
- **Location:** `src/feather/sources/file_source.py:45-53`, `src/feather/sources/duckdb_file.py:79`, `src/feather/sources/csv.py:61`, `src/feather/sources/sqlite.py:78`
- **What happens:** `watermark_column`, `filter`, `source_table`, and file/table literals are interpolated directly into SQL strings.
- **Why this matters:** Trusted-config deployments may accept this risk, but today there is no guardrail. Malformed config values can break queries; hostile config can execute unintended SQL in process context.
- **Suggested fix:**
  - Treat `filter` as trusted advanced expression but validate/reject semicolons and comment tokens at minimum.
  - Validate/quote identifiers for `watermark_column` and `source_table`.
  - Use parameterized literals for paths and non-identifier values where possible.
- **Test additions (Known bug):**
  - `tests/test_integration.py::TestKnownBugs::test_M1_filter_rejects_statement_separators`
  - `tests/test_incremental.py` case for quoted identifier timestamp column.

### M-2 — State DB connection leak pattern remains on exception paths

- **Severity:** MEDIUM
- **Location:** `src/feather/state.py:32-226` (all DB-touching methods)
- **What happens:** Methods call `con.close()` on normal path, but many do not protect close in `finally`. Exceptions before explicit close leak handles.
- **Why this matters:** Under repeated failures this can accumulate open handles and destabilize long-running CLI loops/tests.
- **Suggested fix:** Wrap each method in `try/finally` (or `contextlib.closing`) to guarantee `con.close()`.
- **Test additions:**
  - Fault-injection unit tests around state methods are optional; practical guard is repeated-failure integration test ensuring no handle exhaustion.

### L-1 — Duplicate validation JSON write on run path

- **Severity:** LOW
- **Location:** `src/feather/cli.py:106-108`, `src/feather/pipeline.py:193-195`
- **What happens:** `_load_and_validate()` writes `feather_validation.json`, then `run_all()` writes it again immediately.
- **Why this matters:** No correctness impact, but unnecessary file churn and confusing ownership of validation artifact.
- **Suggested fix:** Keep write responsibility in one layer (prefer CLI), remove duplicate from `run_all()`.

---

## Test matrix coverage notes (Slice 3)

Matrix scenarios in `docs/prd.md:1848-1855` are mostly covered in `tests/test_incremental.py` and S-INCR checks. Coverage gaps worth adding:

1. Touch-file unchanged path for incremental tables preserving watermark (`H-1`).
2. Negative/invalid overlap config guard (`H-2`).
3. Safety/validation tests for `filter` and `timestamp_column` SQL construction (`M-1`).

---

## Overall verdict

Slices 2 and 3 are close to spec and functionally strong on happy paths. The one high-priority correctness bug to fix before calling this production-safe for incremental workloads is **H-1** (watermark nulling on touch-skip). **H-2** should be fixed in the same pass to prevent misconfiguration-driven data loss.
