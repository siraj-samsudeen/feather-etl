# feather-etl — agent context

## What this repo is

A config-driven Python ETL package for extracting data from heterogeneous ERP
sources (SQL Server, SAP, custom Indian ERPs) into local DuckDB.  See
`docs/prd.md` for full requirements and `README.md` for architecture and CLI
reference.

## Before you write a single line of code

Run the test suites and confirm both are green:

```bash
pytest tests/                  # currently: 92 tests
bash scripts/hands_on_test.sh  # currently: 51 checks
```

If anything is red before you touch anything, report it immediately — don't
paper over it.

## How work is organised

Full conventions are in `docs/CONTRIBUTING.md`.  The short version:

```
docs/plans/YYYY-MM-DD-<slice>-<topic>.md    ← what you plan to build/fix
docs/reviews/YYYY-MM-DD-<slice>-review.md   ← what a reviewer found
docs/plans/YYYY-MM-DD-<slice>-fixes.md      ← plan to address review findings
```

**Always create a plan doc before starting work.**  Use today's date.
Copy the header format from any existing plan in `docs/plans/`.

## Current state

| Slice | Plan | Review | Status |
|---|---|---|---|
| 1 — foundation | `docs/plans/2026-03-26-slice1-foundation.md` | `docs/reviews/2026-03-26-slice1-review.md` | Review OPEN — fixes not started |

## If you are fixing bugs from a review

1. Read the review doc listed above.  Every finding has a severity, file, and
   suggested fix.
2. Create `docs/plans/YYYY-MM-DD-slice1-fixes.md` scoping what you will and
   won't address, and why.
3. As you fix each bug, find its test in `tests/test_integration.py` →
   `TestKnownBugs`.  The docstring tells you exactly what to invert.  Invert
   the assertion, remove the `BUG-N` prefix, move to the right test class.
4. Update the matching `BUG-N` check in `scripts/hands_on_test.sh` to assert
   the corrected behaviour.
5. Never edit findings in the review doc.  Only touch its `Status:` line when
   completely done.
6. Don't request a re-review — tell the human what's fixed and what's deferred.
   The human decides when to ask for a re-review.

## If you are implementing a new slice

1. Read `docs/prd.md` for requirements.
2. Check `docs/plans/` for any existing plan for the slice.  If none, create one.
3. Keep production code focused — no speculative features.
4. All tests use real DuckDB fixtures, no mocking.  See fixture table below.

## Test fixtures

| File | Schema | Rows | Use for |
|---|---|---|---|
| `tests/fixtures/client.duckdb` | `icube.*`, 6 tables | ~14K | Real-world ERP, messy schemas |
| `tests/fixtures/client_update.duckdb` | `icube.*`, 3 tables | ~12K | Future incremental tests |
| `tests/fixtures/sample_erp.duckdb` | `erp.*`, 3 tables | 12 | Fast tests, NULL handling |

Regenerate `sample_erp.duckdb`: `python scripts/create_sample_erp_fixture.py`

## Dev setup

```bash
uv sync                        # install deps
pytest tests/                  # run tests
bash scripts/hands_on_test.sh  # run CLI-level checks
```
