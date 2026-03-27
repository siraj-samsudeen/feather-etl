# Contributing conventions

This document describes the file conventions for plans, reviews, and fixes.
Every agent working on this repo should read it before starting.

---

## Document chain

Each piece of work follows this chain:

```
docs/plans/YYYY-MM-DD-<slice>-<topic>.md       ← implementation plan
docs/reviews/YYYY-MM-DD-<slice>-review.md      ← review of that implementation
docs/plans/YYYY-MM-DD-<slice>-fixes.md         ← plan to address review findings
docs/reviews/YYYY-MM-DD-<slice>-review-2.md    ← re-review after fixes
...
```

Links are two-way: every plan has a `Review:` field once reviewed; every review
has a `Plan:` field pointing to what was reviewed and (once it exists) a
`Fixes:` field pointing to the fix plan.

---

## Plan document header

```markdown
# <Title>

Created: YYYY-MM-DD
Status: DRAFT | APPROVED | IN PROGRESS | VERIFIED
Approved: Yes | No | Pending
Slice: N
Type: Feature | Fix | Refactor | Review-fixes
Review: <path to review doc, added after review>
```

## Review document header

```markdown
# <Slice> Review

Created: YYYY-MM-DD
Slice: N
Plan: <path to plan doc>
Commit: <short hash reviewed>
Status: OPEN | RESOLVED — see <fixes plan path>
Prior review: <path, if this is review-2+>
```

---

## Rules for the fixing agent

1. **Never edit findings in a review doc.**  The review is a permanent record.
   Editing it destroys the audit trail.

2. **Create a fixes plan** at `docs/plans/YYYY-MM-DD-<slice>-fixes.md`.
   Scope it to the findings you intend to address.  List any you are deferring
   and why.

3. **Graduate `TestKnownBugs` tests** as you fix each bug:
   - Read the test docstring — it states current wrong behaviour and correct
     post-fix behaviour.
   - Invert the assertion.
   - Remove the `BUG-N` prefix from the test name.
   - Move the test to the appropriate positive-behaviour test class.

4. **Update `scripts/hands_on_test.sh`** BUG-labelled checks to assert the
   corrected behaviour and update the label.

5. **Run both suites before opening for re-review:**
   ```bash
   pytest tests/            # must be all green
   bash scripts/hands_on_test.sh   # must be all green
   ```

6. **Update the review doc Status line** (and only that line) to:
   `RESOLVED — see docs/plans/YYYY-MM-DD-<slice>-fixes.md`

7. **Do not request a re-review yourself.**  Tell the human what was fixed,
   what was deferred, and why.  The human decides when to ask for a re-review.

---

## Rules for the reviewing agent

1. Read `docs/reviews/` — find the most recent review for the slice.  Its
   `Status:` line tells you whether fixes have been applied.

2. Run both test suites first.  If any test fails before you read a line of
   code, note it immediately.

3. Create a new review file `docs/reviews/YYYY-MM-DD-<slice>-review-N.md`.
   Add `Prior review: <path>` to the header.

4. Cover:
   - Which findings from the prior review were fixed correctly.
   - Which were fixed incorrectly or partially.
   - Which are still open.
   - Any new issues introduced by the fixes.
   - New hands-on scenarios to add to `scripts/hands_on_test.sh`.

5. Update artefacts:
   - Add new `TestKnownBugs` tests for any new bugs found.
   - Add new scenarios to `scripts/hands_on_test.sh`.
   - If new fixtures are needed, add a script in `scripts/` to regenerate them.

---

## Test suite conventions

### `tests/test_integration.py`

- `TestKnownBugs` — one test per open bug from the most recent review.
  Docstring format:
  ```
  BUG-N: <one sentence of current wrong behaviour>.
  After fix: <one sentence of correct behaviour>.
  ```
  When fixed: invert assertion, remove `BUG-N` prefix, move to positive class.

- All other test classes cover positive behaviour and edge cases.
  No mocking — use real DuckDB fixtures.

### `scripts/hands_on_test.sh`

- BUG-labelled `check()` calls assert **current wrong behaviour** (they PASS
  while the bug is open, FAIL when fixed — acting as a regression guard).
- When a bug is fixed: invert the condition, change the label from `BUG-N: ...`
  to the correct expected behaviour description.

### Fixtures

| File | Schema | Use for |
|---|---|---|
| `tests/fixtures/client.duckdb` | `icube.*`, 6 tables, ~14K rows | Real-world ERP data, messy schemas |
| `tests/fixtures/client_update.duckdb` | `icube.*`, 3 tables, ~12K rows | Future incremental/append tests |
| `tests/fixtures/sample_erp.duckdb` | `erp.*`, 3 tables, 12 rows | Fast tests, NULL handling, simple schema |
| `tests/fixtures/sample_erp.sqlite` | `erp.*`, 3 tables, 12 rows | SQLite source tests |
| `tests/fixtures/csv_data/` | 3 CSV files (orders, customers, products) | CSV source tests |

To regenerate fixtures:
```bash
python scripts/create_sample_erp_fixture.py        # DuckDB
python scripts/create_csv_sqlite_fixtures.py       # CSV + SQLite
```

---

## Current open reviews

| File | Slice | Status |
|---|---|---|
| [docs/reviews/2026-03-26-slice1-review.md](reviews/2026-03-26-slice1-review.md) | 1 | RESOLVED — see [fixes plan](plans/2026-03-26-slice1-fixes.md) |
