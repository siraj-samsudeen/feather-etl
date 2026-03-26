# Slice 1 Review

Created: 2026-03-26
Slice: 1 (foundation)
Plan: [docs/plans/2026-03-26-slice1-foundation.md](../plans/2026-03-26-slice1-foundation.md)
Commit: 869a372

## What was reviewed

The full Slice 1 implementation: 456 LOC across 11 source files, 64 original tests.
Two review passes were performed:

1. **Static review** — read every source file and test file, identified issues by
   severity without running any code.
2. **Hands-on review** — installed the package in a fresh directory, ran every CLI
   command in the documented operator workflow against both the real Icube fixture
   and a synthetic ERP database created for the review.

---

## Artefacts produced by this review

All artefacts are committed and should be used by the next agent working on this
codebase.

### `tests/fixtures/sample_erp.duckdb`

A synthetic ERP database created during hands-on testing.  Intentionally small
(12 rows) but covers schema features the Icube fixture does not exercise in
isolation.

| Table | Rows | Notable |
|---|---|---|
| `erp.orders` | 5 | DATE, DECIMAL, VARCHAR, TIMESTAMP |
| `erp.customers` | 4 | DECIMAL credit_limit |
| `erp.products` | 3 | `stock_qty` is NULL on row 3 — tests NULL pass-through |

To regenerate from scratch:
```bash
python scripts/create_sample_erp_fixture.py
```

### `scripts/create_sample_erp_fixture.py`

Idempotent Python script that creates `tests/fixtures/sample_erp.duckdb`.
Run it if the fixture file is ever corrupted or needs schema changes.

### `scripts/hands_on_test.sh`

A 16-scenario shell script (51 checks) that reproduces the complete hands-on
review workflow from a cold `mktemp -d` directory.  Run from repo root with the
venv active:

```bash
source .venv/bin/activate
bash scripts/hands_on_test.sh
```

Scenarios covered:

| ID | What it tests |
|---|---|
| S1 | `feather init` scaffold, re-init guard, `init .` empty-name bug |
| S2 | `feather validate` happy path, `feather_validation.json` content |
| S3 | Validate failure cases: missing source, invalid strategy, BUG-2, BUG-7 |
| S4 | `feather discover` against Icube fixture |
| S5 | `feather setup` + `feather run` + `feather status` full flow; row counts; metadata cols; idempotency |
| S6 | Partial failure: error isolation, exit code (BUG-5), error stored in state |
| S7 | `feather run` without prior `feather setup` auto-creates DBs |
| S8 | `sample_erp` fixture: 3-table run, NULL pass-through, row counts |
| S9 | `tables/` directory merge |
| S10 | `--config` path resolution from a different CWD |
| S11 | BLOB columns (INVITEM), space-in-column-name (`Round Off` in SALESINVOICEMASTER) |
| S12 | `feather status` edge cases: before setup, after setup/before run, after run |
| S13 | BUG-3: missing `feather.yaml` shows raw traceback |
| S14 | BUG-1: error message duplicated on stderr and stdout |
| S15 | BUG-2: `csv` source type passes validate but crashes on run |
| S16 | BUG-7: hyphenated `target_table` passes validate, crashes on run |

**BUG-labelled checks intentionally PASS today** (they assert the current wrong
behaviour).  When a bug is fixed its check will flip to FAIL, acting as a
regression guard.  The check label and description tell you exactly what to
invert.

### `tests/test_integration.py`

28 new pytest tests derived from the hands-on findings.  They run as part of the
normal `pytest tests/` suite (92 total, 92 pass).

Test classes and what they cover:

| Class | Tests | Purpose |
|---|---|---|
| `TestSampleErpFixture` | 3 | Fixture integrity — row counts, NULL, schema |
| `TestSampleErpFullPipeline` | 7 | Full run, row counts, NULL, metadata cols, idempotency, state |
| `TestClientFixtureEdgeCases` | 4 | `Round Off` col, BLOBs (INVITEM), all 6 Icube tables, discover |
| `TestErrorIsolation` | 3 | Good table survives bad table; error in state; bronze not polluted |
| `TestTablesDirectoryMerge` | 2 | `tables/` subdir merge; combined inline + dir tables |
| `TestKnownBugs` | 9 | Pins current wrong behaviour for BUG-2, BUG-3, BUG-5, BUG-7, BUG-8 |

`TestKnownBugs` tests are self-documenting: each docstring explains the current
wrong behaviour, the correct post-fix behaviour, and which assertion to invert.
When a bug is fixed: invert the assertion, remove the `BUG-N` prefix from the
test name, and move it to the appropriate positive-behaviour test class.

---

## Findings summary

### Static review findings

Findings are grouped by severity.  Full detail (file + line + suggested fix) is
in the agent conversation that produced this review.

#### CRITICAL
| ID | File | Issue |
|---|---|---|
| C-1 | `destinations/duckdb.py` | SQL injection via unquoted table/schema identifiers |
| C-2 | `destinations/duckdb.py` | `IndexError` + resource leak when `target_table` has no schema prefix |
| C-3 | `state.py` | Connection leak on exception in all DB-touching methods |
| C-4 | `pipeline.py` | `StateManager` and `DuckDBDestination` re-created per table inside loop |

#### HIGH
| ID | File | Issue |
|---|---|---|
| H-1 | `destinations/duckdb.py` | TOCTOU race in `chmod` after `connect` |
| H-2 | `sources/duckdb_file.py` | Connection leak on exception in `discover()` / `get_schema()` |
| H-3 | `sources/duckdb_file.py` | `extract()` silently ignores `columns`, `filter`, `watermark_*` params |
| H-4 | `state.py` | Connection leak on exception in `init_state()` |
| H-5 | `pipeline.py` | `run_id` contains `:` and `+` — safe now but fragile pattern |
| H-6 | `state.py` | Duplicate-timestamp race in `get_status()` MAX join |
| H-7 | `destinations/duckdb.py` | Staging table zombie if ROLLBACK fails |
| H-8 | `config.py` | No validation that `incremental`/`append` requires `timestamp_column` |

#### MEDIUM (selected)
| ID | File | Issue |
|---|---|---|
| M-5 | `state.py` | Non-atomic SELECT-then-INSERT in `write_watermark()` — use UPSERT |
| M-6 | `pipeline.py` | `strategy` field read but not dispatched — `incremental` silently full-loads |
| M-7 | `cli.py` | `write_validation_json` called twice on success path |
| M-8 | `cli.py` | `feather run` exits 0 on partial or total failure |
| M-10 | `config.py` | Duplicate table names across `tables/*.yaml` files silently both loaded |
| M-12 | `config.py` | Missing required YAML fields raise raw `KeyError`, not `ValueError` |

#### LOW / NIT (selected)
| ID | File | Issue |
|---|---|---|
| L-1 | `state.py` | `import feather` just for `__version__` — circular import risk |
| L-5 | `state.py` | `_schema_snapshots` PRIMARY KEY prevents schema history |
| L-6 | `destinations/__init__.py` | `Destination` Protocol declares 3 unimplemented methods |
| L-7 | `pyproject.toml` | `pytz` declared but never imported |
| NIT-3 | `state.py` | 14-positional `INSERT INTO _runs VALUES (?,?,?,?...)` — fragile |
| NIT-6 | `tests/` | `client_db` fixture duplicated in `conftest.py` and `test_sources.py` |
| NIT-7 | `tests/` | `CURRENT_SCHEMA_VERSION = 1` duplicates `state.SCHEMA_VERSION` |

### Hands-on review findings (bugs found by running)

| ID | Severity | Description |
|---|---|---|
| BUG-1 | HIGH | Error message printed twice: once to stderr (logger) once to stdout (cli summary), out of order |
| BUG-2 | HIGH | `csv`/`sqlite`/other source types pass `feather validate` but crash `feather run` with unhandled `ValueError` |
| BUG-3 | HIGH | Missing `feather.yaml` → raw `FileNotFoundError` traceback instead of friendly message |
| BUG-4 | MEDIUM | Missing required YAML fields → raw `KeyError` traceback |
| BUG-5 | MEDIUM | `feather run` exits 0 on partial or total failure |
| BUG-6 | MEDIUM | Non-existent destination parent directory → raw `IOException` traceback; state DB created anyway |
| BUG-7 | MEDIUM | `target_table` without schema prefix or with hyphens passes validate, crashes at runtime |
| BUG-8 | MEDIUM | `strategy: incremental` passes validate, silently performs a full load |

### UX issues found by running

| ID | Description |
|---|---|
| UX-1 | README documents `--table`, `--tier`, `--json`, `feather history`, `feather schedule`, interactive `feather init` — none exist |
| UX-2 | `feather init .` produces `name = ""` in pyproject.toml |
| UX-3 | `feather init` blocked by pre-existing `.git` directory |
| UX-4 | Unset env var gives confusing path error (`/path/${UNSET_VAR}`) |
| UX-5 | `feather status` shows failure but no error message — must query state DB manually |
| UX-6 | `feather run` error output: interleaved stderr + stdout, error repeated, out of order |
| UX-7 | `feather run` without prior `feather setup` works silently — `setup` appears optional but isn't documented as such |
| UX-8 | `strategy: incremental` passes validate but silently full-loads — no warning anywhere |
| UX-9 | `feather validate` output doesn't show state DB path |

---

## Instructions for the next agent

### Running the full test suite
```bash
source .venv/bin/activate
pytest tests/                        # 92 tests, should all pass
bash scripts/hands_on_test.sh        # 51 CLI-level checks, should all pass
```

### When fixing a bug

1. Find the corresponding `TestKnownBugs` test in `tests/test_integration.py`.
2. Read the docstring — it states the current wrong behaviour and the correct post-fix behaviour.
3. Invert the assertion and remove the `BUG-N` prefix from the test name.
4. Move it to the appropriate positive-behaviour test class.
5. Find the corresponding `BUG-N` check in `scripts/hands_on_test.sh`.
6. Invert its expected condition and update the label.
7. Run both test suites to confirm green.

### Fixtures quick reference

| Fixture | Path | Use when |
|---|---|---|
| Icube real ERP | `tests/fixtures/client.duckdb` | Testing with real-world messy ERP data, 6 tables, ~14K rows |
| Icube incremental | `tests/fixtures/client_update.duckdb` | Future incremental/append strategy tests |
| Synthetic ERP | `tests/fixtures/sample_erp.duckdb` | Fast tests, NULL handling, simple schema, no ERP quirks |

### Priority order for fixes (suggested)

1. **BUG-3 / M-12** — `FileNotFoundError` and `KeyError` surface as raw tracebacks.
   Fix first: small change in `_load_and_validate` and `load_config`, high user-visibility.
2. **BUG-2 / C-2 / BUG-7** — Validate/run gap and SQL injection via identifiers.
   `_validate()` should check source type against `SOURCE_REGISTRY`, require
   schema-qualified `target_table`, and reject non-identifier characters in table names.
3. **BUG-5 / M-8** — Non-zero exit on failure. One-liner in `cli.py`.
4. **BUG-1 / UX-6** — Remove `logger.error()` from `run_table()` to eliminate duplicate output.
5. **C-3 / H-2 / H-4** — Connection leaks. Systematic: add `try/finally` or context managers
   to every DB-touching method in `state.py`, `destinations/duckdb.py`, `sources/duckdb_file.py`.
6. **C-4 / M-6** — Move `state.init_state()` and `dest.setup_schemas()` out of the per-table
   loop into `run_all()`. Add strategy dispatch guard (`NotImplementedError` for non-`full`).
7. **UX-3** — `feather init` `.git`-directory guard.
