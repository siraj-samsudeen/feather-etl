# Slice 1 Review — Round 2

Created: 2026-03-26
Slice: 1 (foundation)
Prior review: [docs/reviews/2026-03-26-slice1-review.md](2026-03-26-slice1-review.md)
Fixes plan: [docs/plans/2026-03-26-slice1-fixes.md](../plans/2026-03-26-slice1-fixes.md)
Commit reviewed: post-fix HEAD

---

## Review method

Two passes against the post-fix state:

1. **Diff review** — read every changed file against the original findings, checked
   each BUG-N and UX-N claim against the actual code.
2. **Test execution** — ran both test suites from a clean shell and recorded
   every result.

---

## Test results

```
pytest tests/ --cov=feather --cov-report=term-missing
  106 passed   0 failed   4.26 s

  Name                                   Stmts   Miss  Cover
  ----------------------------------------------------------
  src/feather/__init__.py                    1      0   100%
  src/feather/cli.py                       100      0   100%
  src/feather/config.py                    147      0   100%
  src/feather/destinations/__init__.py       4      0   100%
  src/feather/destinations/duckdb.py        42      0   100%
  src/feather/init_wizard.py                20      0   100%
  src/feather/pipeline.py                   45      0   100%
  src/feather/sources/__init__.py           16      0   100%
  src/feather/sources/duckdb_file.py        48      0   100%
  src/feather/sources/registry.py           11      0   100%
  src/feather/state.py                      63      0   100%
  ----------------------------------------------------------
  TOTAL                                    497      0   100%

bash scripts/hands_on_test.sh
  51 passed   0 failed   5.54 s
```

Both suites are fully green. Coverage is 100% (497 statements, 0 missed).

---

## Verdict by finding

### BUG findings

| ID | Claimed fix | Verified? | Notes |
|---|---|---|---|
| BUG-1 | Remove `logger.error()` from `run_table()` — error appears on stdout only, not duplicated on stderr | ✅ CORRECT | `pipeline.py` has no `logging` import and no `logger.error()` call. S14 shell check confirms `stderr_lines == 0`. |
| BUG-2 | Validate source type against `SOURCE_REGISTRY` | ✅ CORRECT | `_validate()` imports `SOURCE_REGISTRY` lazily and rejects any type not in the registry. `csv`, `sqlite`, etc. raise `ValueError: Unsupported source type`. S3/S15 confirm. |
| BUG-3 | `_load_and_validate` catches `FileNotFoundError`, prints `"Config file not found: …"`, exits 1 | ✅ CORRECT | `cli.py` catches both `ValueError` and `FileNotFoundError`. S13 and `test_missing_config_file_shows_friendly_error` confirm. |
| BUG-4 | `_parse_tables` wraps `KeyError` in `ValueError("Table entry N missing required field: …")` | ✅ CORRECT | `config.py` has `except KeyError as e: raise ValueError(f"Table entry {i + 1} missing required field: {e}")`. |
| BUG-5 | `feather run` exits 1 when any table fails | ✅ CORRECT | `cli.py` raises `typer.Exit(code=1)` when `successes < len(results)`. S6 and `test_run_with_bad_table_shows_failure` confirm exit code 1. |
| BUG-6 | Validate that destination parent directory exists | ✅ CORRECT | `_validate()` checks `config.destination.path.parent.exists()` and raises a friendly `ValueError`. `test_destination_parent_must_exist` confirms. |
| BUG-7 | `target_table` without schema prefix or with hyphens rejected at validate | ✅ CORRECT | `_validate()` checks for `.` separator (schema required) and runs `_SQL_IDENTIFIER_RE.match()` on the table part. S3/S16 and graduated `TestValidationGuards` tests confirm. |
| BUG-8 | `status` showing all-time history documented as intentional behaviour | ✅ CORRECT (by design) | Declared a design decision, not a bug. `get_status()` docstring updated. `test_status_shows_all_time_history` asserts it as correct behaviour. |

### UX findings

| ID | Claimed fix | Verified? | Notes |
|---|---|---|---|
| UX-1 | README CLI section rewritten to show only implemented commands | ✅ CORRECT | README CLI section now shows only `init`, `validate`, `discover`, `setup`, `run`, `status`. No `--table`, `--tier`, `--json`, `feather history`, `feather schedule`. `setup` is documented as optional. |
| UX-2 | `feather init .` uses CWD name, not empty string | ✅ CORRECT | `init_wizard.py` receives the resolved `project_path`; `cli.py` resolves `.` via `Path(project_name).resolve()` before passing to scaffold. `test_init_dot_uses_cwd_name` and S1 confirm `name = ""` is absent. |
| UX-3 | `feather init` no longer blocked by `.git` directory | ✅ CORRECT | `cli.py` checks `non_hidden = [f for f in project_path.iterdir() if not f.name.startswith(".")]` — hidden dirs are excluded. `test_init_allows_git_only_dir` confirms. |
| UX-4 | Unresolved `${VAR}` gives a clear error naming the variable | ✅ CORRECT | `_check_unresolved_env_vars()` walks the post-expansion YAML tree and raises `ValueError("Unresolved environment variable ${VAR} in 'path'. …")`. `test_unresolved_env_var_gives_clear_error` confirms. |
| UX-5 | `feather status` shows error text for failed tables | ✅ CORRECT | `get_status()` now includes `r.error_message` in the SELECT. `cli.py` prints `"  Error: …"` when `status == "failure"` and `error_message` is non-empty. `test_status_shows_error_message_for_failures` confirms. |
| UX-6 | Interleaved stderr + stdout, error repeated, out of order | ✅ CORRECT | Covered by BUG-1 fix (no `logger.error()`). All error output goes through the CLI summary path. |
| UX-7 | `feather setup` help text updated to say it's optional | ✅ CORRECT | `setup` command docstring now reads: `"Preview and initialize state DB and schemas. Optional — feather run creates them automatically."` `test_run_without_setup_auto_creates_state_and_data` confirms run-without-setup works. |
| UX-8 | `strategy: incremental` without `timestamp_column` rejected at validate | ✅ CORRECT | `_validate()` checks `table.strategy == "incremental" and not table.timestamp_column`. `test_incremental_requires_timestamp_column` and `test_incremental_with_timestamp_column_passes` confirm. |
| UX-9 | `feather validate` output shows state DB path | ✅ CORRECT | `cli.py` prints `f"  State: {cfg.config_dir / 'feather_state.duckdb'}"` in the validate command. `test_validate_shows_state_path` confirms. |

### Static review findings (deferred)

All C/H/M/L/NIT findings remain open per the fixes plan rationale. Nothing was accidentally fixed or accidentally broken. The one static finding that partially overlaps with a BUG finding is:

- **C-2** (IndexError when `target_table` has no schema prefix) — the BUG-7 fix prevents bare table names from ever reaching `destinations/duckdb.py`, so the `IndexError` can no longer be triggered at runtime. The underlying `parts[1]` code is still un-guarded, but the validation fence makes it unreachable. This is the right approach for Slice 1; proper quoting (C-1) is Slice 2 scope.

### Remaining known bug

**M-6 / `TestKnownBugs::test_M6_incremental_strategy_silently_does_full_load`** — strategy dispatch is not implemented. `strategy: incremental` with a valid `timestamp_column` passes validate and runs successfully, but performs a full load. The test is correctly named and documented. It is the only remaining `TestKnownBugs` entry. Correctly deferred to Slice 2.

---

## New issues found in this review

### N-1 — `_schema_snapshots` PRIMARY KEY still blocks schema history (L-5, unchanged)

Not introduced by the fixes, but confirmed still present. `state.py` line 87:
```python
PRIMARY KEY (table_name, column_name)
```
This means schema snapshots are silently overwritten rather than accumulated. Since schema drift detection is a Slice 2 feature, this is still correctly deferred — but should be noted in the Slice 2 plan as a prerequisite.

### N-2 — `write_validation_json` called twice on the `run` success path (M-7, confirmed)

`run_all()` calls `write_validation_json(config_path, config)` at the top of the function. `_load_and_validate()` in `cli.py` also calls `write_validation_json()` before `run_all()` is invoked. So on a successful `feather run`:
1. `_load_and_validate` writes `feather_validation.json` (valid=True)
2. `run_all` overwrites it with another `feather_validation.json` (valid=True, same content)

The file is overwritten with identical content — no user-visible harm. Still deferred per the fixes plan rationale. Worth eliminating in Slice 2 to avoid confusion.

### N-3 — `_SQL_IDENTIFIER_RE` doesn't validate the schema part of `target_table`

`_validate()` in `config.py` validates only the *table* part of `target_table` against
`_SQL_IDENTIFIER_RE`:
```python
schema_prefix, table_part = table.target_table.split(".", 1)
if schema_prefix not in VALID_SCHEMA_PREFIXES:   # membership check only
    ...
if not _SQL_IDENTIFIER_RE.match(table_part):     # regex only on table part
    ...
```
The schema prefix is validated via set membership (`VALID_SCHEMA_PREFIXES`) which is
sufficient since `bronze`, `silver`, `gold` are all valid SQL identifiers. However
a multi-dot string like `bronze.my.table` is not rejected — `split(".", 1)` splits on
the *first* dot, making `table_part = "my.table"`, which fails the regex (`"."` is not
matched by `[a-zA-Z0-9_]`). So the error message says "invalid characters" rather than
"too many dots". This is acceptable behaviour for Slice 1 but should be tightened in
Slice 2 to produce a clearer message.

### N-4 — Connection leak in `read_watermark` on early-return path

`state.py::read_watermark()`:
```python
def read_watermark(self, table_name: str) -> dict[str, object] | None:
    con = self._connect()
    row = con.execute(...).fetchone()
    if row is None:
        con.close()          # ← closed here
        return None
    columns = [desc[0] for desc in con.description]
    con.close()              # ← closed here
    return dict(zip(columns, row))
```
If `con.execute()` raises an exception (e.g., table doesn't exist because `init_state`
hasn't been called), `con` is never closed. This is a pre-existing C-3/H-4 leak
pattern deferred to Slice 2. It is not new, but it is present in the post-fix code
and worth noting for the Slice 2 connection-leak cleanup pass.

---

## Summary

**All 8 BUG-N findings and all 9 UX-N findings from the original review are correctly fixed.**
The fixes are targeted, well-tested, and do not introduce regressions. The one remaining
known bug (M-6, strategy dispatch) is correctly deferred and clearly documented.

The 4 new findings (N-1 through N-4) are all pre-existing or minor issues not
introduced by the fixes. None require immediate action — they are appropriate scope
for the Slice 2 architectural cleanup pass.

**Slice 1 is production-ready for its stated scope (full-strategy DuckDB-to-DuckDB
pipelines with operator-friendly CLI).**

### Recommended Slice 2 priorities (from this review)

In addition to the deferred static findings already listed in the fixes plan:

1. N-3 (multi-dot `target_table` gives confusing error) — 2-line fix in `_validate()`
2. N-1 (schema snapshot PRIMARY KEY) — prerequisite for schema drift detection
3. N-2 (double `write_validation_json`) — remove the call from `run_all()`
4. N-4 (connection leaks) — systematic `try/finally` or context managers across all
   `StateManager` methods (already in deferred list as C-3/H-4)
