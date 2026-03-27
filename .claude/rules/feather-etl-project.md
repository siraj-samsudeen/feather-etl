# feather-etl

Config-driven Python ETL for extracting from heterogeneous ERP sources into local DuckDB.
See `docs/prd.md` for requirements, `README.md` for architecture, `docs/CONTRIBUTING.md` for work conventions.

## Tech Stack

- **Language:** Python >=3.10, managed with `uv`
- **Core:** DuckDB (local processing + file readers), PyArrow (interchange), PyYAML (config), Typer (CLI)
- **Connectors:** pyodbc (SQL Server), openpyxl (Excel .xls fallback)
- **Runtime:** APScheduler (scheduling), smtplib (alerting)
- **Testing:** pytest, pytest-cov | **Quality:** ruff

## Directory Structure

```
src/feather/
  cli.py              Typer CLI: validate, discover, run, setup, status, init
  config.py           YAML config loader + validation
  pipeline.py         run_all() orchestration + change detection integration
  state.py            StateManager: _runs + _watermarks tables in DuckDB
  init_wizard.py      `feather init` client scaffolding
  sources/            Source protocol + implementations (duckdb_file, csv, sqlite)
    file_source.py    FileSource base: detect_changes() with mtime + MD5 hash
    registry.py       Source type registry (type string -> class)
  destinations/       Destination protocol (duckdb only for now)
tests/                pytest (real DuckDB fixtures, no mocking)
scripts/              hands_on_test.sh (CLI integration), fixture generators
docs/                 PRD, plans/, reviews/, CONTRIBUTING.md
```

## Test Fixtures

| File | Schema | Rows | Use for |
|---|---|---|---|
| `tests/fixtures/client.duckdb` | `icube.*`, 6 tables | ~14K | Real-world ERP, messy schemas |
| `tests/fixtures/client_update.duckdb` | `icube.*`, 3 tables | ~12K | Future incremental tests |
| `tests/fixtures/sample_erp.duckdb` | `erp.*`, 3 tables | 12 | Fast tests, NULL handling |
| `tests/fixtures/sample_erp.sqlite` | `erp.*`, 3 tables | 12 | SQLite source tests |
| `tests/fixtures/csv_data/` | 3 CSV files (orders, customers, products) | ~10 each | CSV source tests |

Regenerate: `python scripts/create_sample_erp_fixture.py` (DuckDB) and `python scripts/create_csv_sqlite_fixtures.py` (CSV + SQLite)

## Dev Commands

| Task | Command |
|------|---------|
| Install | `uv sync` |
| Test | `uv run pytest -q` |
| CLI integration | `bash scripts/hands_on_test.sh` |
| Coverage | `uv run pytest -q --cov=src --cov-fail-under=80` |
| Lint | `ruff check .` |
| Format | `ruff format .` |

## Source Protocol

New file sources: extend `FileSource` (~30 lines), implement `_reader_sql()` and `_source_path_for_table()`. DB sources: implement full `Source` protocol (~80 lines). Register in `sources/registry.py`.

Change detection: `FileSource.detect_changes()` checks mtime first, falls back to MD5 hash. Returns `ChangeResult` with reason and metadata. Pipeline skips unchanged tables, updates watermarks on success.

## Entry Point

`feather = "feather.cli:app"` (Typer). All commands accept `--config PATH` (default: `feather.yaml`).
