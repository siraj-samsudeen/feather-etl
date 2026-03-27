# Slice 7: SQL Server Source

Created: 2026-03-27
Status: PENDING
Approved: No
Iterations: 0
Worktree: Yes
Type: Feature

## Summary

**Goal:** `feather run` connects to SQL Server via pyodbc, extracts tables as PyArrow, and loads into local DuckDB — replacing dlt for the extraction layer.

**Architecture:** New `DatabaseSource` base class (not `FileSource`) with `SqlServerSource` implementation. Uses pyodbc cursor → chunked fetchmany → PyArrow batches. Change detection via `CHECKSUM_AGG(BINARY_CHECKSUM(*)) + COUNT(*)` for full-strategy tables. Incremental tables reuse the existing watermark system (timestamp column). All tests mock `pyodbc.connect()`.

**Real-world target:** iCube ERP — 10 tables (3 full refresh, 7 incremental with various timestamp columns).

## Scope

### In Scope

- `DatabaseSource` base class with shared DB extraction logic
- `SqlServerSource` implementing `Source` protocol (check, discover, extract, detect_changes, get_schema)
- pyodbc connection with `SET NOCOUNT ON`, configurable `batch_size`
- `cursor.fetchmany(batch_size)` → PyArrow Table conversion
- Change detection: `CHECKSUM_AGG(BINARY_CHECKSUM(*))` + `COUNT(*)` for full-strategy tables
- Incremental extraction: `WHERE {timestamp_column} >= {effective_watermark}` (reuses existing pipeline logic)
- `discover()` via `INFORMATION_SCHEMA.TABLES` + `INFORMATION_SCHEMA.COLUMNS`
- `get_schema()` via `INFORMATION_SCHEMA.COLUMNS`
- Registry: add `"sqlserver"` → `SqlServerSource`
- Config validation: `sqlserver` type requires `connection_string`, not `path`
- `create_source()` factory: pass `connection_string` for DB sources
- Watermark persistence: reuse existing `last_checksum` + `last_row_count` columns in `_watermarks`
- Unit tests: all pyodbc calls mocked (connect, cursor, fetchmany, execute)
- Integration-style test: end-to-end with mocked pyodbc returning realistic PyArrow data
- Real smoke test against user's SQL Server (manual, not in CI)

### Out of Scope

- ROWVERSION-based change detection (open question — defer)
- SQL Server → MotherDuck direct sync (V13)
- Multi-database support (single connection_string per config)
- Connection pooling / retry logic (V14)
- Schema drift detection (V10)

## Tasks

Done: 0 / 7  Left: 7

### Task 1: DatabaseSource base class
- [ ] Create `src/feather/sources/database_source.py`
- New base class `DatabaseSource` (parallel to `FileSource` for DB sources)
- `__init__(connection_string: str)` — stores connection string
- `_build_where_clause()` — reuse same logic as FileSource (extract to shared mixin or duplicate — 6 lines)
- Abstract methods that subclasses implement: `_connect()`, `_extract_query()`, `_checksum_query()`, `_discover_query()`, `_schema_query()`
- Test: unit test that DatabaseSource can't be instantiated directly

### Task 2: SqlServerSource implementation
- [ ] Create `src/feather/sources/sqlserver.py`
- `check()`: attempt pyodbc.connect(), return True/False
- `discover()`: query `INFORMATION_SCHEMA.TABLES` (TABLE_TYPE = 'BASE TABLE') + `INFORMATION_SCHEMA.COLUMNS`
- `get_schema()`: query `INFORMATION_SCHEMA.COLUMNS` for specific table
- `extract()`: `SELECT [columns] FROM {table} [WHERE ...]`, `cursor.fetchmany(batch_size)` → build PyArrow RecordBatches → combine to Table
- `detect_changes()` for full-strategy:
  - `SELECT CHECKSUM_AGG(BINARY_CHECKSUM(*)) AS checksum, COUNT(*) AS row_count FROM {table}`
  - Compare against `last_checksum` + `last_row_count` from watermark
  - Return `ChangeResult(changed=False)` if both match
- For incremental tables: return `ChangeResult(changed=True)` always (watermark filtering handles the skip)
- `SET NOCOUNT ON` after connection
- Test: mocked pyodbc, verify each method's SQL and return values

### Task 3: Registry + config changes
- [ ] Add `"sqlserver": SqlServerSource` to `SOURCE_REGISTRY`
- [ ] Update `create_source()` to pass `connection_string` for non-file sources
- [ ] Update `config.py` validation:
  - `sqlserver` requires `connection_string`, not `path`
  - Add `"sqlserver"` to a new `DB_SOURCE_TYPES` set
  - Skip file-existence check for DB sources
- Test: config validation rejects sqlserver without connection_string

### Task 4: Pipeline integration for DB change detection
- [ ] `pipeline.py` `run_table()`: when source is DB + full strategy, use `last_checksum` + `last_row_count` from watermark for change detection
- [ ] `write_watermark()`: pass `last_checksum` and `last_row_count` after full-strategy DB extraction
- [ ] `state.py` `write_watermark()`: add `last_checksum` and `last_row_count` params (columns already exist in schema)
- Note: incremental DB tables don't need special pipeline changes — existing watermark logic works
- Test: mock pipeline with DB source, verify checksum-based skip

### Task 5: Chunked fetch → PyArrow conversion
- [ ] Implement `_fetch_to_arrow(cursor, batch_size)` helper
- `cursor.description` → PyArrow schema (SQL Server type → Arrow type mapping)
- `cursor.fetchmany(batch_size)` in loop → `pa.RecordBatch.from_pydict()`
- Combine batches: `pa.concat_tables()`
- Handle empty result (0 rows) → return empty table with correct schema
- Type mapping: INT/BIGINT→int64, VARCHAR/NVARCHAR→string, DATETIME/DATETIME2→timestamp, DECIMAL→decimal128, BIT→bool, FLOAT/REAL→float64
- Test: verify type mapping for each SQL Server type

### Task 6: Mocked integration tests
- [ ] Create `tests/test_sqlserver.py`
- Test full pipeline: config → create_source → extract → load into real DuckDB → verify rows
- All pyodbc calls mocked (connect returns mock connection, cursor returns mock cursor)
- Mock cursor.description returns realistic column metadata
- Mock cursor.fetchmany returns realistic row data (matching iCube ERP shapes)
- Tests:
  - `test_sqlserver_full_extract_loads_rows` — full strategy, verify rows in DuckDB
  - `test_sqlserver_incremental_extract_with_watermark` — verify WHERE clause includes watermark
  - `test_sqlserver_change_detection_skip` — checksum unchanged → skip
  - `test_sqlserver_change_detection_changed` — checksum changed → extract
  - `test_sqlserver_discover_returns_schemas` — verify INFORMATION_SCHEMA query
  - `test_sqlserver_connection_failure` — pyodbc.Error → check() returns False
  - `test_sqlserver_empty_table` — 0 rows → empty PyArrow table with schema

### Task 7: Real smoke test
- [ ] Manual test with user's SQL Server (not automated)
- Create a test `feather.yaml` pointing at iCube ERP
- `feather discover` → list tables
- `feather run` → extract 1-2 small tables (EmployeeForm, InventoryGroup)
- Verify rows in local DuckDB
- This validates the real pyodbc + SQL Server path

## File Changes

| File | Change |
|------|--------|
| `src/feather/sources/database_source.py` | **NEW** — DatabaseSource base class |
| `src/feather/sources/sqlserver.py` | **NEW** — SqlServerSource implementation |
| `src/feather/sources/registry.py` | Add sqlserver to registry, update create_source |
| `src/feather/config.py` | Add DB_SOURCE_TYPES, validation for connection_string |
| `src/feather/pipeline.py` | Pass checksum/row_count to write_watermark for DB sources |
| `src/feather/state.py` | Add last_checksum/last_row_count params to write_watermark |
| `tests/test_sqlserver.py` | **NEW** — mocked tests |

## Risk & Decisions

- **pyodbc type mapping:** SQL Server types are complex (DATETIME vs DATETIME2, DECIMAL precision). Start with the types used by iCube ERP and extend as needed.
- **CHECKSUM_AGG limitations:** CHECKSUM_AGG can have hash collisions. Acceptable for change detection (false positive = unnecessary re-extract, not data loss).
- **Connection string format:** Use standard ODBC format: `DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=...;UID=...;PWD=...;TrustServerCertificate=yes`. Driver 18 requires `TrustServerCertificate=yes` for self-signed certs. Support `${ENV_VAR}` substitution in YAML.
- **Real-world row counts (iCube1920):** EmployeeForm 61, InventoryGroup 70, CUSTOMERMASTER 1,363, INVITEM 90K, SALESINVOICE 440K, SALESINVOICEMASTER 8K, POSMaster 1.4K, POSDetail 2K, POS_Return_Master 12, POS_Return_Detail 16. Chunked fetch matters for SALESINVOICE/INVITEM.
