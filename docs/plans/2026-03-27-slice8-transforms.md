# Slice 8: Silver/Gold Transform Engine

Created: 2026-03-27
Status: COMPLETE
Approved: No
Iterations: 0
Worktree: Yes
Type: Feature

## Summary

**Goal:** `feather setup` reads plain `.sql` files from `transforms/silver/` and `transforms/gold/`, resolves dependencies via `-- depends_on` comments, and executes them in topological order as DuckDB views (silver) or materialized tables (gold, when `-- materialized: true`). After each `feather run`, materialized gold tables are rebuilt.

**Architecture:** New `src/feather/transforms.py` module. Parses SQL file headers for metadata comments (`-- depends_on`, `-- materialized`). Uses `graphlib.TopologicalSorter` (stdlib) for execution ordering. Silver = always `CREATE OR REPLACE VIEW`. Gold = `CREATE OR REPLACE VIEW` by default, `CREATE OR REPLACE TABLE ... AS` when materialized. Template variable substitution via `string.Template`. Integrates into `cli.py setup` and `pipeline.py run_all`.

**Real-world target:** Replicate the 10 silver + 2 gold sqlmesh transforms from afans-reporting-dev as plain DuckDB SQL files.

## Scope

### In Scope

- New module `src/feather/transforms.py`
- Discover `.sql` files in `transforms/silver/` and `transforms/gold/` relative to config dir
- Parse SQL file header comments:
  - `-- depends_on: silver.sales_invoice` (one per line, multiple allowed)
  - `-- materialized: true` (gold only)
- Build dependency graph, topological sort via `graphlib.TopologicalSorter`
- Execute silver transforms as `CREATE OR REPLACE VIEW silver.{name} AS ...`
- Execute gold transforms as `CREATE OR REPLACE VIEW gold.{name} AS ...` (default)
- Execute materialized gold as `CREATE OR REPLACE TABLE gold.{name} AS SELECT * FROM (...)`
- `string.Template` substitution for `${variable}` in SQL (variables from config `defaults` section)
- Error on missing dependency (declared `-- depends_on` doesn't resolve to known transform)
- `feather setup`: execute all transforms after schema creation
- `feather run`: rebuild materialized gold tables after extraction completes
- Unit tests with real DuckDB (create test SQL files, verify views/tables created)
- E2E test: extract data into bronze → silver views read from bronze → gold views join silver

### Out of Scope

- Transform change detection (re-running only changed SQL files)
- Transform validation / linting
- DQ checks on transform output (V9)
- MotherDuck sync of gold tables (V13)
- Template variables from config (beyond defaults — could add a `variables` section later)
- Jinja-style templating (only `string.Template` — `${var}` syntax)

## Tasks

Done: 6 / 6  Left: 0

### Task 1: SQL file parser
- [x] `parse_transform_file(path: Path) → TransformMeta`
- `TransformMeta` dataclass: `name` (str), `schema` (silver|gold), `sql` (str), `depends_on` (list[str]), `materialized` (bool)
- Parse `-- depends_on: X` lines from file header
- Parse `-- materialized: true` from file header
- Derive `name` from filename: `sales_invoice.sql` → `sales_invoice`
- Derive `schema` from parent dir: `transforms/silver/` → `silver`
- Read full SQL content (everything after header comments is the SQL body)
- Test: parse a sample SQL file, verify all fields

### Task 2: Discovery + dependency graph
- [x] `discover_transforms(config_dir: Path) → list[TransformMeta]`
- Scan `transforms/silver/*.sql` and `transforms/gold/*.sql`
- Parse each file via Task 1's parser
- [x] `build_execution_order(transforms: list[TransformMeta]) → list[TransformMeta]`
- Build graph: each transform's qualified name (`silver.X`, `gold.X`) as node
- Add edges from `depends_on` declarations
- `graphlib.TopologicalSorter.static_order()` → execution list
- Error if dependency not found in discovered transforms
- Error on circular dependencies (TopologicalSorter raises `CycleError`)
- Test: verify ordering with silver→gold chain, verify error on missing dep

### Task 3: Transform execution engine
- [x] `execute_transforms(con: duckdb.DuckDBPyConnection, transforms: list[TransformMeta], variables: dict[str, str] | None = None) → list[TransformResult]`
- `TransformResult` dataclass: `name` (str), `schema` (str), `type` (view|table), `status` (success|error), `error` (str|None)
- For each transform in topological order:
  - Apply `string.Template` substitution to SQL body
  - Silver: `CREATE OR REPLACE VIEW {schema}.{name} AS {sql}`
  - Gold (default): `CREATE OR REPLACE VIEW {schema}.{name} AS {sql}`
  - Gold (materialized): `CREATE OR REPLACE TABLE {schema}.{name} AS {sql}`
- [x] `rebuild_materialized_gold(con, transforms, variables) → list[TransformResult]`
  - Filters to only `materialized=True` gold transforms
  - Re-executes them as `CREATE OR REPLACE TABLE`
  - Called by pipeline after extraction run completes
- Test: create bronze table, execute silver view over it, verify view returns data

### Task 4: CLI integration — `feather setup`
- [x] After `dest.setup_schemas()` in `cli.py setup`, discover and execute all transforms
- Show output: `Transforms applied: N silver views, M gold views, K gold tables`
- Handle missing transforms dir gracefully (no error if `transforms/` doesn't exist)
- Test: `feather setup` with transform files creates views in DuckDB

### Task 5: Pipeline integration — `feather run`
- [x] After all tables extracted in `run_all()`, rebuild materialized gold tables
- Only rebuild if at least one table succeeded (skip if all skipped/failed)
- Log: `Rebuilt N materialized gold table(s)`
- Handle no transforms gracefully (skip silently)
- Test: extract → verify gold table rebuilt with fresh data

### Task 6: End-to-end test with realistic transforms
- [x] Create test SQL files matching afans-reporting-dev patterns:
  - `transforms/silver/employee_master.sql` — simple column rename from bronze
  - `transforms/silver/item_master.sql` — join with dimension table
  - `transforms/gold/sales_invoice.sql` — multi-table join, `-- materialized: true`
- [x] Test: load sample data into bronze → `feather setup` → verify silver views → verify gold materialized table
- [x] Test: modify bronze data → `feather run` → verify gold table rebuilt with new data
- [x] Test: `-- depends_on` ordering — gold depends on silver, execution order correct

## File Changes

| File | Change |
|------|--------|
| `src/feather/transforms.py` | **NEW** — parser, discovery, execution engine |
| `src/feather/cli.py` | `setup`: add transform execution after schema creation |
| `src/feather/pipeline.py` | `run_all`: add materialized gold rebuild after extraction |
| `tests/test_transforms.py` | **NEW** — unit + integration tests |
| `tests/fixtures/transforms/silver/*.sql` | **NEW** — test SQL files |
| `tests/fixtures/transforms/gold/*.sql` | **NEW** — test SQL files |

## Design Decisions

- **Silver = always VIEW.** No config needed. Views are lazy, always reflect current data, zero pipeline overhead. This matches the PRD's "silver is the canonical mapping layer" decision.
- **Gold = VIEW by default, TABLE when `-- materialized: true`.** The comment-in-SQL approach avoids yet another config file. The SQL file IS the config.
- **`string.Template` not Jinja.** Simpler, stdlib, sufficient for `${schema}` substitutions. Jinja would add a dependency for no real gain in v1.
- **Header comments for metadata.** `-- depends_on` and `-- materialized` are SQL comments — the SQL remains valid DuckDB SQL that you can paste directly into a DuckDB CLI. No magic syntax.
- **Body = everything in the file.** The SQL file contains the SELECT body. The engine wraps it in `CREATE OR REPLACE VIEW/TABLE ... AS`. This keeps SQL files clean and portable — they contain only the SELECT logic.

Wait — actually, let me reconsider. If the SQL file only contains the SELECT body, you can't paste it into DuckDB directly as a `CREATE VIEW` statement. But the PRD says:

> "THE SYSTEM SHALL store transforms as plain .sql files"

And the example in the PRD (line 1652) shows:

```sql
CREATE OR REPLACE VIEW silver.sales_invoice AS
SELECT ...
```

So the SQL file contains the **full DDL statement**. The engine just executes it directly, optionally wrapping materialized gold transforms.

**Revised approach:**
- Silver SQL files contain `CREATE OR REPLACE VIEW silver.{name} AS SELECT ...`
- Gold SQL files (non-materialized) contain `CREATE OR REPLACE VIEW gold.{name} AS SELECT ...`
- Gold SQL files (materialized) contain the SELECT body only — engine wraps in `CREATE OR REPLACE TABLE gold.{name} AS`

Actually no, let me simplify further:
- **All SQL files contain the full statement.** `CREATE OR REPLACE VIEW ...` for views, whatever the author writes.
- **For materialized gold:** Engine reads the file, finds `-- materialized: true`, and replaces `CREATE OR REPLACE VIEW` with `CREATE OR REPLACE TABLE` (or wraps the SELECT). During `feather run` rebuild, it drops and recreates the table.

**Final approach: SQL files contain the SELECT body. Engine wraps.**
This is cleaner because:
1. The file just says what the data looks like (SELECT)
2. The engine decides view vs table based on metadata
3. Template substitution is cleaner on the SELECT alone
4. Rebuild is simple: re-execute the same wrap

## Example Transform Files

### `transforms/silver/employee_master.sql`
```sql
-- depends_on: bronze.employee_form
SELECT
    id AS employee_id,
    "EMPLOYEEID" AS employee_code,
    first_name AS employee_name
FROM bronze.employee_form
```

### `transforms/gold/sales_invoice.sql`
```sql
-- depends_on: silver.sales_invoice_detail
-- depends_on: silver.sales_invoice_master
-- depends_on: silver.customer_master
-- depends_on: silver.item_master
-- materialized: true
SELECT
    d.*,
    m.invoice_type,
    m.invoice_date,
    c.customer_name,
    c.state,
    i.item_description,
    i.brand
FROM silver.sales_invoice_detail d
LEFT JOIN silver.sales_invoice_master m USING (invoice_no)
LEFT JOIN silver.customer_master c USING (customer_code)
LEFT JOIN silver.item_master i USING (item_code)
```
