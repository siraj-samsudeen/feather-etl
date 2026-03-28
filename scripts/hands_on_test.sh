#!/usr/bin/env bash
# =============================================================================
# hands_on_test.sh — Reproduce the full hands-on review workflow
#
# This script recreates every scenario tested in the slice-1 hands-on review.
# Run it from the repo root:
#
#   bash scripts/hands_on_test.sh
#
# It prints PASS/FAIL for each scenario.  A non-zero exit means at least one
# scenario failed.  Output is verbose so individual failures are easy to trace.
# =============================================================================

set -euo pipefail

FEATHER="$(pwd)/.venv/bin/feather"
FIXTURE_DIR="$(pwd)/tests/fixtures"
WORK_DIR="$(mktemp -d)"
PASS=0
FAIL=0

green()  { printf '\033[32m%s\033[0m\n' "$*"; }
red()    { printf '\033[31m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }

check() {
    local label="$1"
    local result="$2"   # "ok" or "fail"
    if [[ "$result" == "ok" ]]; then
        green "  PASS: $label"
        (( PASS++ )) || true
    else
        red   "  FAIL: $label"
        (( FAIL++ )) || true
    fi
}

run_ok()   { "$@" > /dev/null 2>&1;  echo $?; }
run_fail() { "$@" > /dev/null 2>&1 && echo fail || echo ok; }

echo ""
yellow "=== feather-etl hands-on test suite ==="
yellow "Work dir: $WORK_DIR"
echo ""

# ---------------------------------------------------------------------------
# S1 — feather init creates expected scaffold
# ---------------------------------------------------------------------------
yellow "--- S1: feather init ---"
S1="$WORK_DIR/s1"

"$FEATHER" init "$S1" > /dev/null 2>&1
[[ -f "$S1/feather.yaml" ]]         && check "feather.yaml created"       ok || check "feather.yaml created"       fail
[[ -f "$S1/pyproject.toml" ]]       && check "pyproject.toml created"     ok || check "pyproject.toml created"     fail
[[ -f "$S1/.gitignore" ]]           && check ".gitignore created"         ok || check ".gitignore created"         fail
[[ -f "$S1/.env.example" ]]         && check ".env.example created"       ok || check ".env.example created"       fail
[[ -d "$S1/transforms/silver" ]]    && check "transforms/silver/ created" ok || check "transforms/silver/ created" fail
[[ -d "$S1/transforms/gold" ]]      && check "transforms/gold/ created"   ok || check "transforms/gold/ created"   fail
[[ -d "$S1/tables" ]]               && check "tables/ created"            ok || check "tables/ created"            fail
[[ -d "$S1/extracts" ]]             && check "extracts/ created"          ok || check "extracts/ created"          fail

# re-init non-empty dir must be rejected
code=$("$FEATHER" init "$S1" > /dev/null 2>&1; echo $?) || true
[[ "$code" != "0" ]] && check "init on non-empty dir exits non-zero" ok || check "init on non-empty dir exits non-zero" fail

# init with "." as name → should use directory name, not empty string
S1B="$WORK_DIR/s1b" && mkdir "$S1B"
(cd "$S1B" && "$FEATHER" init . > /dev/null 2>&1)
grep -q 'name = ""' "$S1B/pyproject.toml" \
    && check "init '.' uses directory name in pyproject.toml" fail \
    || check "init '.' uses directory name in pyproject.toml" ok

echo ""

# ---------------------------------------------------------------------------
# S2 — feather validate (client fixture)
# ---------------------------------------------------------------------------
yellow "--- S2: feather validate with Icube client fixture ---"
S2="$WORK_DIR/s2" && mkdir "$S2"
cp "$FIXTURE_DIR/client.duckdb" "$S2/source.duckdb"
cat > "$S2/feather.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: sales_invoice
    source_table: icube.SALESINVOICE
    target_table: bronze.sales_invoice
    strategy: full
  - name: customer_master
    source_table: icube.CUSTOMERMASTER
    target_table: bronze.customer_master
    strategy: full
  - name: inventory_group
    source_table: icube.InventoryGroup
    target_table: bronze.inventory_group
    strategy: full
YAML

out=$("$FEATHER" validate --config "$S2/feather.yaml" 2>&1)
echo "$out" | grep -q "Config valid: 3 table" \
    && check "validate succeeds with 3 tables" ok \
    || check "validate succeeds with 3 tables" fail

[[ -f "$S2/feather_validation.json" ]] \
    && check "feather_validation.json written" ok \
    || check "feather_validation.json written" fail

uv run python -c "
import json, sys
d = json.load(open('$S2/feather_validation.json'))
assert d['valid'] is True
assert d['tables_count'] == 3
assert 'source' in d['resolved_paths']
" 2>/dev/null \
    && check "feather_validation.json content correct" ok \
    || check "feather_validation.json content correct" fail

echo ""

# ---------------------------------------------------------------------------
# S3 — validate failure cases
# ---------------------------------------------------------------------------
yellow "--- S3: validate failure cases ---"
S3="$WORK_DIR/s3" && mkdir "$S3"
cp "$FIXTURE_DIR/client.duckdb" "$S3/source.duckdb"

# missing source file
cat > "$S3/missing_source.yaml" << 'YAML'
source:
  type: duckdb
  path: ./does_not_exist.duckdb
destination:
  path: ./feather_data.duckdb
tables: []
YAML
code=$("$FEATHER" validate --config "$S3/missing_source.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$code" != "0" ]] \
    && check "validate fails when source missing" ok \
    || check "validate fails when source missing" fail

# invalid strategy
cat > "$S3/bad_strategy.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: t
    source_table: icube.SALESINVOICE
    target_table: bronze.t
    strategy: bogus_strategy
YAML
code=$("$FEATHER" validate --config "$S3/bad_strategy.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$code" != "0" ]] \
    && check "validate rejects invalid strategy" ok \
    || check "validate rejects invalid strategy" fail

# unimplemented source type rejected by validate
cat > "$S3/excel_source.yaml" << 'YAML'
source:
  type: excel
  path: ./source.xlsx
destination:
  path: ./feather_data.duckdb
tables:
  - name: t
    source_table: main.t
    target_table: bronze.t
    strategy: full
YAML
code=$("$FEATHER" validate --config "$S3/excel_source.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$code" != "0" ]] \
    && check "validate rejects unimplemented source type (excel)" ok \
    || check "validate rejects unimplemented source type (excel)" fail

# target_table without schema prefix rejected by validate (BUG-7 fixed)
cat > "$S3/no_schema_target.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: t
    source_table: icube.SALESINVOICE
    target_table: no_schema_table
    strategy: full
YAML
code=$("$FEATHER" validate --config "$S3/no_schema_target.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$code" != "0" ]] \
    && check "validate rejects target_table without schema prefix" ok \
    || check "validate rejects target_table without schema prefix" fail

# hyphenated table name rejected by validate (BUG-7 fixed)
cat > "$S3/hyphen_target.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: my-table
    source_table: icube.SALESINVOICE
    target_table: bronze.my-table
    strategy: full
YAML
code=$("$FEATHER" validate --config "$S3/hyphen_target.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$code" != "0" ]] \
    && check "validate rejects hyphenated target_table" ok \
    || check "validate rejects hyphenated target_table" fail

echo ""

# ---------------------------------------------------------------------------
# S4 — feather discover (client fixture)
# ---------------------------------------------------------------------------
yellow "--- S4: feather discover ---"
out=$("$FEATHER" discover --config "$S2/feather.yaml" 2>&1)
echo "$out" | grep -q "Found 6 table" \
    && check "discover finds 6 icube tables" ok \
    || check "discover finds 6 icube tables" fail
echo "$out" | grep -q "icube.SALESINVOICE" \
    && check "discover shows icube.SALESINVOICE" ok \
    || check "discover shows icube.SALESINVOICE" fail
echo "$out" | grep -q "ModifiedDate" \
    && check "discover shows columns" ok \
    || check "discover shows columns" fail

echo ""

# ---------------------------------------------------------------------------
# S5 — feather setup + run + status (client fixture)
# ---------------------------------------------------------------------------
yellow "--- S5: feather setup / run / status ---"
"$FEATHER" setup --config "$S2/feather.yaml" > /dev/null 2>&1
[[ -f "$S2/feather_state.duckdb" ]] \
    && check "setup creates feather_state.duckdb" ok \
    || check "setup creates feather_state.duckdb" fail

out=$("$FEATHER" run --config "$S2/feather.yaml" 2>&1)
echo "$out" | grep -q "3/3 tables extracted" \
    && check "run: 3/3 tables succeed" ok \
    || check "run: 3/3 tables succeed" fail

[[ -f "$S2/feather_data.duckdb" ]] \
    && check "run creates feather_data.duckdb" ok \
    || check "run creates feather_data.duckdb" fail

# verify row counts in bronze
uv run python -c "
import duckdb
con = duckdb.connect('$S2/feather_data.duckdb', read_only=True)
assert con.execute('SELECT COUNT(*) FROM bronze.sales_invoice').fetchone()[0]   == 11676
assert con.execute('SELECT COUNT(*) FROM bronze.customer_master').fetchone()[0] == 1339
assert con.execute('SELECT COUNT(*) FROM bronze.inventory_group').fetchone()[0] == 66
con.close()
" \
    && check "row counts correct: sales_invoice=11676 customer=1339 inv_group=66" ok \
    || check "row counts correct: sales_invoice=11676 customer=1339 inv_group=66" fail

# verify _etl metadata columns exist
uv run python -c "
import duckdb
con = duckdb.connect('$S2/feather_data.duckdb', read_only=True)
cols = [r[0] for r in con.execute(\"SELECT column_name FROM information_schema.columns WHERE table_schema='bronze' AND table_name='sales_invoice'\").fetchall()]
assert '_etl_loaded_at' in cols
assert '_etl_run_id' in cols
con.close()
" \
    && check "_etl_loaded_at and _etl_run_id metadata columns present" ok \
    || check "_etl_loaded_at and _etl_run_id metadata columns present" fail

# run again (idempotency — change detection skips unchanged)
out=$("$FEATHER" run --config "$S2/feather.yaml" 2>&1)
echo "$out" | grep -q "3 skipped" \
    && check "second run (idempotency): all tables skipped (unchanged)" ok \
    || check "second run (idempotency): all tables skipped (unchanged)" fail

out=$("$FEATHER" status --config "$S2/feather.yaml" 2>&1)
echo "$out" | grep -q "sales_invoice" \
    && check "status shows sales_invoice" ok \
    || check "status shows sales_invoice" fail
echo "$out" | grep -q "skipped" \
    && check "status shows skipped (after idempotent re-run)" ok \
    || check "status shows skipped (after idempotent re-run)" fail

echo ""

# ---------------------------------------------------------------------------
# S6 — feather run with partial failure (error isolation)
# ---------------------------------------------------------------------------
yellow "--- S6: partial failure — error isolation ---"
S6="$WORK_DIR/s6" && mkdir "$S6"
cp "$FIXTURE_DIR/client.duckdb" "$S6/source.duckdb"
cat > "$S6/feather.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: good_table
    source_table: icube.SALESINVOICE
    target_table: bronze.good_table
    strategy: full
  - name: bad_table
    source_table: icube.NONEXISTENT_TABLE
    target_table: bronze.bad_table
    strategy: full
YAML

out=$("$FEATHER" run --config "$S6/feather.yaml" 2>&1) || true
echo "$out" | grep -q "good_table: success" \
    && check "good table succeeds despite bad table" ok \
    || check "good table succeeds despite bad table" fail
echo "$out" | grep -q "bad_table: failure" \
    && check "bad table shows failure" ok \
    || check "bad table shows failure" fail
echo "$out" | grep -q "1/2 tables extracted" \
    && check "summary shows 1/2" ok \
    || check "summary shows 1/2" fail

# exit code is non-zero on partial failure (BUG-5 fixed)
code=$("$FEATHER" run --config "$S6/feather.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$code" != "0" ]] \
    && check "exit code non-zero on partial failure" ok \
    || check "exit code non-zero on partial failure" fail

# error is stored in state DB
uv run python -c "
import duckdb
con = duckdb.connect('$S6/feather_state.duckdb', read_only=True)
row = con.execute(\"SELECT error_message FROM _runs WHERE table_name='bad_table' AND status='failure' LIMIT 1\").fetchone()
assert row is not None and row[0] is not None and len(row[0]) > 0
con.close()
" \
    && check "error_message stored in _runs" ok \
    || check "error_message stored in _runs" fail

echo ""

# ---------------------------------------------------------------------------
# S7 — feather run without prior setup (auto-init)
# ---------------------------------------------------------------------------
yellow "--- S7: run without setup auto-creates state and data DBs ---"
S7="$WORK_DIR/s7" && mkdir "$S7"
cp "$FIXTURE_DIR/client.duckdb" "$S7/source.duckdb"
cat > "$S7/feather.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: orders
    source_table: icube.SALESINVOICE
    target_table: bronze.orders
    strategy: full
YAML
out=$("$FEATHER" run --config "$S7/feather.yaml" 2>&1)
[[ -f "$S7/feather_data.duckdb" && -f "$S7/feather_state.duckdb" ]] \
    && check "run without setup creates both DBs" ok \
    || check "run without setup creates both DBs" fail

echo ""

# ---------------------------------------------------------------------------
# S8 — sample_erp fixture (custom data)
# ---------------------------------------------------------------------------
yellow "--- S8: sample_erp fixture (custom synthetic data) ---"
S8="$WORK_DIR/s8" && mkdir "$S8"
cp "$FIXTURE_DIR/sample_erp.duckdb" "$S8/source.duckdb"
cat > "$S8/feather.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: orders
    source_table: erp.orders
    target_table: bronze.orders
    strategy: full
  - name: customers
    source_table: erp.customers
    target_table: bronze.customers
    strategy: full
  - name: products
    source_table: erp.products
    target_table: bronze.products
    strategy: full
YAML

out=$("$FEATHER" run --config "$S8/feather.yaml" 2>&1)
echo "$out" | grep -q "3/3 tables extracted" \
    && check "sample_erp: 3/3 tables succeed" ok \
    || check "sample_erp: 3/3 tables succeed" fail

# NULL in stock_qty passes through
uv run python -c "
import duckdb
con = duckdb.connect('$S8/feather_data.duckdb', read_only=True)
row = con.execute(\"SELECT stock_qty FROM bronze.products WHERE product_name='Service Pack'\").fetchone()
assert row is not None and row[0] is None, f'expected NULL, got {row}'
con.close()
" \
    && check "NULL stock_qty passes through correctly" ok \
    || check "NULL stock_qty passes through correctly" fail

# verify exact row counts
uv run python -c "
import duckdb
con = duckdb.connect('$S8/feather_data.duckdb', read_only=True)
assert con.execute('SELECT COUNT(*) FROM bronze.orders').fetchone()[0]    == 5
assert con.execute('SELECT COUNT(*) FROM bronze.customers').fetchone()[0] == 4
assert con.execute('SELECT COUNT(*) FROM bronze.products').fetchone()[0]  == 3
con.close()
" \
    && check "sample_erp row counts: orders=5 customers=4 products=3" ok \
    || check "sample_erp row counts: orders=5 customers=4 products=3" fail

echo ""

# ---------------------------------------------------------------------------
# S9 — tables/ directory merge
# ---------------------------------------------------------------------------
yellow "--- S9: tables/ directory merge ---"
S9="$WORK_DIR/s9" && mkdir -p "$S9/tables"
cp "$FIXTURE_DIR/sample_erp.duckdb" "$S9/source.duckdb"
cat > "$S9/feather.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables: []
YAML
cat > "$S9/tables/sales.yaml" << 'YAML'
tables:
  - name: orders
    source_table: erp.orders
    target_table: bronze.orders
    strategy: full
YAML
cat > "$S9/tables/master.yaml" << 'YAML'
tables:
  - name: customers
    source_table: erp.customers
    target_table: bronze.customers
    strategy: full
YAML

out=$("$FEATHER" validate --config "$S9/feather.yaml" 2>&1)
echo "$out" | grep -q "Config valid: 2 table" \
    && check "tables/ dir merge: 2 tables discovered" ok \
    || check "tables/ dir merge: 2 tables discovered" fail

out=$("$FEATHER" run --config "$S9/feather.yaml" 2>&1)
echo "$out" | grep -q "2/2 tables extracted" \
    && check "tables/ dir merge: 2/2 run succeeds" ok \
    || check "tables/ dir merge: 2/2 run succeeds" fail

echo ""

# ---------------------------------------------------------------------------
# S10 — --config flag with absolute and relative paths from a different CWD
# ---------------------------------------------------------------------------
yellow "--- S10: --config path resolution from different CWD ---"
out=$(cd /tmp && "$FEATHER" run --config "$S8/feather.yaml" 2>&1)
# S8 already ran, so change detection skips — just verify exit code 0
code=$(cd /tmp && "$FEATHER" run --config "$S8/feather.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$code" == "0" ]] \
    && check "--config absolute path from different CWD works" ok \
    || check "--config absolute path from different CWD works" fail

echo ""

# ---------------------------------------------------------------------------
# S11 — BLOB columns and columns with spaces (SALESINVOICEMASTER)
# ---------------------------------------------------------------------------
yellow "--- S11: BLOB columns and column names with spaces ---"
S11="$WORK_DIR/s11" && mkdir "$S11"
cp "$FIXTURE_DIR/client.duckdb" "$S11/source.duckdb"
cat > "$S11/feather.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: sales_invoice_master
    source_table: icube.SALESINVOICEMASTER
    target_table: bronze.sales_invoice_master
    strategy: full
  - name: inv_item
    source_table: icube.INVITEM
    target_table: bronze.inv_item
    strategy: full
YAML

out=$("$FEATHER" run --config "$S11/feather.yaml" 2>&1)
echo "$out" | grep -q "sales_invoice_master: success" \
    && check "SALESINVOICEMASTER (has 'Round Off' column) loads successfully" ok \
    || check "SALESINVOICEMASTER (has 'Round Off' column) loads successfully" fail
echo "$out" | grep -q "inv_item: success" \
    && check "INVITEM (has BLOB columns, ~200 cols) loads successfully" ok \
    || check "INVITEM (has BLOB columns, ~200 cols) loads successfully" fail

uv run python -c "
import duckdb
con = duckdb.connect('$S11/feather_data.duckdb', read_only=True)
cols = [r[0] for r in con.execute(\"SELECT column_name FROM information_schema.columns WHERE table_name='sales_invoice_master'\").fetchall()]
assert 'Round Off' in cols, 'Round Off column missing'
con.close()
" \
    && check "'Round Off' (space in column name) preserved in bronze" ok \
    || check "'Round Off' (space in column name) preserved in bronze" fail

echo ""

# ---------------------------------------------------------------------------
# S12 — status before setup / after setup with no runs / after runs
# ---------------------------------------------------------------------------
yellow "--- S12: status edge cases ---"
S12="$WORK_DIR/s12" && mkdir "$S12"
cp "$FIXTURE_DIR/sample_erp.duckdb" "$S12/source.duckdb"
cat > "$S12/feather.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: orders
    source_table: erp.orders
    target_table: bronze.orders
    strategy: full
YAML

# status before setup
out=$("$FEATHER" status --config "$S12/feather.yaml" 2>&1) || true
echo "$out" | grep -q "No state DB found" \
    && check "status before setup shows 'No state DB found'" ok \
    || check "status before setup shows 'No state DB found'" fail

"$FEATHER" setup --config "$S12/feather.yaml" > /dev/null 2>&1

# status after setup but before run
out=$("$FEATHER" status --config "$S12/feather.yaml" 2>&1)
echo "$out" | grep -q "No runs recorded yet" \
    && check "status after setup but before run shows 'No runs recorded yet'" ok \
    || check "status after setup but before run shows 'No runs recorded yet'" fail

"$FEATHER" run --config "$S12/feather.yaml" > /dev/null 2>&1

out=$("$FEATHER" status --config "$S12/feather.yaml" 2>&1)
echo "$out" | grep -q "orders" \
    && check "status after run shows table" ok \
    || check "status after run shows table" fail

echo ""

# ---------------------------------------------------------------------------
# S13 — missing feather.yaml shows friendly error (BUG-3 fixed)
# ---------------------------------------------------------------------------
yellow "--- S13: missing feather.yaml ---"
out=$(cd "$WORK_DIR" && "$FEATHER" validate 2>&1) || true
echo "$out" | grep -q "Config file not found" \
    && check "missing feather.yaml shows friendly error message" ok \
    || check "missing feather.yaml shows friendly error message" fail

echo ""

# ---------------------------------------------------------------------------
# S14 — error message appears only on stdout, not duplicated (BUG-1 fixed)
# ---------------------------------------------------------------------------
yellow "--- S14: error output not duplicated ---"
S14="$WORK_DIR/s14" && mkdir "$S14"
cp "$FIXTURE_DIR/sample_erp.duckdb" "$S14/source.duckdb"
cat > "$S14/feather.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: bad
    source_table: erp.NOSUCH
    target_table: bronze.bad
    strategy: full
YAML

stdout_lines=$({ "$FEATHER" run --config "$S14/feather.yaml" 2>/dev/null || true; } | grep -c "NOSUCH" || true)
stderr_lines=$({ "$FEATHER" run --config "$S14/feather.yaml" 2>&1 1>/dev/null || true; } | grep -c "NOSUCH" || true)
# error should appear on stdout only, not on stderr
[[ "$stdout_lines" -gt 0 && "$stderr_lines" == 0 ]] \
    && check "error appears on stdout only (not duplicated on stderr)" ok \
    || check "error appears on stdout only (not duplicated on stderr)" fail

echo ""

# ---------------------------------------------------------------------------
# S15 — CSV source: validate, discover, run
# ---------------------------------------------------------------------------
yellow "--- S15: CSV source validate + discover + run ---"
S15="$WORK_DIR/s15" && mkdir "$S15"
cp -r "$FIXTURE_DIR/csv_data" "$S15/csv_data"
cat > "$S15/feather.yaml" << YAML
source:
  type: csv
  path: ./csv_data
destination:
  path: ./feather_data.duckdb
tables:
  - name: orders
    source_table: orders.csv
    target_table: bronze.orders
    strategy: full
  - name: customers
    source_table: customers.csv
    target_table: bronze.customers
    strategy: full
  - name: products
    source_table: products.csv
    target_table: bronze.products
    strategy: full
YAML
validate_out=$("$FEATHER" validate --config "$S15/feather.yaml" 2>&1) || true
echo "$validate_out" | grep -q "3 table" \
    && check "csv validate succeeds with 3 tables" ok \
    || check "csv validate succeeds with 3 tables" fail

discover_out=$("$FEATHER" discover --config "$S15/feather.yaml" 2>&1) || true
echo "$discover_out" | grep -q "orders.csv" \
    && check "csv discover finds orders.csv" ok \
    || check "csv discover finds orders.csv" fail

run_out=$("$FEATHER" run --config "$S15/feather.yaml" 2>&1) || true
echo "$run_out" | grep -q "3/3" \
    && check "csv run: 3/3 tables succeed" ok \
    || check "csv run: 3/3 tables succeed" fail

echo ""

# ---------------------------------------------------------------------------
# S16a — CSV validation: file path instead of directory rejected
# ---------------------------------------------------------------------------
yellow "--- S16a: CSV rejects file path (not directory) ---"
S16A="$WORK_DIR/s16a" && mkdir "$S16A"
touch "$S16A/source.csv"
cat > "$S16A/feather.yaml" << 'YAML'
source:
  type: csv
  path: ./source.csv
destination:
  path: ./feather_data.duckdb
tables:
  - name: data
    source_table: data.csv
    target_table: bronze.data
    strategy: full
YAML
validate_code=$("$FEATHER" validate --config "$S16A/feather.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$validate_code" != "0" ]] \
    && check "csv rejects file path (not directory)" ok \
    || check "csv rejects file path (not directory)" fail

echo ""

# ---------------------------------------------------------------------------
# S17 — SQLite source: validate, discover, run
# ---------------------------------------------------------------------------
yellow "--- S17: SQLite source validate + discover + run ---"
S17="$WORK_DIR/s17" && mkdir "$S17"
cp "$FIXTURE_DIR/sample_erp.sqlite" "$S17/source.sqlite"
cat > "$S17/feather.yaml" << YAML
source:
  type: sqlite
  path: ./source.sqlite
destination:
  path: ./feather_data.duckdb
tables:
  - name: orders
    source_table: orders
    target_table: bronze.orders
    strategy: full
  - name: customers
    source_table: customers
    target_table: bronze.customers
    strategy: full
  - name: products
    source_table: products
    target_table: bronze.products
    strategy: full
YAML
validate_out=$("$FEATHER" validate --config "$S17/feather.yaml" 2>&1) || true
echo "$validate_out" | grep -q "3 table" \
    && check "sqlite validate succeeds with 3 tables" ok \
    || check "sqlite validate succeeds with 3 tables" fail

discover_out=$("$FEATHER" discover --config "$S17/feather.yaml" 2>&1) || true
echo "$discover_out" | grep -q "orders" \
    && check "sqlite discover finds orders table" ok \
    || check "sqlite discover finds orders table" fail

run_out=$("$FEATHER" run --config "$S17/feather.yaml" 2>&1) || true
echo "$run_out" | grep -q "3/3" \
    && check "sqlite run: 3/3 tables succeed" ok \
    || check "sqlite run: 3/3 tables succeed" fail

echo ""

# ---------------------------------------------------------------------------
# S18 — hyphenated target_table rejected at validate (BUG-7 fixed)
# ---------------------------------------------------------------------------
yellow "--- S18: hyphenated target_table rejected at validate ---"
S18="$WORK_DIR/s18" && mkdir "$S18"
cp "$FIXTURE_DIR/sample_erp.duckdb" "$S18/source.duckdb"
cat > "$S18/feather.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: my-table
    source_table: erp.orders
    target_table: bronze.my-table
    strategy: full
YAML
validate_code=$("$FEATHER" validate --config "$S18/feather.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$validate_code" != "0" ]] \
    && check "hyphenated target_table rejected at validate" ok \
    || check "hyphenated target_table rejected at validate" fail

# ---------------------------------------------------------------------------
# S19–S22 — Change detection (Slice 2)
# ---------------------------------------------------------------------------
yellow "--- S19-S22: Change detection (skip unchanged files) ---"
S19="$WORK_DIR/s19" && mkdir "$S19"
cp "$FIXTURE_DIR/sample_erp.duckdb" "$S19/source.duckdb"
cat > "$S19/feather.yaml" << 'YAML'
source:
  type: duckdb
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: orders
    source_table: erp.orders
    target_table: bronze.orders
    strategy: full
YAML

# S19: First run → success
out=$("$FEATHER" run --config "$S19/feather.yaml" 2>&1) || true
echo "$out" | grep -q "orders: success" \
    && check "S19: first run extracts successfully" ok \
    || check "S19: first run extracts successfully" fail

# S20: Second run (unchanged) → skipped
out=$("$FEATHER" run --config "$S19/feather.yaml" 2>&1) || true
echo "$out" | grep -q "skipped (unchanged)" \
    && check "S20: second run skips unchanged file" ok \
    || check "S20: second run skips unchanged file" fail
code=$("$FEATHER" run --config "$S19/feather.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$code" == "0" ]] \
    && check "S20: skipped run exits with code 0" ok \
    || check "S20: skipped run exits with code 0" fail

# S21: Modify source → re-extracts
uv run python -c "
import duckdb
con = duckdb.connect('$S19/source.duckdb')
con.execute(\"INSERT INTO erp.orders (order_id, customer_id, order_date, total_amount, status) VALUES (99, 99, '2026-01-01', 999.99, 'new')\")
con.close()
"
out=$("$FEATHER" run --config "$S19/feather.yaml" 2>&1) || true
echo "$out" | grep -q "orders: success" \
    && check "S21: modified source re-extracts" ok \
    || check "S21: modified source re-extracts" fail

# S22: Touch source (mtime changes, content identical) → skipped
sleep 0.1
touch "$S19/source.duckdb"
out=$("$FEATHER" run --config "$S19/feather.yaml" 2>&1) || true
echo "$out" | grep -q "skipped (unchanged)" \
    && check "S22: touched file skipped (hash unchanged)" ok \
    || check "S22: touched file skipped (hash unchanged)" fail

# ---------------------------------------------------------------------------
# S-INCR — Incremental extraction
# ---------------------------------------------------------------------------
yellow "--- S-INCR: Incremental extraction ---"

SINCR="$WORK_DIR/incr_test"
mkdir -p "$SINCR"

# Copy sample_erp.duckdb as incremental source
cp "$FIXTURE_DIR/sample_erp.duckdb" "$SINCR/source.duckdb"

# Write config with incremental strategy
cat > "$SINCR/feather.yaml" <<YAML
source:
  type: duckdb
  path: source.duckdb
destination:
  path: dest.duckdb
tables:
  - name: sales
    source_table: erp.sales
    target_table: bronze.sales
    strategy: incremental
    timestamp_column: modified_at
YAML

# S-INCR-1: First incremental run extracts all rows
out=$("$FEATHER" run --config "$SINCR/feather.yaml" 2>&1) || true
echo "$out" | grep -q "sales: success (10 rows)" \
    && check "S-INCR-1: first incremental run extracts all 10 rows" ok \
    || check "S-INCR-1: first incremental run extracts all 10 rows" fail

# S-INCR-2: Verify watermark is set
wm=$(uv run python -c "
import duckdb
con = duckdb.connect('$SINCR/feather_state.duckdb', read_only=True)
row = con.execute(\"SELECT last_value FROM _watermarks WHERE table_name = 'sales'\").fetchone()
print(row[0] if row else 'NONE')
con.close()
")
[[ "$wm" == *"2025-01-10"* ]] \
    && check "S-INCR-2: watermark set to MAX(modified_at) = 2025-01-10" ok \
    || check "S-INCR-2: watermark set to MAX(modified_at) = 2025-01-10" fail

# S-INCR-3: Second run, file unchanged → skipped
out=$("$FEATHER" run --config "$SINCR/feather.yaml" 2>&1) || true
echo "$out" | grep -q "skipped (unchanged)" \
    && check "S-INCR-3: second run skips unchanged source" ok \
    || check "S-INCR-3: second run skips unchanged source" fail

# S-INCR-4: Add new rows → only new rows + overlap extracted
uv run python -c "
import duckdb
con = duckdb.connect('$SINCR/source.duckdb')
con.execute(\"INSERT INTO erp.sales VALUES (11, 105, 1100.00, 'completed', '2025-01-11 00:00:00'), (12, 106, 1200.00, 'completed', '2025-01-12 00:00:00')\")
con.close()
"
out=$("$FEATHER" run --config "$SINCR/feather.yaml" 2>&1) || true
echo "$out" | grep -q "sales: success (3 rows)" \
    && check "S-INCR-4: incremental extracts only new + overlap rows (3)" ok \
    || check "S-INCR-4: incremental extracts only new + overlap rows (3)" fail

# S-INCR-5: Watermark advanced to new MAX
wm2=$(uv run python -c "
import duckdb
con = duckdb.connect('$SINCR/feather_state.duckdb', read_only=True)
row = con.execute(\"SELECT last_value FROM _watermarks WHERE table_name = 'sales'\").fetchone()
print(row[0] if row else 'NONE')
con.close()
")
[[ "$wm2" == *"2025-01-12"* ]] \
    && check "S-INCR-5: watermark advanced to 2025-01-12" ok \
    || check "S-INCR-5: watermark advanced to 2025-01-12" fail

# S-INCR-6: Destination has correct total rows (12)
dest_rows=$(uv run python -c "
import duckdb
con = duckdb.connect('$SINCR/dest.duckdb', read_only=True)
print(con.execute('SELECT COUNT(*) FROM bronze.sales').fetchone()[0])
con.close()
")
[[ "$dest_rows" == "12" ]] \
    && check "S-INCR-6: destination has 12 total rows after incremental" ok \
    || check "S-INCR-6: destination has 12 total rows after incremental" fail

# S-INCR-7: Filter excludes matching rows
SINCR_F="$WORK_DIR/incr_filter"
mkdir -p "$SINCR_F"
cp "$FIXTURE_DIR/sample_erp.duckdb" "$SINCR_F/source.duckdb"

cat > "$SINCR_F/feather.yaml" <<YAML
source:
  type: duckdb
  path: source.duckdb
destination:
  path: dest.duckdb
tables:
  - name: sales
    source_table: erp.sales
    target_table: bronze.sales
    strategy: incremental
    timestamp_column: modified_at
    filter: "status <> 'cancelled'"
YAML

out=$("$FEATHER" run --config "$SINCR_F/feather.yaml" 2>&1) || true
echo "$out" | grep -q "sales: success (8 rows)" \
    && check "S-INCR-7: filter excludes cancelled rows (8 of 10 extracted)" ok \
    || check "S-INCR-7: filter excludes cancelled rows (8 of 10 extracted)" fail

# S-INCR-8: No cancelled rows in destination
cancelled=$(uv run python -c "
import duckdb
con = duckdb.connect('$SINCR_F/dest.duckdb', read_only=True)
print(con.execute(\"SELECT COUNT(*) FROM bronze.sales WHERE status = 'cancelled'\").fetchone()[0])
con.close()
")
[[ "$cancelled" == "0" ]] \
    && check "S-INCR-8: no cancelled rows in filtered destination" ok \
    || check "S-INCR-8: no cancelled rows in filtered destination" fail

echo ""

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
yellow "======================================="
yellow "  Results: $PASS passed, $FAIL failed"
yellow "======================================="
echo ""

if [[ "$FAIL" -gt 0 ]]; then
    red "Some checks failed — see output above."
    exit 1
else
    green "All checks passed."
    exit 0
fi
