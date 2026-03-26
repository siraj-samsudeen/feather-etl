#!/usr/bin/env bash
# =============================================================================
# hands_on_test.sh — Reproduce the full hands-on review workflow
#
# This script recreates every scenario tested in the slice-1 hands-on review.
# Run it from the repo root with the venv active:
#
#   source .venv/bin/activate
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

# init with "." as name → pyproject name becomes "" — must cd into the dir first
S1B="$WORK_DIR/s1b" && mkdir "$S1B"
(cd "$S1B" && "$FEATHER" init . > /dev/null 2>&1)
grep -q 'name = ""' "$S1B/pyproject.toml" \
    && check "init '.' produces empty project name (known UX issue)" ok \
    || check "init '.' produces empty project name (known UX issue)" fail

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

python3 -c "
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

# unimplemented source type accepted by validate (known bug BUG-2)
cat > "$S3/csv_source.yaml" << 'YAML'
source:
  type: csv
  path: ./source.duckdb
destination:
  path: ./feather_data.duckdb
tables:
  - name: t
    source_table: main.t
    target_table: bronze.t
    strategy: full
YAML
code=$("$FEATHER" validate --config "$S3/csv_source.yaml" > /dev/null 2>&1; echo $?) || true
# currently exits 0 (BUG-2) — this check documents the current wrong behaviour
[[ "$code" == "0" ]] \
    && check "BUG-2: validate accepts csv (unimplemented) — exits 0 (should be non-zero)" ok \
    || check "BUG-2: validate accepts csv (unimplemented) — exits 0 (should be non-zero)" fail

# target_table without schema prefix accepted by validate (known BUG-7)
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
out=$("$FEATHER" validate --config "$S3/no_schema_target.yaml" 2>&1)
echo "$out" | grep -q "Config valid" \
    && check "BUG-7: validate accepts target_table with no schema prefix (should fail)" ok \
    || check "BUG-7: validate accepts target_table with no schema prefix (should fail)" fail

# hyphenated table name accepted by validate (known BUG-7)
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
out=$("$FEATHER" validate --config "$S3/hyphen_target.yaml" 2>&1)
echo "$out" | grep -q "Config valid" \
    && check "BUG-7: validate accepts hyphenated target_table (should fail)" ok \
    || check "BUG-7: validate accepts hyphenated target_table (should fail)" fail

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
echo "$out" | grep -q "3/3 tables succeeded" \
    && check "run: 3/3 tables succeed" ok \
    || check "run: 3/3 tables succeed" fail

[[ -f "$S2/feather_data.duckdb" ]] \
    && check "run creates feather_data.duckdb" ok \
    || check "run creates feather_data.duckdb" fail

# verify row counts in bronze
python3 -c "
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
python3 -c "
import duckdb
con = duckdb.connect('$S2/feather_data.duckdb', read_only=True)
cols = [r[0] for r in con.execute(\"SELECT column_name FROM information_schema.columns WHERE table_schema='bronze' AND table_name='sales_invoice'\").fetchall()]
assert '_etl_loaded_at' in cols
assert '_etl_run_id' in cols
con.close()
" \
    && check "_etl_loaded_at and _etl_run_id metadata columns present" ok \
    || check "_etl_loaded_at and _etl_run_id metadata columns present" fail

# run again (idempotency)
out=$("$FEATHER" run --config "$S2/feather.yaml" 2>&1)
echo "$out" | grep -q "3/3 tables succeeded" \
    && check "second run (idempotency): 3/3 succeed" ok \
    || check "second run (idempotency): 3/3 succeed" fail

out=$("$FEATHER" status --config "$S2/feather.yaml" 2>&1)
echo "$out" | grep -q "sales_invoice" \
    && check "status shows sales_invoice" ok \
    || check "status shows sales_invoice" fail
echo "$out" | grep -q "success" \
    && check "status shows success" ok \
    || check "status shows success" fail

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

out=$("$FEATHER" run --config "$S6/feather.yaml" 2>&1)
echo "$out" | grep -q "good_table: success" \
    && check "good table succeeds despite bad table" ok \
    || check "good table succeeds despite bad table" fail
echo "$out" | grep -q "bad_table: failure" \
    && check "bad table shows failure" ok \
    || check "bad table shows failure" fail
echo "$out" | grep -q "1/2 tables succeeded" \
    && check "summary shows 1/2" ok \
    || check "summary shows 1/2" fail

# BUG-5: exit code is 0 on partial failure (known bug)
code=$("$FEATHER" run --config "$S6/feather.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$code" == "0" ]] \
    && check "BUG-5: exit code 0 on partial failure (should be 1)" ok \
    || check "BUG-5: exit code 0 on partial failure (should be 1)" fail

# error is stored in state DB
python3 -c "
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
echo "$out" | grep -q "3/3 tables succeeded" \
    && check "sample_erp: 3/3 tables succeed" ok \
    || check "sample_erp: 3/3 tables succeed" fail

# NULL in stock_qty passes through
python3 -c "
import duckdb
con = duckdb.connect('$S8/feather_data.duckdb', read_only=True)
row = con.execute(\"SELECT stock_qty FROM bronze.products WHERE product_name='Service Pack'\").fetchone()
assert row is not None and row[0] is None, f'expected NULL, got {row}'
con.close()
" \
    && check "NULL stock_qty passes through correctly" ok \
    || check "NULL stock_qty passes through correctly" fail

# verify exact row counts
python3 -c "
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
echo "$out" | grep -q "2/2 tables succeeded" \
    && check "tables/ dir merge: 2/2 run succeeds" ok \
    || check "tables/ dir merge: 2/2 run succeeds" fail

echo ""

# ---------------------------------------------------------------------------
# S10 — --config flag with absolute and relative paths from a different CWD
# ---------------------------------------------------------------------------
yellow "--- S10: --config path resolution from different CWD ---"
out=$(cd /tmp && "$FEATHER" run --config "$S8/feather.yaml" 2>&1)
echo "$out" | grep -q "3/3 tables succeeded" \
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

python3 -c "
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
# S13 — BUG-3: missing feather.yaml gives unhandled FileNotFoundError traceback
# ---------------------------------------------------------------------------
yellow "--- S13: BUG-3 missing feather.yaml ---"
out=$(cd "$WORK_DIR" && "$FEATHER" validate 2>&1) || true
echo "$out" | grep -q "FileNotFoundError" \
    && check "BUG-3: missing feather.yaml shows raw FileNotFoundError traceback (should show friendly message)" ok \
    || check "BUG-3: missing feather.yaml shows raw FileNotFoundError traceback (should show friendly message)" fail

echo ""

# ---------------------------------------------------------------------------
# S14 — BUG-1: error message duplicated (stderr + stdout)
# ---------------------------------------------------------------------------
yellow "--- S14: BUG-1 duplicate error output ---"
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

stdout_lines=$("$FEATHER" run --config "$S14/feather.yaml" 2>/dev/null | grep -c "NOSUCH" || true)
stderr_lines=$("$FEATHER" run --config "$S14/feather.yaml" 2>&1 1>/dev/null | grep -c "NOSUCH" || true)
# both > 0 → duplicate
[[ "$stdout_lines" -gt 0 && "$stderr_lines" -gt 0 ]] \
    && check "BUG-1: error appears on both stdout and stderr (duplicate)" ok \
    || check "BUG-1: error appears on both stdout and stderr (duplicate)" fail

echo ""

# ---------------------------------------------------------------------------
# S15 — BUG-2: csv source type passes validate but crashes on run
# ---------------------------------------------------------------------------
yellow "--- S15: BUG-2 unimplemented source type ---"
S15="$WORK_DIR/s15" && mkdir "$S15"
touch "$S15/source.csv"
cat > "$S15/feather.yaml" << 'YAML'
source:
  type: csv
  path: ./source.csv
destination:
  path: ./feather_data.duckdb
tables:
  - name: data
    source_table: main.data
    target_table: bronze.data
    strategy: full
YAML
validate_code=$("$FEATHER" validate --config "$S15/feather.yaml" > /dev/null 2>&1; echo $?) || true
run_code=$("$FEATHER" run --config "$S15/feather.yaml" > /dev/null 2>&1; echo $?) || true
# validate passes (0), run fails (non-zero) → gap
[[ "$validate_code" == "0" && "$run_code" != "0" ]] \
    && check "BUG-2: csv validate=0 but run non-zero (gap between validation and runtime)" ok \
    || check "BUG-2: csv validate=0 but run non-zero (gap between validation and runtime)" fail

echo ""

# ---------------------------------------------------------------------------
# S16 — BUG-7: hyphenated table name crashes run, not caught by validate
# ---------------------------------------------------------------------------
yellow "--- S16: BUG-7 hyphenated target_table name ---"
S16="$WORK_DIR/s16" && mkdir "$S16"
cp "$FIXTURE_DIR/sample_erp.duckdb" "$S16/source.duckdb"
cat > "$S16/feather.yaml" << 'YAML'
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
validate_code=$("$FEATHER" validate --config "$S16/feather.yaml" > /dev/null 2>&1; echo $?) || true
run_code=$("$FEATHER" run --config "$S16/feather.yaml" > /dev/null 2>&1; echo $?) || true
[[ "$validate_code" == "0" && "$run_code" == "0" ]] \
    && check "BUG-7: hyphen in target_table: validate=0, run=0 (run silently fails per-table but exits 0)" ok \
    || check "BUG-7: hyphen in target_table: validate=0, run=0" fail

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
