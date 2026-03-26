"""
Create the sample_erp.duckdb test fixture used in integration and hands-on tests.

This fixture is a minimal synthetic ERP database with three tables in the `erp`
schema.  It is intentionally small so it runs fast, but covers the important
schema features that the Icube client fixture does not exercise:

  - erp.orders      — 5 rows, standard numeric / date / varchar types
  - erp.customers   — 4 rows, DECIMAL credit_limit, TIMESTAMP created_at
  - erp.products    — 3 rows, one NULL in stock_qty (tests NULL handling)

Run from the repo root:

    python scripts/create_sample_erp_fixture.py

The script is idempotent — re-running recreates the file from scratch.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

FIXTURE_PATH = Path(__file__).parent.parent / "tests" / "fixtures" / "sample_erp.duckdb"


def create_fixture(path: Path) -> None:
    if path.exists():
        path.unlink()

    con = duckdb.connect(str(path))

    con.execute("CREATE SCHEMA erp")

    # --- orders ---
    con.execute("""
        CREATE TABLE erp.orders (
            order_id     INTEGER PRIMARY KEY,
            customer_id  INTEGER,
            order_date   DATE,
            total_amount DECIMAL(10,2),
            status       VARCHAR,
            created_at   TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO erp.orders VALUES
        (1, 101, '2025-01-15', 1250.00, 'shipped',   '2025-01-15 09:00:00'),
        (2, 102, '2025-01-16',  450.50, 'pending',   '2025-01-16 10:30:00'),
        (3, 101, '2025-01-20', 3200.75, 'delivered', '2025-01-20 14:00:00'),
        (4, 103, '2025-02-01',  789.00, 'shipped',   '2025-02-01 08:00:00'),
        (5, 104, '2025-02-10',  125.25, 'cancelled', '2025-02-10 11:00:00')
    """)

    # --- customers ---
    con.execute("""
        CREATE TABLE erp.customers (
            customer_id  INTEGER PRIMARY KEY,
            name         VARCHAR,
            email        VARCHAR,
            city         VARCHAR,
            credit_limit DECIMAL(10,2),
            created_at   TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO erp.customers VALUES
        (101, 'Acme Corp',  'billing@acme.com',    'Mumbai',    50000.00, '2024-01-01 00:00:00'),
        (102, 'Globe Ltd',  'accounts@globe.co',   'Delhi',     25000.00, '2024-03-15 00:00:00'),
        (103, 'Nexus Inc',  'finance@nexus.in',    'Bangalore', 75000.00, '2024-06-01 00:00:00'),
        (104, 'Apex Co',    'pay@apex.com',         'Chennai',  10000.00, '2024-09-01 00:00:00')
    """)

    # --- products — intentional NULL in stock_qty to test NULL pass-through ---
    con.execute("""
        CREATE TABLE erp.products (
            product_id   INTEGER PRIMARY KEY,
            product_name VARCHAR,
            category     VARCHAR,
            unit_price   DECIMAL(10,2),
            stock_qty    INTEGER          -- NULL for service products
        )
    """)
    con.execute("""
        INSERT INTO erp.products VALUES
        (1, 'Widget A',    'Hardware',    45.99,  500),
        (2, 'Gadget Pro',  'Electronics', 299.00, 120),
        (3, 'Service Pack','Services',   1500.00, NULL)
    """)

    con.close()

    orders_n    = duckdb.connect(str(path), read_only=True).execute("SELECT COUNT(*) FROM erp.orders").fetchone()[0]
    customers_n = duckdb.connect(str(path), read_only=True).execute("SELECT COUNT(*) FROM erp.customers").fetchone()[0]
    products_n  = duckdb.connect(str(path), read_only=True).execute("SELECT COUNT(*) FROM erp.products").fetchone()[0]

    print(f"Created {path}")
    print(f"  erp.orders:    {orders_n} rows")
    print(f"  erp.customers: {customers_n} rows")
    print(f"  erp.products:  {products_n} rows  (row 3 has NULL stock_qty)")


if __name__ == "__main__":
    create_fixture(FIXTURE_PATH)
