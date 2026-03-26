"""Extract a subset of real client data into test fixtures.

Source: afans-reporting-dev/discovery/source_data.duckdb
Output: tests/fixtures/client.duckdb (data before 2025-10-01)
        tests/fixtures/client_update.duckdb (data Oct-Dec 2025)
"""

import duckdb
from pathlib import Path

SOURCE = Path.home() / "Desktop/NonDropBoxProjects/afans-reporting-dev/discovery/source_data.duckdb"
FIXTURES = Path(__file__).parent.parent / "tests" / "fixtures"

# Tables with timestamp columns: extract date-windowed subsets
TIMESTAMPED = [
    ("SALESINVOICE", "ModifiedDate"),
    ("SALESINVOICEMASTER", "ModifyByDate"),
    ("INVITEM", '"Timestamp"'),
]

# Reference tables: extract all rows
REFERENCE = ["CUSTOMERMASTER", "EmployeeForm", "InventoryGroup"]

CUTOFF = "2025-10-01"
WINDOW_START = "2025-08-01"


def create_fixture(out_path: Path, queries: list[tuple[str, str]]):
    out_path.unlink(missing_ok=True)
    con = duckdb.connect(str(out_path))
    con.execute(f"ATTACH '{SOURCE}' AS src (READ_ONLY)")
    con.execute("CREATE SCHEMA IF NOT EXISTS icube")
    for table_name, query in queries:
        print(f"  {table_name}: ", end="")
        con.execute(f"CREATE TABLE icube.{table_name} AS {query}")
        count = con.execute(f"SELECT COUNT(*) FROM icube.{table_name}").fetchone()[0]
        print(f"{count} rows")
    con.close()


def main():
    FIXTURES.mkdir(parents=True, exist_ok=True)

    # client.duckdb — initial data (before cutoff)
    print("Creating client.duckdb (initial data)...")
    queries = []
    for table, ts_col in TIMESTAMPED:
        q = f"SELECT * FROM src.icube.{table} WHERE {ts_col} >= '{WINDOW_START}' AND {ts_col} < '{CUTOFF}'"
        queries.append((table, q))
    for table in REFERENCE:
        queries.append((table, f"SELECT * FROM src.icube.{table}"))
    create_fixture(FIXTURES / "client.duckdb", queries)

    # client_update.duckdb — new data (after cutoff)
    print("\nCreating client_update.duckdb (new data)...")
    queries = []
    for table, ts_col in TIMESTAMPED:
        q = f"SELECT * FROM src.icube.{table} WHERE {ts_col} >= '{CUTOFF}'"
        queries.append((table, q))
    create_fixture(FIXTURES / "client_update.duckdb", queries)

    print("\nDone.")


if __name__ == "__main__":
    main()
