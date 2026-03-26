"""DuckDB destination — loads data into local DuckDB file."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pyarrow as pa


SCHEMAS = ["bronze", "silver", "gold", "_quarantine"]


class DuckDBDestination:
    """Loads PyArrow Tables into a local DuckDB file."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> duckdb.DuckDBPyConnection:
        is_new = not self.path.exists()
        con = duckdb.connect(str(self.path))
        if is_new and os.name != "nt":
            try:
                os.chmod(self.path, 0o600)
            except OSError:
                pass
        return con

    def setup_schemas(self) -> None:
        con = self._connect()
        for schema in SCHEMAS:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        con.close()

    def load_full(self, table: str, data: pa.Table, run_id: str) -> int:
        """Full refresh via swap pattern (FR4.3)."""
        con = self._connect()

        # Parse schema.table_name
        parts = table.split(".")
        schema, table_name = parts[0], parts[1]
        staging = f"{schema}.{table_name}_new"
        final = f"{schema}.{table_name}"

        # Register arrow data so DuckDB can query it
        con.register("_arrow_data", data)

        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(
                f"CREATE TABLE {staging} AS "
                f"SELECT *, CURRENT_TIMESTAMP AS _etl_loaded_at, "
                f"'{run_id}' AS _etl_run_id FROM _arrow_data"
            )
            con.execute(f"DROP TABLE IF EXISTS {final}")
            # RENAME keeps table in same schema; result is {schema}.{table_name}
            con.execute(f"ALTER TABLE {staging} RENAME TO {table_name}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.unregister("_arrow_data")
            con.close()

        return data.num_rows
