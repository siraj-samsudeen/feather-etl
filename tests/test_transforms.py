"""Tests for the silver/gold transform engine (Slice 8)."""

from __future__ import annotations

import graphlib
from pathlib import Path

import duckdb
import pytest

from feather.transforms import (
    TransformMeta,
    build_execution_order,
    discover_transforms,
    execute_transforms,
    parse_transform_file,
    rebuild_materialized_gold,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_sql(base: Path, schema: str, name: str, content: str) -> Path:
    """Write a .sql file under base/transforms/{schema}/{name}.sql."""
    d = base / "transforms" / schema
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{name}.sql"
    p.write_text(content)
    return p


def _make_bronze_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create sample bronze tables for transform tests."""
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    con.execute("""
        CREATE TABLE bronze.employees (
            id INTEGER,
            employee_code VARCHAR,
            first_name VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO bronze.employees VALUES
        (1, 'E001', 'Alice'),
        (2, 'E002', 'Bob'),
        (3, 'E003', 'Charlie')
    """)

    con.execute("""
        CREATE TABLE bronze.items (
            id INTEGER,
            item_code VARCHAR,
            item_name VARCHAR,
            category VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO bronze.items VALUES
        (1, 'I001', 'Widget', 'Hardware'),
        (2, 'I002', 'Gadget', 'Electronics')
    """)

    con.execute("""
        CREATE TABLE bronze.sales (
            id INTEGER,
            employee_code VARCHAR,
            item_code VARCHAR,
            amount DECIMAL(10,2)
        )
    """)
    con.execute("""
        INSERT INTO bronze.sales VALUES
        (1, 'E001', 'I001', 100.00),
        (2, 'E002', 'I002', 250.50),
        (3, 'E001', 'I002', 75.00)
    """)


# ===========================================================================
# Task 1: SQL file parser
# ===========================================================================


class TestParseTransformFile:
    def test_parse_silver_no_metadata(self, tmp_path: Path):
        p = _write_sql(
            tmp_path, "silver", "employees", "SELECT * FROM bronze.employees"
        )
        meta = parse_transform_file(p)

        assert meta.name == "employees"
        assert meta.schema == "silver"
        assert meta.sql == "SELECT * FROM bronze.employees"
        assert meta.depends_on == []
        assert meta.materialized is False
        assert meta.path == p
        assert meta.qualified_name == "silver.employees"

    def test_parse_with_depends_on(self, tmp_path: Path):
        content = (
            "-- depends_on: bronze.employees\n"
            "-- depends_on: bronze.departments\n"
            "SELECT e.*, d.dept_name\n"
            "FROM bronze.employees e\n"
            "JOIN bronze.departments d USING (dept_id)"
        )
        p = _write_sql(tmp_path, "silver", "emp_with_dept", content)
        meta = parse_transform_file(p)

        assert meta.depends_on == ["bronze.employees", "bronze.departments"]
        assert "SELECT e.*" in meta.sql

    def test_parse_materialized_gold(self, tmp_path: Path):
        content = (
            "-- depends_on: silver.employees\n"
            "-- materialized: true\n"
            "SELECT * FROM silver.employees"
        )
        p = _write_sql(tmp_path, "gold", "employee_summary", content)
        meta = parse_transform_file(p)

        assert meta.schema == "gold"
        assert meta.materialized is True
        assert meta.depends_on == ["silver.employees"]
        assert meta.qualified_name == "gold.employee_summary"

    def test_parse_gold_non_materialized(self, tmp_path: Path):
        content = "SELECT count(*) AS total FROM silver.employees"
        p = _write_sql(tmp_path, "gold", "emp_count", content)
        meta = parse_transform_file(p)

        assert meta.schema == "gold"
        assert meta.materialized is False

    def test_parse_preserves_non_metadata_comments(self, tmp_path: Path):
        content = (
            "-- depends_on: bronze.sales\n"
            "-- This is a regular comment\n"
            "SELECT amount FROM bronze.sales"
        )
        p = _write_sql(tmp_path, "silver", "sales_clean", content)
        meta = parse_transform_file(p)

        assert "-- This is a regular comment" in meta.sql
        assert meta.depends_on == ["bronze.sales"]

    def test_parse_strips_leading_trailing_blank_lines(self, tmp_path: Path):
        content = "\n\n-- depends_on: bronze.x\n\nSELECT 1\n\n"
        p = _write_sql(tmp_path, "silver", "stripped", content)
        meta = parse_transform_file(p)

        assert meta.sql == "SELECT 1"

    def test_parse_invalid_schema_directory(self, tmp_path: Path):
        d = tmp_path / "transforms" / "platinum"
        d.mkdir(parents=True)
        p = d / "test.sql"
        p.write_text("SELECT 1")

        with pytest.raises(ValueError, match="platinum"):
            parse_transform_file(p)

    def test_parse_multiple_depends_on_interleaved(self, tmp_path: Path):
        content = (
            "-- depends_on: silver.a\n"
            "-- materialized: true\n"
            "-- depends_on: silver.b\n"
            "SELECT * FROM silver.a JOIN silver.b USING (id)"
        )
        p = _write_sql(tmp_path, "gold", "joined", content)
        meta = parse_transform_file(p)

        assert meta.depends_on == ["silver.a", "silver.b"]
        assert meta.materialized is True


# ===========================================================================
# Task 2: Discovery + dependency graph
# ===========================================================================


class TestDiscoverTransforms:
    def test_discover_finds_silver_and_gold(self, tmp_path: Path):
        _write_sql(tmp_path, "silver", "a", "SELECT 1")
        _write_sql(tmp_path, "silver", "b", "SELECT 2")
        _write_sql(tmp_path, "gold", "c", "SELECT 3")

        transforms = discover_transforms(tmp_path)
        names = [t.qualified_name for t in transforms]

        assert len(transforms) == 3
        assert "silver.a" in names
        assert "silver.b" in names
        assert "gold.c" in names

    def test_discover_returns_silver_before_gold(self, tmp_path: Path):
        _write_sql(tmp_path, "gold", "x", "SELECT 1")
        _write_sql(tmp_path, "silver", "y", "SELECT 2")

        transforms = discover_transforms(tmp_path)
        schemas = [t.schema for t in transforms]

        assert schemas == ["silver", "gold"]

    def test_discover_no_transforms_dir(self, tmp_path: Path):
        result = discover_transforms(tmp_path)
        assert result == []

    def test_discover_empty_transforms_dir(self, tmp_path: Path):
        (tmp_path / "transforms").mkdir()
        result = discover_transforms(tmp_path)
        assert result == []

    def test_discover_ignores_non_sql_files(self, tmp_path: Path):
        _write_sql(tmp_path, "silver", "valid", "SELECT 1")
        (tmp_path / "transforms" / "silver" / "notes.txt").write_text("ignore me")

        transforms = discover_transforms(tmp_path)
        assert len(transforms) == 1
        assert transforms[0].name == "valid"

    def test_discover_sorted_alphabetically(self, tmp_path: Path):
        _write_sql(tmp_path, "silver", "zebra", "SELECT 1")
        _write_sql(tmp_path, "silver", "alpha", "SELECT 2")

        transforms = discover_transforms(tmp_path)
        names = [t.name for t in transforms]
        assert names == ["alpha", "zebra"]


class TestBuildExecutionOrder:
    def test_simple_chain(self):
        silver = TransformMeta(name="a", schema="silver", sql="SELECT 1")
        gold = TransformMeta(
            name="b",
            schema="gold",
            sql="SELECT * FROM silver.a",
            depends_on=["silver.a"],
        )

        ordered = build_execution_order([gold, silver])
        names = [t.qualified_name for t in ordered]

        assert names.index("silver.a") < names.index("gold.b")

    def test_no_dependencies(self):
        a = TransformMeta(name="a", schema="silver", sql="SELECT 1")
        b = TransformMeta(name="b", schema="silver", sql="SELECT 2")

        ordered = build_execution_order([a, b])
        assert len(ordered) == 2

    def test_diamond_dependency(self):
        a = TransformMeta(name="a", schema="silver", sql="SELECT 1")
        b = TransformMeta(name="b", schema="silver", sql="S", depends_on=["silver.a"])
        c = TransformMeta(name="c", schema="silver", sql="S", depends_on=["silver.a"])
        d = TransformMeta(
            name="d",
            schema="gold",
            sql="S",
            depends_on=["silver.b", "silver.c"],
        )

        ordered = build_execution_order([d, c, b, a])
        names = [t.qualified_name for t in ordered]

        assert names.index("silver.a") < names.index("silver.b")
        assert names.index("silver.a") < names.index("silver.c")
        assert names.index("silver.b") < names.index("gold.d")
        assert names.index("silver.c") < names.index("gold.d")

    def test_missing_dependency_raises(self):
        t = TransformMeta(
            name="bad",
            schema="gold",
            sql="SELECT 1",
            depends_on=["silver.nonexistent"],
        )

        with pytest.raises(ValueError, match="nonexistent"):
            build_execution_order([t])

    def test_cycle_raises(self):
        a = TransformMeta(name="a", schema="silver", sql="S", depends_on=["silver.b"])
        b = TransformMeta(name="b", schema="silver", sql="S", depends_on=["silver.a"])

        with pytest.raises(graphlib.CycleError):
            build_execution_order([a, b])


# ===========================================================================
# Task 3: Transform execution engine
# ===========================================================================


class TestExecuteTransforms:
    def test_silver_creates_view(self, tmp_path: Path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        _make_bronze_tables(con)

        t = TransformMeta(
            name="employee_clean",
            schema="silver",
            sql="SELECT id, employee_code, first_name FROM bronze.employees",
        )

        results = execute_transforms(con, [t])
        assert len(results) == 1
        assert results[0].status == "success"
        assert results[0].type == "view"
        assert results[0].schema == "silver"

        rows = con.execute("SELECT * FROM silver.employee_clean").fetchall()
        assert len(rows) == 3
        con.close()

    def test_gold_non_materialized_creates_view(self, tmp_path: Path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        _make_bronze_tables(con)
        con.execute("CREATE VIEW silver.emp AS SELECT * FROM bronze.employees")

        t = TransformMeta(
            name="emp_count",
            schema="gold",
            sql="SELECT count(*) AS total FROM silver.emp",
        )

        results = execute_transforms(con, [t])
        assert results[0].status == "success"
        assert results[0].type == "view"

        row = con.execute("SELECT * FROM gold.emp_count").fetchone()
        assert row[0] == 3
        con.close()

    def test_gold_materialized_creates_table(self, tmp_path: Path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        _make_bronze_tables(con)
        con.execute("CREATE VIEW silver.emp AS SELECT * FROM bronze.employees")

        t = TransformMeta(
            name="emp_snapshot",
            schema="gold",
            sql="SELECT * FROM silver.emp",
            materialized=True,
        )

        results = execute_transforms(con, [t])
        assert results[0].status == "success"
        assert results[0].type == "table"

        # Verify it's a TABLE not a VIEW
        info = con.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema='gold' AND table_name='emp_snapshot'"
        ).fetchone()
        assert info[0] == "BASE TABLE"
        con.close()

    def test_error_continues_to_next(self, tmp_path: Path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")

        bad = TransformMeta(
            name="broken",
            schema="silver",
            sql="SELECT * FROM nonexistent_table",
        )
        good = TransformMeta(
            name="ok",
            schema="silver",
            sql="SELECT 1 AS val",
        )

        results = execute_transforms(con, [bad, good])
        assert results[0].status == "error"
        assert results[0].error is not None
        assert results[1].status == "success"
        con.close()

    def test_template_variable_substitution(self, tmp_path: Path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        _make_bronze_tables(con)

        t = TransformMeta(
            name="filtered",
            schema="silver",
            sql="SELECT * FROM bronze.employees WHERE first_name = '${name}'",
        )

        results = execute_transforms(con, [t], variables={"name": "Alice"})
        assert results[0].status == "success"

        rows = con.execute("SELECT * FROM silver.filtered").fetchall()
        assert len(rows) == 1
        assert rows[0][2] == "Alice"
        con.close()

    def test_safe_substitute_leaves_unknown_vars(self, tmp_path: Path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        _make_bronze_tables(con)

        t = TransformMeta(
            name="with_unknown",
            schema="silver",
            sql="SELECT '${unknown}' AS val, * FROM bronze.employees",
        )

        # safe_substitute doesn't raise on unknown vars — leaves them as-is
        results = execute_transforms(con, [t], variables={})
        assert results[0].status == "success"
        con.close()


class TestRebuildMaterializedGold:
    def test_rebuilds_only_materialized_gold(self, tmp_path: Path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        _make_bronze_tables(con)
        con.execute("CREATE VIEW silver.emp AS SELECT * FROM bronze.employees")

        mat = TransformMeta(
            name="emp_snap",
            schema="gold",
            sql="SELECT * FROM silver.emp",
            materialized=True,
        )
        view = TransformMeta(
            name="emp_view",
            schema="gold",
            sql="SELECT count(*) FROM silver.emp",
            materialized=False,
        )
        silver = TransformMeta(
            name="emp",
            schema="silver",
            sql="SELECT * FROM bronze.employees",
        )

        results = rebuild_materialized_gold(con, [mat, view, silver])
        assert len(results) == 1
        assert results[0].name == "emp_snap"
        assert results[0].type == "table"
        con.close()

    def test_rebuild_reflects_new_data(self, tmp_path: Path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        _make_bronze_tables(con)
        con.execute("CREATE VIEW silver.emp AS SELECT * FROM bronze.employees")

        mat = TransformMeta(
            name="emp_snap",
            schema="gold",
            sql="SELECT * FROM silver.emp",
            materialized=True,
        )

        # Initial creation
        execute_transforms(con, [mat])
        count1 = con.execute("SELECT count(*) FROM gold.emp_snap").fetchone()[0]
        assert count1 == 3

        # Insert new data into bronze
        con.execute("INSERT INTO bronze.employees VALUES (4, 'E004', 'Diana')")

        # Rebuild
        rebuild_materialized_gold(con, [mat])
        count2 = con.execute("SELECT count(*) FROM gold.emp_snap").fetchone()[0]
        assert count2 == 4
        con.close()

    def test_rebuild_returns_empty_when_no_materialized(self, tmp_path: Path):
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        t = TransformMeta(name="v", schema="gold", sql="SELECT 1")
        results = rebuild_materialized_gold(con, [t])
        assert results == []
        con.close()


# ===========================================================================
# Task 6: E2E test with realistic transforms
# ===========================================================================


class TestE2ETransformPipeline:
    """End-to-end: bronze data -> discover -> order -> execute -> verify."""

    def test_full_pipeline_with_fixtures(self, tmp_path: Path):
        # 1. Set up bronze data
        db_path = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db_path))
        _make_bronze_tables(con)

        # 2. Write transform SQL files
        _write_sql(
            tmp_path,
            "silver",
            "employee_master",
            (
                "-- depends_on: bronze.employees\n"
                "SELECT\n"
                "    id AS employee_id,\n"
                "    employee_code,\n"
                "    first_name AS employee_name\n"
                "FROM bronze.employees"
            ),
        )
        _write_sql(
            tmp_path,
            "silver",
            "item_master",
            (
                "-- depends_on: bronze.items\n"
                "SELECT\n"
                "    id AS item_id,\n"
                "    item_code,\n"
                "    item_name,\n"
                "    category\n"
                "FROM bronze.items"
            ),
        )
        _write_sql(
            tmp_path,
            "gold",
            "sales_summary",
            (
                "-- depends_on: silver.employee_master\n"
                "-- depends_on: silver.item_master\n"
                "-- materialized: true\n"
                "SELECT\n"
                "    s.id AS sale_id,\n"
                "    e.employee_name,\n"
                "    i.item_name,\n"
                "    i.category,\n"
                "    s.amount\n"
                "FROM bronze.sales s\n"
                "LEFT JOIN silver.employee_master e ON s.employee_code = e.employee_code\n"
                "LEFT JOIN silver.item_master i ON s.item_code = i.item_code"
            ),
        )

        # 3. Discover and order
        transforms = discover_transforms(tmp_path)
        assert len(transforms) == 3

        ordered = build_execution_order(transforms)
        names = [t.qualified_name for t in ordered]

        # Gold must come after both silver deps
        assert names.index("silver.employee_master") < names.index("gold.sales_summary")
        assert names.index("silver.item_master") < names.index("gold.sales_summary")

        # 4. Execute all transforms
        results = execute_transforms(con, ordered)
        assert all(r.status == "success" for r in results)

        # 5. Verify silver views
        emp_rows = con.execute("SELECT * FROM silver.employee_master").fetchall()
        assert len(emp_rows) == 3
        assert emp_rows[0][2] == "Alice"  # employee_name

        item_rows = con.execute("SELECT * FROM silver.item_master").fetchall()
        assert len(item_rows) == 2

        # 6. Verify gold materialized table
        gold_rows = con.execute(
            "SELECT * FROM gold.sales_summary ORDER BY sale_id"
        ).fetchall()
        assert len(gold_rows) == 3
        # sale_id=1: Alice, Widget, Hardware, 100.00
        assert gold_rows[0][1] == "Alice"
        assert gold_rows[0][2] == "Widget"

        # 7. Verify it's a TABLE (materialized)
        ttype = con.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema='gold' AND table_name='sales_summary'"
        ).fetchone()
        assert ttype[0] == "BASE TABLE"

        # 8. Insert new data into bronze, rebuild gold
        con.execute("INSERT INTO bronze.sales VALUES (4, 'E003', 'I001', 500.00)")
        rebuild_results = rebuild_materialized_gold(con, ordered)
        assert len(rebuild_results) == 1
        assert rebuild_results[0].status == "success"

        gold_after = con.execute("SELECT count(*) FROM gold.sales_summary").fetchone()
        assert gold_after[0] == 4

        con.close()

    def test_bronze_dependencies_silently_skipped(self, tmp_path: Path):
        """Dependencies on bronze.* are external (extraction layer) and should
        be silently ignored by the dependency graph — no error raised."""
        db_path = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db_path))
        _make_bronze_tables(con)

        _write_sql(
            tmp_path,
            "silver",
            "emp",
            ("-- depends_on: bronze.employees\nSELECT * FROM bronze.employees"),
        )

        transforms = discover_transforms(tmp_path)
        ordered = build_execution_order(transforms)

        assert len(ordered) == 1
        assert ordered[0].qualified_name == "silver.emp"

        # Verify it actually executes
        results = execute_transforms(con, ordered)
        assert results[0].status == "success"
        con.close()

    def test_missing_silver_dependency_still_raises(self, tmp_path: Path):
        """Dependencies on silver/gold that don't exist should still error."""
        _write_sql(
            tmp_path, "gold", "bad", ("-- depends_on: silver.nonexistent\nSELECT 1")
        )

        transforms = discover_transforms(tmp_path)
        with pytest.raises(ValueError, match="silver.nonexistent"):
            build_execution_order(transforms)


# ===========================================================================
# Task 4: CLI integration — feather setup
# ===========================================================================


class TestCLISetupTransforms:
    """Test that `feather setup` discovers and executes transforms."""

    def test_setup_creates_transforms(self, tmp_path: Path):
        """feather setup with transform SQL files creates views/tables in DuckDB."""
        import yaml
        from typer.testing import CliRunner

        from feather.cli import app

        # Create a source DB (needed for config validation)
        source_db = tmp_path / "source.duckdb"
        con = duckdb.connect(str(source_db))
        con.execute("CREATE SCHEMA IF NOT EXISTS erp")
        con.execute("CREATE TABLE erp.employees (id INT, name VARCHAR)")
        con.execute("INSERT INTO erp.employees VALUES (1, 'Alice'), (2, 'Bob')")
        con.close()

        # Pre-populate destination with bronze data (normally done by `feather run`)
        dest_db = tmp_path / "feather_data.duckdb"
        con = duckdb.connect(str(dest_db))
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute("CREATE TABLE bronze.employees (id INT, name VARCHAR)")
        con.execute("INSERT INTO bronze.employees VALUES (1, 'Alice'), (2, 'Bob')")
        con.close()

        # Write feather.yaml
        config = {
            "source": {"type": "duckdb", "path": str(source_db)},
            "destination": {"path": str(dest_db)},
            "tables": [
                {
                    "name": "employees",
                    "source_table": "erp.employees",
                    "target_table": "bronze.employees",
                    "strategy": "full",
                }
            ],
        }
        config_file = tmp_path / "feather.yaml"
        config_file.write_text(yaml.dump(config))

        # Write transform SQL files
        _write_sql(
            tmp_path,
            "silver",
            "emp_clean",
            ("SELECT id, name AS employee_name FROM bronze.employees"),
        )

        runner = CliRunner()
        result = runner.invoke(app, ["setup", "--config", str(config_file)])

        assert result.exit_code == 0
        assert "Transforms applied" in result.output
        assert "1 silver view(s)" in result.output

    def test_setup_no_transforms_dir_ok(self, tmp_path: Path):
        """feather setup without transforms/ dir should not fail."""
        import yaml
        from typer.testing import CliRunner

        from feather.cli import app

        source_db = tmp_path / "source.duckdb"
        con = duckdb.connect(str(source_db))
        con.execute("CREATE SCHEMA IF NOT EXISTS erp")
        con.execute("CREATE TABLE erp.employees (id INT, name VARCHAR)")
        con.close()

        config = {
            "source": {"type": "duckdb", "path": str(source_db)},
            "destination": {"path": str(tmp_path / "feather_data.duckdb")},
            "tables": [
                {
                    "name": "employees",
                    "source_table": "erp.employees",
                    "target_table": "bronze.employees",
                    "strategy": "full",
                }
            ],
        }
        config_file = tmp_path / "feather.yaml"
        config_file.write_text(yaml.dump(config))

        runner = CliRunner()
        result = runner.invoke(app, ["setup", "--config", str(config_file)])

        assert result.exit_code == 0
        assert "Transforms applied" not in result.output


# ===========================================================================
# Task 5: Pipeline integration — feather run
# ===========================================================================


class TestPipelineTransformRebuild:
    """Test that run_all() rebuilds materialized gold after extraction."""

    def test_run_rebuilds_materialized_gold(self, tmp_path: Path):
        """After extraction, materialized gold tables are rebuilt."""
        import yaml

        from feather.config import load_config
        from feather.pipeline import run_all

        # Create source with data
        source_db = tmp_path / "source.duckdb"
        con = duckdb.connect(str(source_db))
        con.execute("CREATE SCHEMA IF NOT EXISTS erp")
        con.execute("CREATE TABLE erp.employees (id INT, name VARCHAR)")
        con.execute("INSERT INTO erp.employees VALUES (1, 'Alice'), (2, 'Bob')")
        con.close()

        dest_db = tmp_path / "feather_data.duckdb"
        config = {
            "source": {"type": "duckdb", "path": str(source_db)},
            "destination": {"path": str(dest_db)},
            "tables": [
                {
                    "name": "employees",
                    "source_table": "erp.employees",
                    "target_table": "bronze.employees",
                    "strategy": "full",
                }
            ],
        }
        config_file = tmp_path / "feather.yaml"
        config_file.write_text(yaml.dump(config))

        # Write transforms
        _write_sql(
            tmp_path,
            "silver",
            "emp_clean",
            "SELECT id, name AS employee_name FROM bronze.employees",
        )
        _write_sql(
            tmp_path,
            "gold",
            "emp_snapshot",
            (
                "-- depends_on: silver.emp_clean\n"
                "-- materialized: true\n"
                "SELECT * FROM silver.emp_clean"
            ),
        )

        cfg = load_config(config_file)
        results = run_all(cfg, config_file)

        assert any(r.status == "success" for r in results)

        # Verify gold table was created by the rebuild
        con = duckdb.connect(str(dest_db))
        gold_rows = con.execute("SELECT * FROM gold.emp_snapshot").fetchall()
        assert len(gold_rows) == 2
        con.close()

    def test_run_mode_switch_rematerializes_gold(self, tmp_path: Path):
        """Dev→prod mode switch rematerializes gold VIEW as TABLE even when extraction skipped."""
        import yaml

        from feather.config import load_config
        from feather.pipeline import run_all

        # Create source with data
        source_db = tmp_path / "source.duckdb"
        con = duckdb.connect(str(source_db))
        con.execute("CREATE SCHEMA IF NOT EXISTS erp")
        con.execute("CREATE TABLE erp.employees (id INT, name VARCHAR)")
        con.execute("INSERT INTO erp.employees VALUES (1, 'Alice'), (2, 'Bob')")
        con.close()

        dest_db = tmp_path / "feather_data.duckdb"
        config = {
            "source": {"type": "duckdb", "path": str(source_db)},
            "destination": {"path": str(dest_db)},
            "mode": "dev",
            "tables": [
                {
                    "name": "employees",
                    "source_table": "erp.employees",
                    "target_table": "bronze.employees",
                    "strategy": "full",
                }
            ],
        }
        config_file = tmp_path / "feather.yaml"
        config_file.write_text(yaml.dump(config))

        # Write transforms
        _write_sql(
            tmp_path,
            "silver",
            "emp_clean",
            "SELECT id, name AS employee_name FROM bronze.employees",
        )
        _write_sql(
            tmp_path,
            "gold",
            "emp_snapshot",
            (
                "-- depends_on: silver.emp_clean\n"
                "-- materialized: true\n"
                "SELECT * FROM silver.emp_clean"
            ),
        )

        # Run 1: dev mode — gold should be a VIEW
        cfg_dev = load_config(config_file)
        results1 = run_all(cfg_dev, config_file)
        assert any(r.status == "success" for r in results1)

        con = duckdb.connect(str(dest_db))
        gold_type_dev = con.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema='gold' AND table_name='emp_snapshot'"
        ).fetchone()[0]
        con.close()
        assert gold_type_dev == "VIEW"

        # Run 2: switch to prod mode — extraction skipped (unchanged), but gold should become TABLE
        config["mode"] = "prod"
        config_file.write_text(yaml.dump(config))
        cfg_prod = load_config(config_file)
        results2 = run_all(cfg_prod, config_file)
        assert all(r.status == "skipped" for r in results2)

        con = duckdb.connect(str(dest_db))
        gold_type_prod = con.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema='gold' AND table_name='emp_snapshot'"
        ).fetchone()[0]
        con.close()
        assert gold_type_prod == "BASE TABLE", (
            f"Expected gold to be materialized as BASE TABLE after prod mode switch, "
            f"got {gold_type_prod}"
        )

    def test_run_no_rebuild_when_all_skipped(self, tmp_path: Path):
        """If all tables are skipped, gold table still exists from prior run."""
        import yaml

        from feather.config import load_config
        from feather.pipeline import run_all

        source_db = tmp_path / "source.duckdb"
        con = duckdb.connect(str(source_db))
        con.execute("CREATE SCHEMA IF NOT EXISTS erp")
        con.execute("CREATE TABLE erp.employees (id INT, name VARCHAR)")
        con.execute("INSERT INTO erp.employees VALUES (1, 'Alice')")
        con.close()

        dest_db = tmp_path / "feather_data.duckdb"
        config = {
            "source": {"type": "duckdb", "path": str(source_db)},
            "destination": {"path": str(dest_db)},
            "tables": [
                {
                    "name": "employees",
                    "source_table": "erp.employees",
                    "target_table": "bronze.employees",
                    "strategy": "full",
                }
            ],
        }
        config_file = tmp_path / "feather.yaml"
        config_file.write_text(yaml.dump(config))

        _write_sql(
            tmp_path, "gold", "emp_snap", ("-- materialized: true\nSELECT 1 AS val")
        )

        cfg = load_config(config_file)

        # First run: extracts data (success)
        results1 = run_all(cfg, config_file)
        assert any(r.status == "success" for r in results1)

        # Second run: should skip (source unchanged)
        results2 = run_all(cfg, config_file)
        assert all(r.status == "skipped" for r in results2)

        # Gold table should still exist from the first run's rebuild
        con = duckdb.connect(str(dest_db))
        row = con.execute("SELECT * FROM gold.emp_snap").fetchone()
        assert row is not None
        con.close()
