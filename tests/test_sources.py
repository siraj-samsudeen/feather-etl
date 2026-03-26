"""Tests for feather.sources module."""

from pathlib import Path

import pytest

from tests.conftest import FIXTURES_DIR


@pytest.fixture
def client_db(tmp_path: Path) -> Path:
    """Local copy of client.duckdb for source tests."""
    import shutil

    src = FIXTURES_DIR / "client.duckdb"
    dst = tmp_path / "client.duckdb"
    shutil.copy2(src, dst)
    return dst


class TestSourceProtocol:
    def test_stream_schema_fields(self):
        from feather.sources import StreamSchema

        s = StreamSchema(
            name="test",
            columns=[("id", "BIGINT"), ("name", "VARCHAR")],
            primary_key=["id"],
            supports_incremental=True,
        )
        assert s.name == "test"
        assert len(s.columns) == 2

    def test_change_result_fields(self):
        from feather.sources import ChangeResult

        r = ChangeResult(changed=True, reason="first_run", metadata={})
        assert r.changed is True
        assert r.reason == "first_run"


class TestDuckDBFileSource:
    def test_check_valid_file(self, client_db: Path):
        from feather.sources.duckdb_file import DuckDBFileSource

        source = DuckDBFileSource(path=client_db)
        assert source.check() is True

    def test_check_nonexistent_file(self, tmp_path: Path):
        from feather.sources.duckdb_file import DuckDBFileSource

        source = DuckDBFileSource(path=tmp_path / "nope.duckdb")
        assert source.check() is False

    def test_discover_lists_tables(self, client_db: Path):
        from feather.sources.duckdb_file import DuckDBFileSource

        source = DuckDBFileSource(path=client_db)
        schemas = source.discover()
        names = {s.name for s in schemas}
        assert "icube.SALESINVOICE" in names
        assert "icube.CUSTOMERMASTER" in names
        assert "icube.InventoryGroup" in names
        assert len(schemas) == 6

    def test_discover_has_columns(self, client_db: Path):
        from feather.sources.duckdb_file import DuckDBFileSource

        source = DuckDBFileSource(path=client_db)
        schemas = source.discover()
        si = next(s for s in schemas if s.name == "icube.SALESINVOICE")
        col_names = [c[0] for c in si.columns]
        assert "ID" in col_names
        assert "SI_NO" in col_names

    def test_extract_returns_arrow(self, client_db: Path):
        import pyarrow as pa

        from feather.sources.duckdb_file import DuckDBFileSource

        source = DuckDBFileSource(path=client_db)
        table = source.extract("icube.InventoryGroup")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 66

    def test_extract_salesinvoice_row_count(self, client_db: Path):
        from feather.sources.duckdb_file import DuckDBFileSource

        source = DuckDBFileSource(path=client_db)
        table = source.extract("icube.SALESINVOICE")
        assert table.num_rows == 11676

    def test_get_schema(self, client_db: Path):
        from feather.sources.duckdb_file import DuckDBFileSource

        source = DuckDBFileSource(path=client_db)
        schema = source.get_schema("icube.InventoryGroup")
        col_names = [c[0] for c in schema]
        assert len(col_names) > 0

    def test_detect_changes_always_true(self, client_db: Path):
        from feather.sources.duckdb_file import DuckDBFileSource

        source = DuckDBFileSource(path=client_db)
        result = source.detect_changes("icube.SALESINVOICE")
        assert result.changed is True
        assert result.reason == "first_run"


class TestSourceRegistry:
    def test_registry_resolves_duckdb(self):
        from feather.sources.duckdb_file import DuckDBFileSource
        from feather.sources.registry import SOURCE_REGISTRY

        assert SOURCE_REGISTRY["duckdb"] is DuckDBFileSource

    def test_create_source(self, client_db: Path):
        from feather.config import SourceConfig
        from feather.sources.registry import create_source

        cfg = SourceConfig(type="duckdb", path=client_db)
        source = create_source(cfg)
        assert source.check() is True
