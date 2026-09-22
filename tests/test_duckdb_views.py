import pytest

from lakehouse.config import LakehouseConfig
from lakehouse.duckdb_views import register_iceberg_view, register_silver_yellow_duckdb


def test_register_iceberg_view_executes_create_view(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    cfg = LakehouseConfig.from_profile("local")
    meta = tmp_path / "table" / "metadata" / "00001-abc.metadata.json"
    meta.parent.mkdir(parents=True)
    meta.write_text("{}", encoding="utf-8")

    executed: list[str] = []

    class FakeConnection:
        def execute(self, sql: str) -> None:
            executed.append(sql)

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "lakehouse.duckdb_views.duckdb.connect",
        lambda _path: FakeConnection(),
    )

    register_iceberg_view(cfg, view_name="silver_yellow_taxi_trips", metadata_path=meta)

    assert any("INSTALL iceberg" in sql for sql in executed)
    assert any("CREATE OR REPLACE VIEW silver_yellow_taxi_trips" in sql for sql in executed)
    assert meta.as_posix() in executed[-1]


def test_register_silver_yellow_duckdb_requires_table(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    cfg = LakehouseConfig.from_profile("local")

    with pytest.raises(FileNotFoundError, match="does not exist"):
        register_silver_yellow_duckdb(cfg)
