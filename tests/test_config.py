from lakehouse.config import LakehouseConfig


def test_local_profile_loads():
    cfg = LakehouseConfig.from_profile("local")
    assert cfg.profile == "local"
    assert cfg.dagster_module == "orchestration.definitions"
    assert cfg.bronze_path.name == "bronze"
    assert cfg.catalog_db.name == "catalog.db"


def test_ensure_directories(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    cfg = LakehouseConfig.from_profile("local")
    cfg.ensure_directories()
    assert cfg.duckdb_path.parent.is_dir()
    assert cfg.catalog_db.parent.is_dir()
