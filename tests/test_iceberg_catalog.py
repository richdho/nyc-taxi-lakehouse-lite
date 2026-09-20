from lakehouse.config import LakehouseConfig
from lakehouse.iceberg_catalog import (
    BRONZE_NAMESPACE,
    SILVER_NAMESPACE,
    ensure_namespace,
    load_catalog_from_config,
    prepare_bronze_catalog,
    prepare_silver_catalog,
)


def test_load_catalog_creates_sqlite_file(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    cfg = LakehouseConfig.from_profile("local")

    catalog = load_catalog_from_config(cfg)

    assert cfg.catalog_db.is_file()
    assert catalog is not None


def test_prepare_bronze_catalog_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    cfg = LakehouseConfig.from_profile("local")

    prepare_bronze_catalog(cfg)
    prepare_bronze_catalog(cfg)

    catalog = load_catalog_from_config(cfg)
    namespaces = catalog.list_namespaces()
    assert (BRONZE_NAMESPACE,) in namespaces


def test_prepare_silver_catalog_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    cfg = LakehouseConfig.from_profile("local")

    prepare_silver_catalog(cfg)
    prepare_silver_catalog(cfg)

    catalog = load_catalog_from_config(cfg)
    assert (SILVER_NAMESPACE,) in catalog.list_namespaces()


def test_ensure_namespace_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    cfg = LakehouseConfig.from_profile("local")
    catalog = load_catalog_from_config(cfg)

    ensure_namespace(catalog, "staging")
    ensure_namespace(catalog, "staging")

    assert ("staging",) in catalog.list_namespaces()
