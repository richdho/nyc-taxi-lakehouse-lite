# Ingestion (Phase 1+)

**dlt** pipelines download NYC TLC Parquet and land **bronze** Iceberg tables via **pyiceberg**.

Use `LakehouseConfig.from_profile()` for paths. Open the Iceberg catalog with
`lakehouse.iceberg_catalog.load_catalog_from_config()` or `prepare_bronze_catalog()`.
