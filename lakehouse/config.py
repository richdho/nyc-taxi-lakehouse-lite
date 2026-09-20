from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROFILES_DIR = PROJECT_ROOT / "config" / "profiles"
_ENV_LOADED = False


def load_project_env() -> None:
    """Load ``PROJECT_ROOT/.env`` into ``os.environ`` (existing vars win)."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    env_path = PROJECT_ROOT / ".env"
    if env_path.is_file():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            if not key or key in os.environ:
                continue
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            os.environ[key] = value
    _ENV_LOADED = True


@dataclass(frozen=True)
class LakehouseConfig:
    profile: str
    warehouse_root: Path
    catalog_db: Path
    bronze_path: Path
    silver_path: Path
    gold_path: Path
    duckdb_path: Path
    dagster_module: str

    @classmethod
    def from_profile(cls, profile: str | None = None) -> LakehouseConfig:
        load_project_env()
        name = profile or os.getenv("LAKEHOUSE_PROFILE", "local")
        path = PROFILES_DIR / f"{name}.yaml"
        if not path.is_file():
            raise FileNotFoundError(f"Profile not found: {path}")

        raw: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
        storage = raw["storage"]
        duckdb = raw["duckdb"]
        dagster = raw["dagster"]

        root = _resolve_path(os.getenv("LAKEHOUSE_ROOT", storage.get("root", "./lake")))

        def layer(key: str, default: str) -> Path:
            rel = storage["layers"].get(key, default)
            return _resolve_path(rel, base=root if not Path(rel).is_absolute() else None)

        catalog_rel = storage.get("catalog_db", "catalog/catalog.db")
        catalog = _resolve_path(catalog_rel, base=root)

        return cls(
            profile=name,
            warehouse_root=_resolve_path(storage.get("warehouse_root", "warehouse"), base=root),
            catalog_db=catalog,
            bronze_path=layer("bronze", "bronze"),
            silver_path=layer("silver", "silver"),
            gold_path=layer("gold", "gold"),
            duckdb_path=_resolve_path(duckdb["path"], base=root),
            dagster_module=dagster["module"],
        )

    def ensure_directories(self) -> None:
        for path in (
            self.warehouse_root,
            self.catalog_db.parent,
            self.bronze_path,
            self.silver_path,
            self.gold_path,
            self.duckdb_path.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)


def _resolve_path(value: str, base: Path | None = None) -> Path:
    path = Path(value)
    if not path.is_absolute() and base is not None:
        path = base / path
    elif not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path.resolve()
