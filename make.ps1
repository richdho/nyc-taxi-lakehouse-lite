param(
    [Parameter(Position = 0)]
    [ValidateSet("help", "env", "sync", "test", "lint", "fmt", "dagster", "bootstrap", "ingest-yellow")]
    [string]$Target = "help"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Ensure-Env {
    if (-not (Test-Path ".env")) {
        Copy-Item ".env.example" ".env"
        Write-Host "Created .env from .env.example"
    }
}

function Set-DagsterHomeAbsolute {
    $candidate = $env:DAGSTER_HOME
    if ([string]::IsNullOrWhiteSpace($candidate)) {
        $candidate = Join-Path $PSScriptRoot ".dagster_home"
    } elseif (-not [System.IO.Path]::IsPathRooted($candidate)) {
        $candidate = Join-Path $PSScriptRoot $candidate
    }
    $resolved = [System.IO.Path]::GetFullPath($candidate)
    New-Item -ItemType Directory -Force -Path $resolved | Out-Null
    $env:DAGSTER_HOME = $resolved
}

switch ($Target) {
    "help" {
        Write-Host "Usage: .\make.ps1 sync|test|lint|fmt|dagster|bootstrap|ingest-yellow|env"
    }
    "env" { Ensure-Env }
    "sync" {
        Ensure-Env
        uv sync --extra dev
    }
    "test" { uv run pytest }
    "lint" { uv run ruff check . }
    "fmt" { uv run ruff format . }
    "dagster" {
        Ensure-Env
        Set-DagsterHomeAbsolute
        uv run dagster dev -m orchestration.definitions
    }
    "bootstrap" {
        Ensure-Env
        uv run python -c "from lakehouse.config import LakehouseConfig; from lakehouse.iceberg_catalog import prepare_bronze_catalog; c=LakehouseConfig.from_profile(); prepare_bronze_catalog(c); print('lake ready:', c.warehouse_root)"
    }
    "ingest-yellow" {
        Ensure-Env
        uv run python -c "from lakehouse.config import LakehouseConfig; from ingestion.bronze_yellow import run_bronze_yellow_ingest; c=LakehouseConfig.from_profile(); print(run_bronze_yellow_ingest(c))"
    }
}
