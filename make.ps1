param(
    [Parameter(Position = 0)]
    [ValidateSet("help", "env", "sync", "test", "lint", "fmt", "dagster", "bootstrap")]
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

switch ($Target) {
    "help" {
        Write-Host "Usage: .\make.ps1 sync|test|lint|fmt|dagster|bootstrap|env"
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
        if (-not $env:DAGSTER_HOME) { $env:DAGSTER_HOME = (Resolve-Path ".\.dagster_home").Path }
        New-Item -ItemType Directory -Force -Path $env:DAGSTER_HOME | Out-Null
        uv run dagster dev -m orchestration.definitions
    }
    "bootstrap" {
        Ensure-Env
        uv run python -c "from lakehouse.config import LakehouseConfig; c=LakehouseConfig.from_profile(); c.ensure_directories(); print('lake ready:', c.warehouse_root)"
    }
}
