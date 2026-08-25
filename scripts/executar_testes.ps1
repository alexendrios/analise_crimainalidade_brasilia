$ErrorActionPreference = "Stop"

$projeto = Split-Path -Parent $PSScriptRoot
Set-Location $projeto

if (-not (Test-Path "venv\Scripts\python.exe")) {
    Write-Host "[ERRO] Ambiente virtual nao encontrado: venv\Scripts\python.exe" -ForegroundColor Red
    exit 1
}

New-Item -ItemType Directory -Force -Path "logs" | Out-Null
New-Item -ItemType Directory -Force -Path "test_report" | Out-Null

$log = Join-Path $projeto "logs\testes.log"

Write-Host "Executando pytest com coverage... saida salva em: $log"
Write-Host ""

& .\venv\Scripts\python.exe -m pytest -q --no-header `
    --override-ini="addopts=" `
    --cov=analysis --cov=api --cov=config --cov=dashboard `
    --cov=database --cov=domain --cov=geoespacial --cov=ingestion `
    --cov=processing --cov=src --cov=util --cov=validation `
    --cov-report=term-missing `
    --cov-report=html:test_report/coverage `
    --cov-report=xml:test_report/coverage.xml `
    --cov-fail-under=95 2>&1 | Tee-Object -FilePath $log

$exit = $LASTEXITCODE

Write-Host ""
Write-Host "======================================================================"
Write-Host "Resultado da execucao dos testes: $exit"
Write-Host "Log completo: $log"
Write-Host "======================================================================"

exit $exit
