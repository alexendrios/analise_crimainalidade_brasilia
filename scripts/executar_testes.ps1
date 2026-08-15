# Executa a suite pytest e salva a saida completa em logs/testes.log
$ErrorActionPreference = "Stop"

$projeto = Split-Path -Parent $PSScriptRoot
Set-Location $projeto

if (-not (Test-Path "venv\Scripts\python.exe")) {
    Write-Host "[ERRO] Ambiente virtual nao encontrado: venv\Scripts\python.exe" -ForegroundColor Red
    exit 1
}

New-Item -ItemType Directory -Force -Path "logs" | Out-Null

$log = Join-Path $projeto "logs\testes.log"

Write-Host "Executando pytest... saida completa salva em: $log"
Write-Host ""

& .\venv\Scripts\python.exe -m pytest 2>&1 | Tee-Object -FilePath $log

$exit = $LASTEXITCODE

Write-Host ""
Write-Host "======================================================================"
Write-Host "Resultado da execucao dos testes: $exit"
Write-Host "Log completo: $log"
Write-Host "======================================================================"

exit $exit
