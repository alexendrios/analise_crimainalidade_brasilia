@echo off
setlocal EnableExtensions

cd /d "%~dp0.."

echo.
echo ======================================================================
echo              TESTES COM COBERTURA (pytest-cov)
echo ======================================================================
echo.

if not exist "venv\Scripts\python.exe" (
    echo [ERRO] Ambiente virtual nao encontrado.
    pause
    exit /b 1
)

if not exist "test_report" (
    mkdir "test_report"
)

echo Executando pytest com coverage (sem xdist para dados precisos)...
echo.

venv\Scripts\python.exe -m pytest -x -q --no-header ^
    --override-ini="addopts=" ^
    --cov=analysis --cov=api --cov=config --cov=dashboard ^
    --cov=database --cov=domain --cov=geoespacial --cov=ingestion ^
    --cov=processing --cov=src --cov=util --cov=validation ^
    --cov-report=term-missing ^
    --cov-report=html:test_report/coverage ^
    --cov-report=xml:test_report/coverage.xml ^
    --cov-fail-under=95

set COV_EXIT=%ERRORLEVEL%

echo.

if not "%COV_EXIT%"=="0" (
    echo [ERRO] Coverage abaixo de 95%% ou falha nos testes.
    pause
    exit /b %COV_EXIT%
)

echo [OK] Coverage >= 95%%. Gerando relatorio executivo...
echo.

if exist "scripts\gerar_relatorio_cobertura.py" (
    call "venv\Scripts\python.exe" "scripts\gerar_relatorio_cobertura.py"
)

echo.
echo Relatorios disponiveis:
echo   test_report\relatorio-testes.html
echo   test_report\cobertura-executiva.html
echo   test_report\coverage\index.html
echo   test_report\coverage.xml
echo.

endlocal
exit /b 0
