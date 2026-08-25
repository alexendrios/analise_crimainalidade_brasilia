@echo off
setlocal EnableExtensions

title Criminalidade Brasilia - DF - Testes e Cobertura

cd /d "%~dp0.."

echo.
echo ======================================================================
echo              CRIMINALIDADE BRASILIA - DF
echo              TESTES E COBERTURA DE CODIGO
echo ======================================================================
echo.
echo Diretorio do projeto:
echo %CD%
echo.

REM ============================================================
REM VERIFICAR AMBIENTE VIRTUAL
REM ============================================================

if not exist "venv\Scripts\python.exe" (
    echo [ERRO] Ambiente virtual nao encontrado.
    pause
    exit /b 1
)

echo [1/4] Ambiente virtual encontrado.
echo.

REM ============================================================
REM CRIAR DIRETORIOS
REM ============================================================

if not exist "test_report" mkdir "test_report"
if not exist "logs" mkdir "logs"

echo [2/4] Diretorios preparados.
echo.

REM ============================================================
REM FASE 1: PYTEST RAPIDO (xdist, sem coverage)
REM ============================================================

echo [3/4] Fase 1 - pytest rapido com xdist...
echo.

powershell -NoProfile -Command "& 'venv\Scripts\python.exe' -m pytest -q --no-header --tb=line 2>&1 | Tee-Object -FilePath 'logs\testes.log'"

set TEST_EXIT=%ERRORLEVEL%

echo.
echo ----------------------------------------------------------------------

if not "%TEST_EXIT%"=="0" (
    echo [ERRO] Suite de testes falhou (exit %TEST_EXIT%).
    echo Log: logs\testes.log
    pause
    exit /b %TEST_EXIT%
)

echo [OK] Todos os testes passaram.
echo.

REM ============================================================
REM FASE 2: TESTAR-COM-COVERAGE (pytest-cov + relatorios)
REM ============================================================

echo [4/4] Fase 2 - testes com coverage + relatorios...
echo.

if not exist "scripts\testar-com-coverage.bat" (
    echo [ERRO] scripts\testar-com-coverage.bat nao encontrado.
    pause
    exit /b 1
)

call "scripts\testar-com-coverage.bat"

set COV_EXIT=%ERRORLEVEL%

echo.

if not "%COV_EXIT%"=="0" (
    echo [ERRO] Coverage abaixo da meta ou falha nos testes.
    pause
    exit /b %COV_EXIT%
)

REM ============================================================
REM RELATORIO EXECUTIVO
REM ============================================================

if exist "scripts\gerar_relatorio_cobertura.py" (
    call "venv\Scripts\python.exe" "scripts\gerar_relatorio_cobertura.py"
)

echo.
echo ======================================================================
echo                  EXECUCAO CONCLUIDA COM SUCESSO
echo ======================================================================
echo.
echo Relatorios:
echo   test_report\relatorio-testes.html
echo   test_report\cobertura-executiva.html
echo   test_report\coverage\index.html
echo   test_report\coverage.xml
echo   logs\testes.log
echo.

if exist "test_report\relatorio-testes.html" start "" "test_report\relatorio-testes.html"
if exist "test_report\cobertura-executiva.html" start "" "test_report\cobertura-executiva.html"

echo Processo finalizado.
echo.

endlocal
exit /b 0
