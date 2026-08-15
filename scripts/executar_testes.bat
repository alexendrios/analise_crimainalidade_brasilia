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
    echo [ERRO] Ambiente virtual nao encontrado:
    echo.
    echo venv\Scripts\python.exe
    echo.
    echo Crie o ambiente virtual antes de executar os testes.
    echo.
    pause
    exit /b 1
)

echo [1/5] Ambiente virtual encontrado.
echo.

REM ============================================================
REM CRIAR DIRETORIO DE RELATORIOS
REM ============================================================

if not exist "tests_report" (
    mkdir "tests_report"
)

echo [2/5] Diretorio de relatorios preparado.
echo.

REM ============================================================
REM EXECUTAR TESTES
REM ============================================================

echo [3/5] Executando testes automatizados...
echo.

if not exist "logs" (
    mkdir "logs"
)

echo Salvando saida completa do pytest em logs\testes.log
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\executar_testes.ps1"

set TEST_EXIT_CODE=%ERRORLEVEL%

echo.
echo ======================================================================
echo Resultado da execucao dos testes: %TEST_EXIT_CODE%
echo ======================================================================
echo.

if not "%TEST_EXIT_CODE%"=="0" (
    echo [ERRO] A suite de testes apresentou falhas.
    echo.
    echo O relatorio de testes pode ser consultado em:
    echo tests_report\relatorio-testes.html
    echo.
    echo O relatorio de cobertura pode estar incompleto.
    echo.
    pause
    exit /b %TEST_EXIT_CODE%
)

echo [OK] Todos os testes foram executados com sucesso.
echo.

REM ============================================================
REM GERAR RELATORIO EXECUTIVO DE COBERTURA
REM ============================================================

echo [4/5] Gerando relatorio executivo de cobertura...
echo.

if not exist "scripts\gerar_relatorio_cobertura.py" (
    echo [ERRO] Arquivo nao encontrado:
    echo scripts\gerar_relatorio_cobertura.py
    echo.
    pause
    exit /b 1
)

call "venv\Scripts\python.exe" "scripts\gerar_relatorio_cobertura.py"

set COVERAGE_EXIT_CODE=%ERRORLEVEL%

echo.

if not "%COVERAGE_EXIT_CODE%"=="0" (
    echo [ERRO] Falha ao gerar o relatorio executivo de cobertura.
    echo.
    pause
    exit /b %COVERAGE_EXIT_CODE%
)

echo [OK] Relatorio executivo gerado.
echo.

REM ============================================================
REM RESUMO FINAL
REM ============================================================

echo [5/5] Relatorios disponiveis:
echo.

echo ----------------------------------------------------------------------
echo RELATORIO DE TESTES
echo ----------------------------------------------------------------------
echo test_report\relatorio-testes.html
echo.

echo ----------------------------------------------------------------------
echo RELATORIO EXECUTIVO DE COBERTURA
echo ----------------------------------------------------------------------
echo test_report\cobertura-executiva.html
echo.

echo ----------------------------------------------------------------------
echo RELATORIO TECNICO DO COVERAGE.PY
echo ----------------------------------------------------------------------
echo test_report\coverage\index.html
echo.

echo ----------------------------------------------------------------------
echo RELATORIO JUNIT
echo ----------------------------------------------------------------------
echo test_report\junit.xml
echo.

echo ----------------------------------------------------------------------
echo COBERTURA XML
echo ----------------------------------------------------------------------
echo test_report\coverage.xml
echo.

echo ======================================================================
echo                  EXECUCAO CONCLUIDA COM SUCESSO
echo ======================================================================
echo.

REM ============================================================
REM ABRIR RELATORIOS NO NAVEGADOR
REM ============================================================

if exist "test_report\relatorio-testes.html" (
    start "" "test_report\relatorio-testes.html"
)

if exist "test_report\cobertura-executiva.html" (
    start "" "test_report\cobertura-executiva.html"
)

:END

echo.
echo Processo finalizado.
echo.

endlocal
exit /b 0