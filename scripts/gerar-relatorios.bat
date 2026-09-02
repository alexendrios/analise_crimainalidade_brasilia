@echo off
setlocal EnableExtensions

title Criminalidade Brasilia - DF - Relatorios (Testes + Cobertura)

cd /d "%~dp0.."

echo.
echo ======================================================================
echo              CRIMINALIDADE BRASILIA - DF
echo              GERACAO DE RELATORIOS - TESTES E COBERTURA
echo ======================================================================
echo.
echo Diretorio do projeto:
echo %CD%
echo.

if not exist "venv\Scripts\python.exe" goto :erro_venv

echo [1/3] Ambiente virtual encontrado.
echo.

if not exist "test_report" mkdir "test_report"
if not exist "logs" mkdir "logs"

echo [2/3] Executando pytest --cov (gate --cov-fail-under=95)...
echo.

venv\Scripts\python.exe -m pytest --cov=analysis --cov=api --cov=config --cov=dashboard --cov=database --cov=domain --cov=geoespacial --cov=ingestion --cov=processing --cov=src --cov=util --cov=validation --cov-report=term-missing --cov-report=html:test_report/coverage --cov-report=xml:test_report/coverage.xml --cov-fail-under=95 > logs\testes-cov.log 2>&1
set PYTEST_EXIT=%ERRORLEVEL%
type logs\testes-cov.log

if not "%PYTEST_EXIT%"=="0" goto :erro_pytest

echo.
echo [OK] Testes passaram e coverage acima da meta.
echo.

echo [3/3] Gerando relatorio executivo (cobertura-executiva.html)...
echo.

if not exist "scripts\gerar_relatorio_cobertura.py" goto :erro_script

call "venv\Scripts\python.exe" "scripts\gerar_relatorio_cobertura.py"
set GEN_EXIT=%ERRORLEVEL%

if not "%GEN_EXIT%"=="0" goto :erro_gen

echo.
echo ======================================================================
echo                  RELATORIOS GERADOS COM SUCESSO
echo ======================================================================
echo.
echo Relatorios:
echo   test_report\relatorio-testes.html
echo   test_report\cobertura-executiva.html
echo   test_report\coverage\index.html
echo   test_report\coverage.xml
echo   test_report\junit.xml
echo   logs\testes-cov.log
echo.

if exist "test_report\relatorio-testes.html" start "" "test_report\relatorio-testes.html"
if exist "test_report\cobertura-executiva.html" start "" "test_report\cobertura-executiva.html"

echo Processo finalizado.
echo.

goto :fim

:erro_venv
echo [ERRO] Ambiente virtual nao encontrado.
goto :fim_erro

:erro_pytest
echo.
echo [ERRO] Testes ou coverage abaixo da meta (exit code: %PYTEST_EXIT%).
echo Log: logs\testes-cov.log
goto :fim_erro

:erro_script
echo [ERRO] scripts\gerar_relatorio_cobertura.py nao encontrado.
goto :fim_erro

:erro_gen
echo.
echo [ERRO] Falha ao gerar relatorio executivo (exit code: %GEN_EXIT%).
goto :fim_erro

:fim_erro
pause
exit /b 1

:fim
endlocal
exit /b 0
