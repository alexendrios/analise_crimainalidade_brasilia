@echo off
REM Roda testes com coverage (executado separadamente para nao atrapalhar o pytest rapido)
echo === Rodando testes com coverage ===
venv\Scripts\python.exe -m pytest -x -q ^
    --cov=analysis --cov=api --cov=config --cov=dashboard ^
    --cov=database --cov=domain --cov=geoespacial --cov=ingestion ^
    --cov=processing --cov=src --cov=util --cov=validation ^
    --cov-report=term-missing ^
    --cov-report=html:test_report/coverage ^
    --cov-report=xml:test_report/coverage.xml ^
    --cov-fail-under=95
