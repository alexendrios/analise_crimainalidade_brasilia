#!/usr/bin/env bash
set -e
source venv/bin/activate

case "$1" in
  pipeline)
    # 4. Pipeline completo (coleta + gold + modelagem)
    python -m src.main
    ;;
  api)
    # 7. API (documentação em http://localhost:8000/docs)
    uvicorn api.main:app --reload --port 8000
    ;;
  dashboard)
    # 8. Dashboard
    streamlit run dashboard/app.py
    ;;
  *)
    echo "Uso: ./run.sh [pipeline|api|dashboard]"
    exit 1
    ;;
esac
