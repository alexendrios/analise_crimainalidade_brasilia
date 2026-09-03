#!/usr/bin/env bash
set -e

# 2. Ambiente virtual e dependências
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 4. Pipeline completo (coleta + gold + modelagem)
python -m src.main
