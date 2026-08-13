# api/main.py
"""
Camada de Consumo (API) do projeto Criminalidade Brasília - DF.

Expõe via HTTP/REST os dados das tabelas gold e as previsões do modelo
híbrido Prophet + XGBoost já existentes no projeto, sem alterar o
pipeline de coleta/tratamento/modelagem atual.

Execução local:
    uvicorn api.main:app --reload --port 8000

Documentação interativa: http://localhost:8000/docs
"""

from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import gold, previsao
from api.schemas import HealthResponse
from database.repository.repository import listar_tabelas
from util.log import logs

logger = logs()

app = FastAPI(
    title="API - Criminalidade Brasília/DF",
    description=(
        "API de consumo dos dados de criminalidade do Distrito Federal "
        "(camada Gold do lakehouse) e das previsões do modelo híbrido "
        "Prophet + XGBoost."
    ),
    version="1.0.0",
)

# CORS liberado por padrão para permitir consumo de um dashboard/frontend
# separado durante o desenvolvimento local. Restrinja `allow_origins` em
# produção.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(gold.router)
app.include_router(previsao.router)


@app.get("/", include_in_schema=False)
def raiz():
    return {"mensagem": "API Criminalidade Brasília/DF — veja /docs para a documentação interativa."}


@app.get("/health", response_model=HealthResponse, tags=["Infra"], summary="Verifica saúde da API e do banco")
def health():
    try:
        listar_tabelas()
        status_banco = "ok"
    except Exception as exc:
        logger.warning("⚠️ Health check: banco indisponível (%s)", exc)
        status_banco = f"erro: {exc}"

    return {
        "status": "ok",
        "database": status_banco,
        "timestamp": datetime.now(),
    }
