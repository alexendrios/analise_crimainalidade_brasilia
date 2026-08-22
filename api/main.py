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

import asyncio
import sys
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import analise, classificacao, gold, previsao
from api.schemas import HealthResponse
from database.repository.repository import listar_tabelas
from util.log import logs

logger = logs()

# Evita ConnectionResetError (WinError 10054) do _ProactorBasePipeTransport ao
# usar o event loop Proactor padrão do Windows. O Selector loop não sofre desse
# problema quando o cliente derruba a conexão abruptamente.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI(
    title="API - Criminalidade Brasília/DF",
    description=(
        "API de consumo dos dados de criminalidade do Distrito Federal "
        "(camada Gold do lakehouse), das previsões do modelo híbrido "
        "Prophet + XGBoost, da classificação de criminalidade letal por "
        "Regressão Logística e das análises executivas (correlações "
        "multivariadas, causalidade de Granger, anomalias por Isolation "
        "Forest e zonas quentes na malha geoespacial)."
    ),
    version="1.1.0",
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
app.include_router(classificacao.router)
app.include_router(analise.router)


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
