# api/routers/analise.py
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query

from api.schemas import (
    AnomaliasResponse,
    CorrelacoesResponse,
    GrangerResponse,
    ZonasQuentesResponse,
)
from api.services import analise_service

router = APIRouter(prefix="/analise", tags=["Análises Executivas"])


@router.get(
    "/correlacoes",
    response_model=CorrelacoesResponse,
    summary="Matriz de correlação multivariada entre indicadores gold + pares destaque e insights",
)
def correlacoes(
    metodo: Literal["pearson", "spearman"] = Query("pearson", description="Coeficiente de correlação"),
    top_n: int = Query(5, ge=1, le=30, description="Quantidade de pares destaque"),
):
    try:
        return analise_service.obter_correlacoes(metodo=metodo, top_n=top_n)
    except analise_service.DadosIndisponiveisError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get(
    "/granger",
    response_model=GrangerResponse,
    summary="Causalidade de Granger pairwise entre os indicadores anuais (leitura exploratória)",
)
def granger(
    max_lag: int = Query(1, ge=1, le=3, description="Defasagem máxima testada"),
    apenas_significantes: bool = Query(True, description="Retornar somente pares com p-valor < alpha"),
    limite: int = Query(50, ge=1, le=200, description="Quantidade máxima de pares retornados"),
):
    try:
        return analise_service.obter_granger(
            max_lag=max_lag,
            apenas_significantes=apenas_significantes,
            limite=limite,
        )
    except analise_service.DadosIndisponiveisError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get(
    "/anomalias",
    response_model=AnomaliasResponse,
    summary="Anomalias (Isolation Forest) no painel RA x ano e na série mensal de violência contra idosos",
)
def anomalias(
    limite: int = Query(50, ge=1, le=500, description="Quantidade máxima por série, do mais extremo ao menos"),
):
    try:
        return analise_service.obter_anomalias(limite=limite)
    except analise_service.DadosIndisponiveisError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get(
    "/zonas-quentes",
    response_model=ZonasQuentesResponse,
    summary="Células da malha com mais ocorrências patrimoniais no último ano",
)
def zonas_quentes(
    tamanho_celula_km: float = Query(1.5, gt=0, le=20, description="Lado da célula da malha em km"),
    top_n: int = Query(20, ge=1, le=200, description="Quantidade de células retornadas"),
):
    try:
        return analise_service.obter_zonas_quentes(
            tamanho_celula_km=tamanho_celula_km, top_n=top_n
        )
    except analise_service.DadosIndisponiveisError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
