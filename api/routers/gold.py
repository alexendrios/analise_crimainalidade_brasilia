# api/routers/gold.py
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.schemas import DadosTabelaResponse, ResumoTabelaResponse, TabelasGoldResponse
from api.services import gold_service

router = APIRouter(prefix="/gold", tags=["Tabelas Gold"])


@router.get("/tabelas", response_model=TabelasGoldResponse, summary="Lista as tabelas gold disponíveis")
def listar_tabelas():
    return gold_service.listar_tabelas_gold()


@router.get(
    "/{tabela}/resumo",
    response_model=ResumoTabelaResponse,
    summary="Estatísticas descritivas de uma tabela gold",
)
def resumo_tabela(tabela: str):
    try:
        return gold_service.obter_resumo_tabela(tabela)
    except gold_service.TabelaInvalidaError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except gold_service.TabelaNaoEncontradaError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get(
    "/{tabela}/dados",
    response_model=DadosTabelaResponse,
    summary="Consulta paginada dos registros de uma tabela gold",
)
def dados_tabela(
    tabela: str,
    pagina: int = Query(1, ge=1, description="Número da página (1-indexed)"),
    tamanho_pagina: int = Query(50, ge=1, le=1000, description="Registros por página"),
    ano_min: Optional[int] = Query(None, description="Filtra ano >= ano_min, quando aplicável"),
    ano_max: Optional[int] = Query(None, description="Filtra ano <= ano_max, quando aplicável"),
    regiao_administrativa: Optional[str] = Query(
        None, description="Filtra por Região Administrativa (match exato, case-insensitive)"
    ),
):
    try:
        return gold_service.obter_dados_tabela(
            tabela,
            pagina=pagina,
            tamanho_pagina=tamanho_pagina,
            ano_min=ano_min,
            ano_max=ano_max,
            regiao_administrativa=regiao_administrativa,
        )
    except gold_service.TabelaInvalidaError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except gold_service.TabelaNaoEncontradaError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
