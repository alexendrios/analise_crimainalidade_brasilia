# api/schemas.py
"""Modelos Pydantic (contratos de entrada/saída) da API."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field(..., examples=["ok"])
    database: str = Field(..., description="'ok' ou mensagem de erro de conexão")
    timestamp: datetime


class TabelaGoldInfo(BaseModel):
    nome: str
    descricao: str
    disponivel_no_banco: bool = Field(
        ..., description="True se a tabela já foi materializada no Postgres"
    )


class TabelasGoldResponse(BaseModel):
    total: int
    tabelas: List[TabelaGoldInfo]


class ResumoTabelaResponse(BaseModel):
    tabela: str
    linhas: int
    colunas: int
    nulos_total: int
    colunas_com_nulos: int
    tempo_execucao_s: float


class DadosTabelaResponse(BaseModel):
    tabela: str
    total_linhas: int
    pagina: int
    tamanho_pagina: int
    total_paginas: int
    registros: List[Dict[str, Any]]


class PontoPrevisao(BaseModel):
    ano: int
    valor_previsto: float
    componente_prophet: float
    residual_log_aplicado: float


class MetricasModelo(BaseModel):
    mae: float
    rmse: float


class PrevisaoResponse(BaseModel):
    tabela_origem: str
    coluna_alvo: str
    horizonte_anos: int
    gerado_em: datetime
    cache_ate: Optional[datetime] = None
    metricas_residual: MetricasModelo
    previsao: List[PontoPrevisao]
    fonte_modelo: Optional[str] = Field(
        None,
        description=(
            "'artefato' quando a previsão foi servida a partir de um bundle "
            "Prophet+XGBoost já persistido em models/, sem re-treinar; "
            "'retreino' quando o par foi treinado nesta própria requisição "
            "(nenhum artefato utilizável ainda, ou retreino explícito via "
            "POST /previsao/retrain)."
        ),
        examples=["artefato"],
    )
    modelo_arquivo: Optional[str] = Field(
        None, description="Nome do arquivo .pkl usado (ou recém-salvo) para esta previsão"
    )


class ModeloTreinadoInfo(BaseModel):
    arquivo: str
    criado_em: Optional[str] = None
    tipo_modelo: Optional[str] = None
    formato_artefato: Optional[str] = Field(
        None,
        description="'bundle' (Prophet+XGBoost juntos, servível sem re-treino) ou 'legacy' (apenas XGBoost)",
    )
    metricas: Optional[Dict[str, float]] = None
    dataset_info: Optional[Dict[str, Any]] = None


class ModelosTreinadosResponse(BaseModel):
    total: int
    modelos: List[ModeloTreinadoInfo]


class ErrorResponse(BaseModel):
    detail: str
