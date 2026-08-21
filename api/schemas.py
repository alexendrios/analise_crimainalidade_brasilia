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
    r2: Optional[float] = None
    escala_original: Optional[Dict[str, float]] = Field(
        None,
        description=(
            "Métricas (mae/rmse/r2) na escala original de contagem de casos, "
            "agregadas pelo backtesting — além das métricas do resíduo em log."
        ),
    )


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
    metricas: Optional[Dict[str, Any]] = None
    dataset_info: Optional[Dict[str, Any]] = None


class ModelosTreinadosResponse(BaseModel):
    total: int
    modelos: List[ModeloTreinadoInfo]


class ClassificacaoRegiaoItem(BaseModel):
    regiao_administrativa: str
    ano: int
    classe_prevista: int = Field(
        ..., description="1 = alta criminalidade letal, 0 = baixa"
    )
    rotulo_previsto: str = Field(..., examples=["alta"])
    probabilidade_alta: float = Field(
        ..., ge=0.0, le=1.0, description="Probabilidade prevista de alta criminalidade"
    )


class MetricasClassificacao(BaseModel):
    cv_roc_auc_media: Optional[float] = None
    cv_roc_auc_std: Optional[float] = None
    holdout_accuracy: Optional[float] = None
    holdout_precision: Optional[float] = None
    holdout_recall: Optional[float] = None
    holdout_f1: Optional[float] = None
    holdout_roc_auc: Optional[float] = None


class ClassificacaoResponse(BaseModel):
    tabelas_origem: List[str]
    total_registros: int
    total_ras: int
    periodo: List[int]
    limiar_taxa_mediana: float = Field(
        ...,
        description=(
            "Taxa total de crimes letais por 100 mil habitantes que separa "
            "as classes (mediana da base usada no treino)"
        ),
    )
    distribuicao_real: Dict[str, int] = Field(
        ..., description="Contagem real de registros por classe ('alta'/'baixa')"
    )
    metricas: MetricasClassificacao
    odds_ratios: Dict[str, float] = Field(
        ..., description="Odds ratio por feature (exp(coeficiente))"
    )
    matriz_confusao: List[List[int]]
    classificacoes: List[ClassificacaoRegiaoItem]
    gerado_em: datetime
    cache_ate: Optional[datetime] = None
    fonte_modelo: Optional[str] = Field(
        None,
        description=(
            "'artefato' quando servido a partir do pipeline de Regressão "
            "Logística já persistido em models/, sem re-treinar; 'retreino' "
            "quando treinado nesta requisição."
        ),
        examples=["artefato"],
    )
    modelo_arquivo: Optional[str] = Field(
        None, description="Nome do arquivo .pkl usado (ou recém-salvo) para esta classificação"
    )


class ErrorResponse(BaseModel):
    detail: str
