# api/services/forecast_service.py
"""
Camada de serviço da API para a modelagem preditiva.

Reaproveita as funções já existentes e testadas em
`analysis/data_analyzer.py` (preparar_dados, treinar_residual,
prever_futuro) em vez de duplicar a lógica de modelagem aqui.

Importante: os artefatos hoje persistidos em `models/*.pkl` guardam
apenas o regressor XGBoost do resíduo — o modelo Prophet correspondente
não é salvo (ver `projeto.md`, seção "Pontos de Atenção"). Por isso,
para servir uma previsão consistente (Prophet + resíduo XGBoost), a API
re-treina o par Prophet/XGBoost sob demanda a partir da tabela gold mais
recente, e mantém um cache simples em memória para não re-treinar a
cada requisição.
"""

import glob
import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from analysis.data_analyzer import (
    calcular_metricas,
    preparar_dados,
    prever_futuro,
    save_model_with_metadata,
    treinar_residual,
)
from api.config import (
    CACHE_PREVISAO_TTL_SEGUNDOS,
    COLUNA_ALVO_PREVISAO,
    TABELA_MODELO_PREVISAO,
)
from ingestion.repository_adapter import Repository
from util.log import logs

logger = logs()

# Cache simples em memória: {horizonte_anos: (expira_em_epoch, payload)}
_CACHE: Dict[int, tuple] = {}


class DadosInsuficientesError(ValueError):
    """Levantada quando não há dados suficientes na tabela gold para treinar/prever."""


def gerar_previsao(
    horizonte_anos: int = 5, usar_cache: bool = True, persistir_modelo: bool = False
) -> Dict[str, Any]:
    """
    Gera (ou retorna do cache) a previsão de `crimes_contra_mulher` para
    os próximos `horizonte_anos` anos, usando o pipeline híbrido
    Prophet + XGBoost já existente em `analysis/data_analyzer.py`.
    """
    agora = time.time()

    if usar_cache and horizonte_anos in _CACHE:
        expira_em, payload = _CACHE[horizonte_anos]
        if agora < expira_em:
            logger.info("♻️ Retornando previsão do cache (horizonte=%s anos)", horizonte_anos)
            return payload

    df = Repository.load(TABELA_MODELO_PREVISAO)

    if df is None or len(df) == 0:
        raise DadosInsuficientesError(
            f"A tabela '{TABELA_MODELO_PREVISAO}' está vazia ou não foi materializada. "
            f"Execute o pipeline gold antes de solicitar uma previsão."
        )

    df_preparado = preparar_dados(df, COLUNA_ALVO_PREVISAO)

    if len(df_preparado) < 4:
        raise DadosInsuficientesError(
            "Dados históricos insuficientes após feature engineering "
            "(mínimo recomendado: 4 anos) para treinar o modelo residual."
        )

    (
        model,
        prophet_model,
        metrics,
        residual_min,
        residual_max,
        hyperparams,
    ) = treinar_residual(df_preparado, COLUNA_ALVO_PREVISAO)

    forecast_df = prever_futuro(
        model,
        prophet_model,
        df_preparado,
        COLUNA_ALVO_PREVISAO,
        residual_min,
        residual_max,
        anos=horizonte_anos,
    )

    if persistir_modelo:
        _persistir_modelo(df_preparado, model, metrics, hyperparams, residual_min, residual_max)

    pontos = [
        {
            "ano": int(row["ano"].year) if hasattr(row["ano"], "year") else int(row["ano"]),
            "valor_previsto": round(float(row["final"]), 2),
            "componente_prophet": round(float(row["prophet"]), 2),
            "residual_log_aplicado": round(float(row["residual_log"]), 6),
        }
        for _, row in forecast_df.iterrows()
    ]

    gerado_em = datetime.now()
    cache_ate = gerado_em + timedelta(seconds=CACHE_PREVISAO_TTL_SEGUNDOS)

    payload = {
        "tabela_origem": TABELA_MODELO_PREVISAO,
        "coluna_alvo": COLUNA_ALVO_PREVISAO,
        "horizonte_anos": horizonte_anos,
        "gerado_em": gerado_em,
        "cache_ate": cache_ate,
        "metricas_residual": metrics,
        "previsao": pontos,
    }

    _CACHE[horizonte_anos] = (agora + CACHE_PREVISAO_TTL_SEGUNDOS, payload)

    return payload


def _persistir_modelo(df_preparado, model, metrics, hyperparams, residual_min, residual_max):
    """Salva o modelo residual + metadados, seguindo o mesmo padrão de main.py."""
    from analysis.data_analyzer import FEATURES, MODEL_PATH

    metadata = {
        "metrics": metrics,
        "hyperparameters": hyperparams,
        "features": FEATURES,
        "target": "residual_log",
        "dataset_info": {
            "source_table": TABELA_MODELO_PREVISAO,
            "target_column": COLUNA_ALVO_PREVISAO,
            "total_records": len(df_preparado),
            "period_min": str(df_preparado["ano"].min().year),
            "period_max": str(df_preparado["ano"].max().year),
        },
        "extra": {
            "residual_bounds": {"min": residual_min, "max": residual_max},
            "forecast_horizon_years": 5,
            "gerado_via": "api",
        },
    }
    save_model_with_metadata(model, MODEL_PATH, metadata)


def listar_modelos_treinados(models_dir: str = "models") -> Dict[str, Any]:
    """Lista os modelos já persistidos em disco (`models/*_meta.json`)."""
    padrao = os.path.join(models_dir, "*_meta.json")
    arquivos_meta = sorted(glob.glob(padrao), reverse=True)

    modelos: List[Dict[str, Any]] = []

    for caminho in arquivos_meta:
        try:
            with open(caminho, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            logger.exception("⚠️ Falha ao ler metadados do modelo: %s", caminho)
            continue

        modelos.append(
            {
                "arquivo": meta.get("model_file", os.path.basename(caminho)),
                "criado_em": meta.get("created_at"),
                "tipo_modelo": meta.get("model_type"),
                "metricas": meta.get("metrics"),
                "dataset_info": meta.get("dataset_info"),
            }
        )

    return {"total": len(modelos), "modelos": modelos}


def limpar_cache() -> None:
    """Utilitário de suporte a testes/operacional: limpa o cache em memória."""
    _CACHE.clear()
