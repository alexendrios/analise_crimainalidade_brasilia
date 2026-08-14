# api/services/forecast_service.py
"""
Camada de serviço da API para a modelagem preditiva.

Reaproveita as funções já existentes e testadas em
`analysis/data_analyzer.py` (preparar_dados, treinar_residual,
prever_futuro) em vez de duplicar a lógica de modelagem aqui.

Estratégia de serving (Prophet + XGBoost persistidos juntos):
os artefatos em `models/*.pkl` podem ser salvos no formato "bundle"
(Prophet + XGBoost no mesmo arquivo, ver `analysis.data_analyzer.
save_model_with_metadata`). Por padrão, `gerar_previsao` tenta servir a
previsão a partir do bundle mais recente em disco (`fonte_modelo=
"artefato"`, sem re-treinar). Se nenhum bundle utilizável existir (ainda
não há nenhum na primeira execução, ou o argumento `forcar_retreino=True`
foi passado), o par Prophet/XGBoost é re-treinado sob demanda a partir da
tabela gold mais recente (`fonte_modelo="retreino"`) — esse é o caminho
usado pelo endpoint explícito `POST /previsao/retrain`. Em ambos os
casos, mantém-se um cache simples em memória para não repetir trabalho
a cada requisição.
"""

import glob
import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from analysis.data_analyzer import (
    FEATURES,
    MODELS_DIR,
    calcular_metricas,
    carregar_modelo,
    localizar_ultimo_modelo_bundle,
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
    horizonte_anos: int = 5,
    usar_cache: bool = True,
    forcar_retreino: bool = False,
    persistir_modelo: bool = False,
) -> Dict[str, Any]:
    """
    Gera (ou retorna do cache) a previsão de `crimes_contra_mulher` para
    os próximos `horizonte_anos` anos, usando o pipeline híbrido
    Prophet + XGBoost já existente em `analysis/data_analyzer.py`.

    :param forcar_retreino: se True, ignora qualquer artefato persistido em
        `models/` e treina um par Prophet/XGBoost novo a partir dos dados
        atuais (usado pelo endpoint `POST /previsao/retrain`).
    :param persistir_modelo: se True e um re-treino de fato ocorrer (seja
        por `forcar_retreino=True`, seja por não existir nenhum bundle
        salvo ainda), o novo par Prophet/XGBoost é salvo em `models/`.
    """
    agora = time.time()

    if usar_cache and not forcar_retreino and horizonte_anos in _CACHE:
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

    forecast_df, metrics, fonte_modelo, modelo_arquivo = _obter_previsao(
        df_preparado, horizonte_anos, forcar_retreino, persistir_modelo
    )

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
        "fonte_modelo": fonte_modelo,
        "modelo_arquivo": modelo_arquivo,
    }

    _CACHE[horizonte_anos] = (agora + CACHE_PREVISAO_TTL_SEGUNDOS, payload)

    return payload


def _obter_previsao(df_preparado, horizonte_anos, forcar_retreino, persistir_modelo):
    """
    Decide entre servir a previsão a partir de um artefato já persistido
    (rápido, sem treinar) ou re-treinar o par Prophet/XGBoost na hora.

    :return: tupla `(forecast_df, metrics, fonte_modelo, modelo_arquivo)`.
    """
    if not forcar_retreino:
        forecast_df, metrics, modelo_arquivo = _tentar_servir_de_artefato(
            df_preparado, horizonte_anos
        )
        if forecast_df is not None:
            return forecast_df, metrics, "artefato", modelo_arquivo

    model, prophet_model, metrics, residual_min, residual_max, hyperparams = treinar_residual(
        df_preparado, COLUNA_ALVO_PREVISAO
    )

    forecast_df = prever_futuro(
        model,
        prophet_model,
        df_preparado,
        COLUNA_ALVO_PREVISAO,
        residual_min,
        residual_max,
        anos=horizonte_anos,
    )

    modelo_arquivo = None
    if persistir_modelo:
        modelo_arquivo = _persistir_modelo(
            df_preparado, model, prophet_model, metrics, hyperparams, residual_min, residual_max
        )

    return forecast_df, metrics, "retreino", modelo_arquivo


def _tentar_servir_de_artefato(df_preparado, horizonte_anos):
    """
    Tenta reconstruir a previsão a partir do bundle Prophet+XGBoost mais
    recente persistido em `models/`, sem re-treinar.

    :return: tupla `(forecast_df, metrics, modelo_arquivo)`, ou
        `(None, None, None)` quando não há nenhum artefato utilizável
        (nenhum bundle salvo ainda, artefato corrompido, ou metadados
        sem os limites do resíduo necessários para `prever_futuro`).
    """
    model_path, meta = localizar_ultimo_modelo_bundle(MODELS_DIR)

    if model_path is None:
        return None, None, None

    try:
        xgb_model, prophet_model = carregar_modelo(model_path)
    except Exception:
        logger.exception("⚠️ Falha ao carregar artefato persistido em %s, re-treinando.", model_path)
        return None, None, None

    if xgb_model is None or prophet_model is None:
        logger.warning(
            "⚠️ Artefato %s não contém o par Prophet+XGBoost completo, re-treinando.", model_path
        )
        return None, None, None

    bounds = (meta or {}).get("extra", {}).get("residual_bounds", {})
    residual_min = bounds.get("min")
    residual_max = bounds.get("max")

    if residual_min is None or residual_max is None:
        logger.warning(
            "⚠️ Metadados de %s sem residual_bounds, re-treinando.", model_path
        )
        return None, None, None

    forecast_df = prever_futuro(
        xgb_model,
        prophet_model,
        df_preparado,
        COLUNA_ALVO_PREVISAO,
        residual_min,
        residual_max,
        anos=horizonte_anos,
    )

    metrics = (meta or {}).get("metrics", {})
    modelo_arquivo = (meta or {}).get("model_file", os.path.basename(model_path))

    logger.info("📦 Previsão servida a partir do artefato: %s", modelo_arquivo)

    return forecast_df, metrics, modelo_arquivo


def _gerar_model_path() -> str:
    """Gera um caminho de artefato novo, com timestamp em segundos, para
    evitar sobrescrever o modelo salvo por uma requisição anterior no
    mesmo minuto (o `MODEL_PATH` de `analysis.data_analyzer` é fixado
    uma única vez, no import do módulo, o que não serve para a API)."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(MODELS_DIR, f"xgb_residual_log_{timestamp}.pkl")


def _persistir_modelo(
    df_preparado, model, prophet_model, metrics, hyperparams, residual_min, residual_max
) -> str:
    """Salva o bundle Prophet+XGBoost + metadados, seguindo o mesmo padrão de main.py.

    :return: o nome do arquivo .pkl salvo (basename).
    """
    model_path = _gerar_model_path()

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
    save_model_with_metadata(model, model_path, metadata, prophet_model=prophet_model)

    return os.path.basename(model_path)


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
                "formato_artefato": meta.get("artifact_format", "legacy"),
                "metricas": meta.get("metrics"),
                "dataset_info": meta.get("dataset_info"),
            }
        )

    return {"total": len(modelos), "modelos": modelos}


def limpar_cache() -> None:
    """Utilitário de suporte a testes/operacional: limpa o cache em memória."""
    _CACHE.clear()
