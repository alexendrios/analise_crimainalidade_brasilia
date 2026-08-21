# api/services/classificacao_service.py
"""
Camada de serviço da API para a classificação por Regressão Logística.

Reaproveita as funções já existentes e testadas em
`analysis/logistic_regression.py` (carregar_dados, preparar_features,
treinar_regressao_logistica, salvar_modelo) em vez de duplicar a lógica
de modelagem aqui.

Estratégia de serving: o pipeline (StandardScaler + LogisticRegression)
persistido em `models/logreg_criminalidade_letal_*.pkl` é servido a partir
do artefato mais recente (`fonte_modelo="artefato"`, sem re-treinar). Se
nenhum artefato utilizável existir — ou se `forcar_retreino=True` — o
modelo é treinado sob demanda a partir das tabelas gold mais recentes
(`fonte_modelo="retreino"`), caminho usado pelo endpoint explícito
`POST /classificacao/retrain`. Um cache simples em memória evita repetir
carga/predição a cada requisição.
"""

import glob
import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import joblib

from analysis.logistic_regression import (
    ALVO,
    FEATURES,
    MODELS_DIR,
    TABELA_CRIMES_LETAIS,
    TABELA_POPULACAO,
    carregar_dados,
    preparar_features,
    salvar_modelo,
    treinar_regressao_logistica,
)
from api.config import CACHE_PREVISAO_TTL_SEGUNDOS
from util.log import logs

logger = logs()

# Cache simples em memória: {chave: (expira_em_epoch, payload)}
_CACHE: Dict[str, tuple] = {}
_CHAVE_CACHE = "classificacao_criminalidade_letal"

PREFIXO_ARTEFATO = "logreg_criminalidade_letal_"


class DadosInsuficientesError(ValueError):
    """Levantada quando não há dados suficientes nas tabelas gold para treinar/classificar."""


def limpar_cache() -> None:
    """Utilitário de suporte a testes/operacional: limpa o cache em memória."""
    _CACHE.clear()


def _localizar_ultimo_artefato(models_dir: Optional[str] = None) -> Tuple[Optional[str], Optional[dict]]:
    """
    Procura o artefato de Regressão Logística mais recente em `models_dir`
    (por padrão o MODELS_DIR do módulo de análise, resolvido na chamada),
    com base no campo `created_at` do respectivo *_meta.json.

    :return: tupla `(model_path, meta_dict)`, ou `(None, None)` quando não
        há nenhum artefato utilizável.
    """
    models_dir = models_dir or MODELS_DIR
    padrao = os.path.join(models_dir, f"{PREFIXO_ARTEFATO}*_meta.json")
    candidatos = []

    for meta_path in glob.glob(padrao):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except (OSError, json.JSONDecodeError):
            logger.warning("⚠️ Não foi possível ler metadados em: %s", meta_path)
            continue

        model_file = meta.get("model_file")
        if not model_file:
            continue

        model_path = os.path.join(models_dir, model_file)
        if not os.path.exists(model_path):
            continue

        candidatos.append((meta.get("created_at", ""), model_path, meta))

    if not candidatos:
        return None, None

    candidatos.sort(key=lambda item: item[0], reverse=True)
    return candidatos[0][1], candidatos[0][2]


def _classificar_com_modelo(modelo, df) -> Tuple[List[int], List[float]]:
    """Gera classes e probabilidades previstas para todas as linhas de df."""
    preds = modelo.predict(df[FEATURES]).tolist()
    probas = modelo.predict_proba(df[FEATURES])[:, 1].tolist()
    return [int(p) for p in preds], probas


def _tentar_servir_de_artefato(df):
    """
    Tenta classificar usando o pipeline persistido mais recente, sem re-treinar.

    :return: dict parcial do resultado, ou None quando não há artefato
        utilizável (inexistente ou corrompido).
    """
    model_path, meta = _localizar_ultimo_artefato()

    if model_path is None:
        return None

    try:
        modelo = joblib.load(model_path)
    except Exception:
        logger.exception(
            "⚠️ Falha ao carregar artefato persistido em %s, re-treinando.", model_path
        )
        return None

    preds, probas = _classificar_com_modelo(modelo, df)

    extra = (meta or {}).get("extra", {})
    logger.info("📦 Classificação servida a partir do artefato: %s", model_path)

    return {
        "metricas": (meta or {}).get("metrics", {}),
        "odds_ratios": extra.get("odds_ratios", {}),
        "matriz_confusao": extra.get("matriz_confusao", []),
        "modelo_arquivo": (meta or {}).get("model_file"),
        "preds": preds,
        "probas": probas,
    }


def _montar_classificacoes(df, preds, probas) -> List[Dict[str, Any]]:
    """Combina as linhas da base com as predições, ordenado da maior para a
    menor probabilidade de alta criminalidade."""
    itens = [
        {
            "regiao_administrativa": row.regiao_administrativa,
            "ano": int(row.ano_num),
            "classe_prevista": int(p),
            "rotulo_previsto": "alta" if p == 1 else "baixa",
            "probabilidade_alta": round(float(pr), 4),
        }
        for row, p, pr in zip(df.itertuples(), preds, probas)
    ]
    itens.sort(key=lambda item: item["probabilidade_alta"], reverse=True)
    return itens


def classificar_criminalidade(
    usar_cache: bool = True,
    forcar_retreino: bool = False,
    persistir_modelo: bool = False,
) -> Dict[str, Any]:
    """
    Classifica cada par (RA, ano) como alta/baixa criminalidade letal usando
    a Regressão Logística de `analysis/logistic_regression.py`.

    :param usar_cache: reaproveita payload recente em cache (TTL configurado
        em api/config.py).
    :param forcar_retreino: ignora artefatos persistidos e treina na hora.
    :param persistir_modelo: se True e um treino ocorrer nesta chamada, o
        novo pipeline é salvo em models/.
    """
    agora = time.time()

    if usar_cache and not forcar_retreino and _CHAVE_CACHE in _CACHE:
        expira_em, payload = _CACHE[_CHAVE_CACHE]
        if agora < expira_em:
            logger.info("♻️ Retornando classificação do cache")
            return payload

    df_bruto = carregar_dados()

    if df_bruto is None or len(df_bruto) == 0:
        raise DadosInsuficientesError(
            f"As tabelas '{TABELA_CRIMES_LETAIS}'/'{TABELA_POPULACAO}' estão vazias "
            f"ou não foram materializadas. Execute o pipeline gold antes de "
            f"solicitar uma classificação."
        )

    df, limiar = preparar_features(df_bruto)

    servido = None if forcar_retreino else _tentar_servir_de_artefato(df)

    if servido is not None:
        fonte_modelo = "artefato"
        resultado = servido
    else:
        fonte_modelo = "retreino"
        treinado = treinar_regressao_logistica(df)

        modelo_arquivo = None
        if persistir_modelo:
            model_path, _meta_path = salvar_modelo(treinado)
            modelo_arquivo = os.path.basename(model_path)

        preds, probas = _classificar_com_modelo(treinado["modelo"], df)
        resultado = {
            "metricas": treinado["metricas"],
            "odds_ratios": treinado["odds_ratios"],
            "matriz_confusao": treinado["matriz_confusao"],
            "modelo_arquivo": modelo_arquivo,
            "preds": preds,
            "probas": probas,
        }

    gerado_em = datetime.now()
    cache_ate = gerado_em + timedelta(seconds=CACHE_PREVISAO_TTL_SEGUNDOS)

    distribuicao_real = df[ALVO].value_counts().to_dict()

    payload = {
        "tabelas_origem": [TABELA_CRIMES_LETAIS, TABELA_POPULACAO],
        "total_registros": len(df),
        "total_ras": int(df["regiao_administrativa"].nunique()),
        "periodo": [int(df["ano_num"].min()), int(df["ano_num"].max())],
        "limiar_taxa_mediana": float(limiar),
        "distribuicao_real": {
            "alta": int(distribuicao_real.get(1, 0)),
            "baixa": int(distribuicao_real.get(0, 0)),
        },
        "metricas": resultado["metricas"],
        "odds_ratios": resultado["odds_ratios"],
        "matriz_confusao": resultado["matriz_confusao"],
        "classificacoes": _montar_classificacoes(
            df, resultado["preds"], resultado["probas"]
        ),
        "gerado_em": gerado_em,
        "cache_ate": cache_ate,
        "fonte_modelo": fonte_modelo,
        "modelo_arquivo": resultado["modelo_arquivo"],
    }

    _CACHE[_CHAVE_CACHE] = (agora + CACHE_PREVISAO_TTL_SEGUNDOS, payload)

    return payload
