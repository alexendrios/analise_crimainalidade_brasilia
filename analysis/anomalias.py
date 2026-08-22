# analysis/anomalias.py
"""
Detecção de outliers e anomalias em séries de criminalidade (Isolation Forest).

A detecção opera sobre features causais simples derivadas da própria série
(valor, defasagem, diferença e média móvel), permitindo identificar:

- picos ou quedas abruptas em um ano/mês específico;
- mudanças de padrão persistentes (possível alteração metodológica na fonte).

Funciona tanto para a série mensal de violência contra idosos quanto para
painéis ano x Região Administrativa das tabelas patrimoniais.
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from util.log import logs

logger = logs()

CONTAMINACAO_PADRAO = 0.15
RANDOM_STATE = 42


def _features_temporais(grupo: pd.DataFrame, coluna_valor: str) -> pd.DataFrame:
    """Deriva lag, diferença e média móvel causal para o treino do modelo."""
    serie = grupo.sort_values(coluna_tempo_padrao(grupo)).copy()
    serie["lag_1"] = serie[coluna_valor].shift(1)
    serie["diff_1"] = serie[coluna_valor].diff(1)
    serie["media_movel_3"] = serie[coluna_valor].shift(1).rolling(3, min_periods=2).mean()
    return serie


def coluna_tempo_padrao(df: pd.DataFrame) -> str:
    """Escolhe a coluna temporal do DataFrame (mes_num tem prioridade sobre ano)."""
    if "mes_num" in df.columns:
        return "mes_num"
    if "ano" not in df.columns:
        raise ValueError("Série exige coluna 'ano' ou 'mes_num'")
    return "ano"


def detectar_anomalias(
    serie: pd.DataFrame,
    coluna_valor: str,
    contaminacao: float = CONTAMINACAO_PADRAO,
    random_state: int = RANDOM_STATE,
) -> pd.DataFrame:
    """
    Marca pontos anômalos de uma série temporal com Isolation Forest.

    Linhas iniciais sem histórico suficiente (lags indisponíveis) não são
    avaliadas pelo modelo e recebem `anomalia=False` / `score=NaN`.

    :param serie: DataFrame com coluna temporal ('ano' ou 'mes_num') e `coluna_valor`.
    :param coluna_valor: coluna numérica analisada (ex.: 'fato', 'ocorrencias').
    :return: cópia da série com colunas extras `anomalia` (bool) e `score`.
    """
    for obrigatoria in (coluna_tempo_padrao(serie), coluna_valor):
        if obrigatoria not in serie.columns:
            raise ValueError(f"Coluna '{obrigatoria}' ausente na série")

    base = _features_temporais(serie, coluna_valor)
    features = ["lag_1", "diff_1", "media_movel_3"]
    treinavel = base.dropna(subset=[coluna_valor] + features)

    base["anomalia"] = False
    base["score"] = np.nan

    n_minimo = 6
    if len(treinavel) >= n_minimo:
        modelo = IsolationForest(
            contamination=contaminacao,
            random_state=random_state,
            n_estimators=200,
        )
        modelo.fit(treinavel[[coluna_valor] + features])
        predicao = modelo.predict(treinavel[[coluna_valor] + features])
        escores = modelo.score_samples(treinavel[[coluna_valor] + features])

        base.loc[treinavel.index, "anomalia"] = predicao == -1
        base.loc[treinavel.index, "score"] = escores

        logger.info(
            "Anomalias detectadas",
            extra={"total": int(base["anomalia"].sum()), "avaliados": len(treinavel)},
        )
    else:
        logger.warning(
            "Série curta demais para Isolation Forest",
            extra={"observacoes": len(treinavel), "minimo": n_minimo},
        )

    return base.reset_index(drop=True)


def detectar_anomalias_painel(
    painel: pd.DataFrame,
    coluna_valor: str,
    colunas_grupo: tuple = ("regiao_administrativa",),
    **kwargs,
) -> pd.DataFrame:
    """
    Aplica a detecção por grupo de um painel (ex.: uma série por RA),
    concatenando o resultado com identificação do grupo.
    """
    resultados = []
    for chaves, grupo in painel.groupby(list(colunas_grupo)):
        rotulo = dict(zip(colunas_grupo, chaves if isinstance(chaves, tuple) else (chaves,)))
        marcado = detectar_anomalias(grupo, coluna_valor, **kwargs)
        for nome, valor in rotulo.items():
            marcado[nome] = valor
        resultados.append(marcado)

    if not resultados:
        raise ValueError("Painel vazio")

    return pd.concat(resultados, ignore_index=True)


def resumo_anomalias(marcado: pd.DataFrame, colunas_contexto: tuple = ()) -> pd.DataFrame:
    """
    Tabela-resumo apenas dos pontos anômalos, ordenada do mais extremo para
    o menos extremo (score menor = mais isolado na floresta).
    """
    colunas = list(dict.fromkeys(list(colunas_contexto) + list(marcado.columns)))
    colunas.remove("anomalia")
    colunas.remove("score")
    anomalias = (
        marcado.query("anomalia")
        .sort_values("score")
        .reset_index(drop=True)[colunas]
    )
    logger.info(
        "Resumo de anomalias gerado",
        extra={"casos": len(anomalias)},
    )
    return anomalias
