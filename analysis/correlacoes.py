# analysis/correlacoes.py
"""
Análise de correlação multivariada entre tipos de crime.

Três visões complementares sobre as tabelas gold:

1. Matriz temporal: indicadores agregados por ano (total do DF) e suas
   correlações de Pearson e Spearman.
2. Causalidade de Granger: teste pairwise "origem -> destino" entre séries
   anuais, com salvaguardas para séries curtas (~10 observações).
3. Correlação espacial entre tabelas gold: cross-section por Região
   Administrativa em um ano comum (ex.: violência contra idosos x crimes
   patrimoniais em 2016).

As séries gold são anuais e curtas; resultados de Granger devem ser lidos
como indícios exploratórios, não como evidência causal definitiva.
"""

import itertools

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.tsa.stattools import grangercausalitytests

from geoespacial.centroides import CENTROIDES_RA
from util.log import logs

logger = logs()

MIN_OBS_GRANGER = 8

# Indicadores extraídos de cada tabela gold para a matriz temporal:
# {tabela: {coluna_origem: nome_indicador}}
INDICADORES_TEMPORAIS = {
    "violencia_contra_mulher_gold": {
        "crimes_contra_mulher": "violencia_mulher",
        "casos_feminicidios": "feminicidio",
    },
    "crimes_roubo_furto_gold": {
        "ocorrencia_roubo_pedestre": "roubo_pedestre",
        "ocorrencia_roubo_comercio": "roubo_comercio",
        "ocorrencia_roubo_transporte_coletivo": "roubo_transporte",
        "ocorrencia_roubo_veiculo": "roubo_veiculo",
        "ocorrencia_furto_em_veiculo": "furto_veiculo",
    },
    "crimes_letais_gold": {
        "ocorrencia_homicidio": "homicidio",
        "ocorrencia_latrocinio": "latrocinio",
        "ocorrencia_lesao_morte": "lesao_morte",
    },
    "crimes_discriminatorios_gold": {
        "ocorrencia_racismo": "racismo",
        "ocorrencia_injuria": "injuria_racial",
    },
}


# =========================================================
# MATRIZ TEMPORAL DE INDICADORES
# =========================================================
def construir_matriz_indicadores(dados: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Consolida as tabelas gold anuais em uma matriz ano x indicador (total DF).

    :param dados: dicionário {nome_tabela: DataFrame} já carregadas.
    :return: DataFrame indexado por ano com uma coluna por indicador.
    """
    partes = []
    for tabela, mapeamento in INDICADORES_TEMPORAIS.items():
        if tabela not in dados or dados[tabela] is None or dados[tabela].empty:
            logger.warning("Tabela gold ausente na matriz de indicadores", extra={"tabela": tabela})
            continue

        df = dados[tabela]
        partes.append(
            df.groupby("ano", as_index=False)[list(mapeamento)].sum().rename(columns=mapeamento)
        )

    if not partes:
        raise ValueError("Nenhuma tabela gold válida foi informada")

    matriz = partes[0]
    for parte in partes[1:]:
        matriz = matriz.merge(parte, on="ano", how="outer")
    matriz = matriz.sort_values("ano").set_index("ano")
    return matriz[sorted(matriz.columns)]


def matriz_correlacao(
    matriz: pd.DataFrame, metodo: str = "pearson", minimo_obs: int = 5
) -> pd.DataFrame:
    """
    Matriz de correlação entre indicadores, descartando colunas sem
    observações suficientes (ex.: indicadores que só existem em poucos anos).
    """
    validas = [
        col for col in matriz.columns if matriz[col].notna().sum() >= minimo_obs
    ]
    descartadas = set(matriz.columns) - set(validas)
    if descartadas:
        logger.info(
            "Indicadores descartados por poucas observações",
            extra={"indicadores": sorted(descartadas), "minimo": minimo_obs},
        )
    return matriz[validas].corr(method=metodo)


def pares_mais_correlacionados(correlacao: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """
    Pares de indicadores ordenados pela correlação absoluta (sem duplicar
    simétricos nem a diagonal).
    """
    pares = []
    cols = correlacao.columns
    for a, b in itertools.combinations(cols, 2):
        valor = correlacao.loc[a, b]
        pares.append({"indicador_a": a, "indicador_b": b, "correlacao": float(valor)})
    return (
        pd.DataFrame(pares)
        .assign(absoluto=lambda d: d["correlacao"].abs())
        .sort_values("absoluto", ascending=False)
        .drop(columns="absoluto")
        .head(top_n)
        .reset_index(drop=True)
    )


# =========================================================
# CAUSALIDADE DE GRANGER
# =========================================================
def _pvalor_minimo(resultado: dict) -> tuple[int | None, float]:
    """Extrai o melhor lag (menor p-valor do teste F SSR) de um teste Granger."""
    melhor_lag, melhor_p = None, np.inf
    for lag, valores in resultado.items():
        p = float(valores[0]["ssr_ftest"][1])
        if p < melhor_p:
            melhor_lag, melhor_p = int(lag), p
    return melhor_lag, melhor_p


def causalidade_granger(
    matriz: pd.DataFrame,
    max_lag: int = 1,
    alpha: float = 0.05,
    minimo_obs: int = MIN_OBS_GRANGER,
) -> pd.DataFrame:
    """
    Testa Granger pairwise entre todas as séries com observações suficientes.

    Séries curtas (~10 anos) limitam o poder do teste; cada par é executado
    com proteção e falhas viram linhas com p-valor NaN.
    """
    series_validas = [
        col
        for col in matriz.columns
        if matriz[col].notna().sum() >= minimo_obs and matriz[col].std() > 0
    ]

    registros = []
    for origem, destino in itertools.permutations(series_validas, 2):
        par = matriz[[destino, origem]].dropna()
        try:
            resultado = grangercausalitytests(par, maxlag=max_lag, verbose=False)
            lag, p_valor = _pvalor_minimo(resultado)
            registros.append(
                {
                    "origem": origem,
                    "destino": destino,
                    "melhor_lag": lag,
                    "p_valor": p_valor,
                    "significante": bool(p_valor < alpha),
                }
            )
        except Exception as erro:
            logger.warning(
                "Teste de Granger falhou para o par",
                extra={"origem": origem, "destino": destino, "erro": str(erro)},
            )
            registros.append(
                {
                    "origem": origem,
                    "destino": destino,
                    "melhor_lag": None,
                    "p_valor": np.nan,
                    "significante": False,
                }
            )

    return pd.DataFrame(registros).sort_values("p_valor").reset_index(drop=True)


# =========================================================
# CORRELAÇÃO ESPACIAL ENTRE TABELAS GOLD (cross-section por RA)
# =========================================================
def _chave_ra(nome: str) -> str:
    from util.padronizacao import remover_acentos

    return remover_acentos(str(nome).strip()).upper()


def correlacao_idosos_patrimoniais(
    idosos: pd.DataFrame, patrimonial: pd.DataFrame, ano_patrimonial: int = 2016
) -> dict:
    """
    Correlação espacial (por RA) entre violência contra idosos e crimes
    patrimoniais no mesmo recorte temporal.

    A série anual de idosos cobre 2010-2016 enquanto o painel patrimonial
    começa em 2015 — a interseção útil é 2016, então a comparação é feita em
    cross-section (número de RAs como amostra), não ao longo do tempo.

    :param idosos: tabela `violencia_idosos_gold` (RA + jan_ago_2016/2017).
    :param patrimonial: tabela `crimes_roubo_furto_gold` (RA + ano + tipos).
    """
    coluna_idosos = f"jan_ago_{ano_patrimonial}"
    if coluna_idosos not in idosos.columns:
        raise ValueError(f"Coluna '{coluna_idosos}' ausente na tabela de idosos")

    base_idosos = (
        idosos.assign(regiao=lambda d: d["regiao_administrativa"].map(_chave_ra))
        .loc[lambda d: d["regiao"].isin(set(CENTROIDES_RA))]
        [["regiao", coluna_idosos]]
        .rename(columns={coluna_idosos: "violencia_idosos"})
    )

    tipos = [c for c in patrimonial.columns if c.startswith("ocorrencia_")]
    base_patrimonial = (
        patrimonial.query("ano == @ano_patrimonial")
        .assign(regiao=lambda d: d["regiao_administrativa"].map(_chave_ra))
        .groupby("regiao", as_index=False)[tipos]
        .sum()
        .assign(patrimonial_total=lambda d: d[tipos].sum(axis=1))
        [["regiao", "patrimonial_total"]]
    )

    cruzamento = base_idosos.merge(base_patrimonial, on="regiao", how="inner")
    n = len(cruzamento)

    if n < 3:
        logger.warning(
            "Amostra insuficiente para correlação idosos x patrimonial",
            extra={"n": n},
        )
        return {"n_ra": n, "pearson": np.nan, "spearman": np.nan}

    pearson = stats.pearsonr(cruzamento["violencia_idosos"], cruzamento["patrimonial_total"])
    spearman = stats.spearmanr(cruzamento["violencia_idosos"], cruzamento["patrimonial_total"])

    return {
        "ano_referencia": ano_patrimonial,
        "n_ra": n,
        "pearson": float(pearson.statistic),
        "p_valor_pearson": float(pearson.pvalue),
        "spearman": float(spearman.statistic),
        "p_valor_spearman": float(spearman.pvalue),
    }


# =========================================================
# INSIGHTS TEXTUAIS
# =========================================================
def insights_correlacao(
    correlacao: pd.DataFrame,
    granger: pd.DataFrame | None = None,
    top_n: int = 3,
) -> list[str]:
    """
    Gera frases-síntese em português para o relatório executivo.
    """
    insights = []
    for _, linha in pares_mais_correlacionados(correlacao, top_n=top_n).iterrows():
        direcao = "positiva" if linha["correlacao"] > 0 else "negativa"
        insights.append(
            f"'{linha['indicador_a']}' e '{linha['indicador_b']}' têm correlação "
            f"{direcao} forte ({linha['correlacao']:+.2f})."
        )

    if granger is not None and not granger.empty:
        significantes = granger.query("significante")
        for _, linha in significantes.head(top_n).iterrows():
            insights.append(
                f"Série '{linha['origem']}' antecede (Granger) '{linha['destino']}' "
                f"com {linha['melhor_lag']} ano(s) de defasagem (p={linha['p_valor']:.3f})."
            )
        if significantes.empty:
            insights.append(
                "Nenhuma relação de Granger estatisticamente significante foi detectada "
                "com as séries anuais disponíveis."
            )

    return insights
