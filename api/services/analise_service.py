# api/services/analise_service.py
"""
Camada de serviço da API para as análises executivas.

Reaproveita os módulos de análise já existentes e testados
(`analysis.correlacoes`, `analysis.anomalias`, `analysis.mapa`) sobre as
tabelas gold carregadas pelo mesmo loader do pipeline de análise
(`analysis.pipeline_analise._carregar_tabelas`), sem duplicar lógica.
"""

import json

from analysis.anomalias import detectar_anomalias, detectar_anomalias_painel, resumo_anomalias
from analysis.correlacoes import (
    construir_matriz_indicadores,
    causalidade_granger,
    insights_correlacao,
    matriz_correlacao,
    pares_mais_correlacionados,
)
from analysis.mapa import gerar_agregado_celulas
from analysis.pipeline_analise import _carregar_tabelas as carregar_tabelas_gold
from util.log import logs

logger = logs()

TABELA_PAINEL = "crimes_roubo_furto_gold"
TABELA_MENSAL = "violencia_idosos_mensais_gold"
INDICADOR_PAINEL = "ocorrencia_roubo_pedestre"
INDICADOR_MENSAL = "fato"

ALPHA_GRANGER_PADRAO = 0.05


class DadosIndisponiveisError(LookupError):
    """Levantada quando as tabelas gold necessárias não estão materializadas."""


def _registros(df) -> list[dict]:
    """Converte um DataFrame em registros JSON-safe (NaN -> null, tipos nativos)."""
    return json.loads(df.to_json(orient="records", force_ascii=False))


def _dados() -> dict:
    try:
        return carregar_tabelas_gold()
    except Exception as exc:
        logger.exception("🔥 Falha ao carregar tabelas gold para /analise")
        raise DadosIndisponiveisError(
            f"Não foi possível carregar as tabelas gold: {exc}"
        ) from exc


def obter_correlacoes(metodo: str = "pearson", top_n: int = 5) -> dict:
    """
    Matriz de correlação multivariada entre os indicadores gold (total DF),
    pares mais correlacionados e insights textuais.
    """
    dados = _dados()
    try:
        serie = construir_matriz_indicadores(dados)
        correlacao = matriz_correlacao(serie, metodo=metodo)
        pares = pares_mais_correlacionados(correlacao, top_n=top_n)
        insights = insights_correlacao(correlacao)
    except ValueError as exc:
        raise DadosIndisponiveisError(str(exc)) from exc

    # to_json já serializa NaN como null e tipos numpy como nativos JSON
    matriz_json = json.loads(correlacao.to_json(force_ascii=False, double_precision=4))

    logger.info("Correlações servidas", extra={"metodo": metodo, "indicadores": len(correlacao.columns)})
    return {
        "metodo": metodo,
        "periodo": [int(serie.index.min()), int(serie.index.max())],
        "indicadores": list(correlacao.columns),
        "matriz_correlacao": matriz_json,
        "serie_historica": _registros(serie.reset_index()),
        "pares_destaque": _registros(pares),
        "insights": insights,
    }


def obter_granger(
    max_lag: int = 1,
    apenas_significantes: bool = True,
    limite: int = 50,
    alpha: float = ALPHA_GRANGER_PADRAO,
) -> dict:
    """
    Causalidade de Granger pairwise entre os indicadores anuais.
    Séries curtas (~10 observações) tornam o teste exploratório.
    """
    dados = _dados()
    try:
        serie = construir_matriz_indicadores(dados)
        granger = causalidade_granger(serie, max_lag=max_lag, alpha=alpha)
    except ValueError as exc:
        raise DadosIndisponiveisError(str(exc)) from exc

    if apenas_significantes:
        granger = granger.query("significante").reset_index(drop=True)

    total_significantes = int(granger["significante"].sum())
    logger.info(
        "Granger servido",
        extra={"max_lag": max_lag, "significantes": total_significantes},
    )
    return {
        "max_lag": max_lag,
        "alpha": alpha,
        "total_pares": len(granger),
        "total_significantes": total_significantes,
        "pares": _registros(granger.head(limite)),
    }


def obter_anomalias(limite: int = 50) -> dict:
    """
    Pontos anômalos (Isolation Forest) no painel RA x ano de roubos e na
    série mensal de violência contra idosos, do mais extremo ao menos extremo.
    """
    dados = _dados()

    df_painel = dados.get(TABELA_PAINEL)
    if df_painel is None or df_painel.empty:
        raise DadosIndisponiveisError(f"A tabela '{TABELA_PAINEL}' não está materializada.")

    painel_marcado = detectar_anomalias_painel(
        df_painel.drop(columns=["inserido_em"], errors="ignore"),
        INDICADOR_PAINEL,
    )
    anomalias_painel = resumo_anomalias(painel_marcado, ("regiao_administrativa",))

    anomalias_mensal = []
    df_mensal = dados.get(TABELA_MENSAL)
    if df_mensal is not None and not df_mensal.empty:
        mensal_marcado = detectar_anomalias(df_mensal, INDICADOR_MENSAL)
        anomalias_mensal = resumo_anomalias(mensal_marcado)

    logger.info(
        "Anomalias servidas",
        extra={"painel": len(anomalias_painel), "mensal": len(anomalias_mensal)},
    )
    return {
        "total_painel": int(len(anomalias_painel)),
        "total_mensal": int(len(anomalias_mensal)),
        "painel": _registros(anomalias_painel.head(limite)),
        "mensal": _registros(anomalias_mensal.head(limite)) if len(anomalias_mensal) else [],
    }


def obter_zonas_quentes(tamanho_celula_km: float = 1.5, top_n: int = 20) -> dict:
    """
    Células da malha com mais ocorrências patrimoniais no último ano da
    tabela gold (valores distribuídos para o centróide de cada RA).
    """
    dados = _dados()
    df = dados.get(TABELA_PAINEL)
    if df is None or df.empty:
        raise DadosIndisponiveisError(f"A tabela '{TABELA_PAINEL}' não está materializada.")

    agregado = gerar_agregado_celulas(df, INDICADOR_PAINEL, tamanho_celula_km=tamanho_celula_km)
    zonas = agregado.sort_values(INDICADOR_PAINEL, ascending=False).head(top_n)

    logger.info(
        "Zonas quentes servidas",
        extra={"celulas": len(agregado), "tamanho_km": tamanho_celula_km},
    )
    return {
        "ano_referencia": int(df["ano"].max()),
        "tamanho_celula_km": tamanho_celula_km,
        "celulas_com_ocorrencias": int((agregado[INDICADOR_PAINEL] > 0).sum()),
        "zonas": _registros(zonas),
    }
