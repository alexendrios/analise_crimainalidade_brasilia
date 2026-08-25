# dashboard/contexto_ia.py
"""
Montagem do contexto de dados (texto estruturado) que alimenta o LLM.

Cada seção é opcional: falhas da API degradam a seção com aviso explícito,
sem interromper o restante do contexto enviado ao modelo.
"""

from typing import List

from dashboard.api_client import (
    ApiError,
    listar_tabelas,
    obter_anomalias,
    obter_classificacao,
    obter_correlacoes,
    obter_granger,
    obter_previsao,
    obter_resumo,
    obter_zonas_quentes,
)

TABELAS_RESUMO_IGNORADAS = {
    "identificacao_crimes_contra_mulher_gold",
    "desaparecidos_idade_sexo_gold",
    "desaparecidos_localizados_gold",
}
MAX_ITENS_SECAO = 5


def _secao(titulo: str, linhas: List[str]) -> str:
    corpo = "\n".join(linhas) if linhas else "Indisponível no momento."
    return f"## {titulo}\n{corpo}"


def _secao_tabelas(base_url: str) -> str:
    try:
        tabelas = [t["nome"] for t in listar_tabelas(base_url)]
    except ApiError:
        return _secao("Tabelas gold disponíveis", [])

    linhas: List[str] = []
    for nome in tabelas:
        if nome in TABELAS_RESUMO_IGNORADAS:
            continue
        try:
            resumo = obter_resumo(nome, base_url)
            linhas.append(
                f"- {nome}: {resumo.get('linhas', '?')} linhas, "
                f"{resumo.get('colunas', '?')} colunas, "
                f"{resumo.get('nulos_total', 0)} nulos"
            )
        except ApiError:
            linhas.append(f"- {nome}: resumo indisponível")
    return _secao("Tabelas gold disponíveis", linhas)


def _secao_correlacoes(base_url: str) -> str:
    try:
        corr = obter_correlacoes(base_url=base_url)
    except ApiError:
        return _secao("Correlações entre indicadores", [])

    periodo = corr.get("periodo")
    cabecalho = (
        f"Método {corr.get('metodo', 'pearson')}"
        + (f", período {periodo[0]}–{periodo[1]}" if periodo else "")
        + ":"
    )
    pares = [
        f"- {par.get('indicador_a')} × {par.get('indicador_b')}: "
        f"r = {par.get('correlacao'):+.2f}"
        for par in (corr.get("pares_destaque") or [])[:MAX_ITENS_SECAO]
    ]
    insights = [f"- {insight}" for insight in (corr.get("insights") or [])[:MAX_ITENS_SECAO]]
    return _secao("Correlações entre indicadores", [cabecalho] + pares + insights)


def _secao_granger(base_url: str) -> str:
    try:
        granger = obter_granger(base_url=base_url)
    except ApiError:
        return _secao("Causalidade de Granger (indicadores anuais)", [])

    linhas = [
        f"Pares significantes: {granger.get('total_significantes', 0)} "
        f"de {granger.get('total_pares', 0)} (alpha {granger.get('alpha', 0.05)})."
    ]
    linhas += [
        f"- {par.get('origem')} → {par.get('destino')} "
        f"(lag {par.get('melhor_lag')}, p = {par.get('p_valor')})"
        for par in (granger.get("pares") or [])[:MAX_ITENS_SECAO]
        if par.get("significante")
    ]
    return _secao("Causalidade de Granger (indicadores anuais)", linhas)


def _secao_anomalias(base_url: str) -> str:
    try:
        anomalias = obter_anomalias(base_url=base_url)
    except ApiError:
        return _secao("Anomalias (Isolation Forest)", [])

    linhas = [
        f"Casos anômalos no painel RA × ano: {anomalias.get('total_painel', 0)}; "
        f"na série mensal de violência contra idosos: {anomalias.get('total_mensal', 0)}."
    ]
    linhas += [
        f"- {caso.get('regiao_administrativa', 'RA ?')} em {caso.get('ano', '?')}: "
        f"ocorrências destacadas como anômalas"
        for caso in (anomalias.get("painel") or [])[:MAX_ITENS_SECAO]
    ]
    return _secao("Anomalias (Isolation Forest)", linhas)


def _secao_zonas_quentes(base_url: str) -> str:
    try:
        zonas = obter_zonas_quentes(base_url=base_url)
    except ApiError:
        return _secao("Zonas quentes (roubo a pedestre por célula)", [])

    ano = zonas.get("ano_referencia")
    linhas = [
        f"Células de {(zonas.get('tamanho_celula_km', 1.5))} km com mais ocorrências"
        + (f" em {ano}" if ano is not None else "") + ":"
    ]
    linhas += [
        f"- {zona.get('celula_id')}: {zona.get('ocorrencia_roubo_pedestre')} ocorrências"
        for zona in (zonas.get("zonas") or [])[:MAX_ITENS_SECAO]
    ]
    return _secao("Zonas quentes (roubo a pedestre por célula)", linhas)


def _secao_classificacao(base_url: str) -> str:
    try:
        classificacao = obter_classificacao(base_url=base_url)
    except ApiError:
        return _secao("Classificação de criminalidade letal por RA", [])

    metricas = classificacao.get("metricas") or {}
    linhas = []
    if metricas:
        linhas.append(
            f"Regressão Logística — ROC-AUC médio (CV): "
            f"{metricas.get('cv_roc_auc_media', '?')}; F1 holdout: "
            f"{metricas.get('holdout_f1', '?')}."
        )
    distribuicao = classificacao.get("distribuicao_real")
    if distribuicao:
        linhas.append(f"Distribuição real das classes: {distribuicao}.")
    linhas += [
        f"- {item.get('regiao_administrativa')} em {item.get('ano')}: "
        f"classe '{item.get('rotulo_previsto')}' "
        f"(prob. alta {item.get('probabilidade_alta')})"
        for item in (classificacao.get("classificacoes") or [])[:MAX_ITENS_SECAO]
    ]
    return _secao("Classificação de criminalidade letal por RA", linhas)


def _secao_previsao(base_url: str) -> str:
    try:
        previsao = obter_previsao(base_url=base_url)
    except ApiError:
        return _secao("Previsão (crimes contra a mulher)", [])

    fonte = previsao.get("fonte_modelo", "?")
    pontos = [
        f"- {ponto.get('ano')}: valor previsto {ponto.get('valor_previsto'):.1f}"
        for ponto in (previsao.get("previsao") or [])[-3:]
    ]
    return _secao(
        "Previsão (crimes contra a mulher)",
        [f"Fonte do modelo: {fonte}."] + pontos,
    )


def montar_contexto_ia(base_url: str) -> str:
    """
    Reúne as seções de dados da API em um único texto para o LLM.
    Seções indisponíveis aparecem marcadas, nunca derrubam o resumo.
    """
    secoes = [
        _secao_tabelas(base_url),
        _secao_correlacoes(base_url),
        _secao_granger(base_url),
        _secao_anomalias(base_url),
        _secao_zonas_quentes(base_url),
        _secao_classificacao(base_url),
        _secao_previsao(base_url),
    ]
    return "\n\n".join(secoes)
