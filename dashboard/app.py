# dashboard/app.py
"""
Dashboard interativo (Streamlit) de Criminalidade Brasília/DF.

Consome a API FastAPI (`api/`) via `dashboard/api_client.py` e desenha
os gráficos com Plotly (`dashboard/visualizacoes.py`). Execução:

    streamlit run dashboard/app.py

A API deve estar no ar antes (uvicorn api.main:app --reload --port 8000).
"""

import pandas as pd
import streamlit as st

from dashboard.api_client import (
    DEFAULT_BASE_URL,
    ApiError,
    health,
    listar_modelos,
    listar_tabelas,
    obter_dados,
    obter_previsao,
    obter_resumo,
)
from dashboard.visualizacoes import (
    SemDadosParaGraficoError,
    coluna_ano_disponivel,
    colunas_numericas,
    figura_heatmap_ra_ano,
    figura_previsao,
    figura_ranking_ra,
    figura_serie_temporal,
    modelos_para_dataframe,
    previsao_para_dataframe,
    registros_para_dataframe,
)

TITULO = "Criminalidade Brasília/DF — Dashboard"


def _carregar_tabela_completa(base_url: str, tabela: str) -> pd.DataFrame:
    """Busca todas as páginas de uma tabela gold e concatena em um DataFrame."""
    resposta = obter_dados(tabela, pagina=1, tamanho_pagina=1000, base_url=base_url)
    registros = list(resposta.get("registros") or [])
    for pagina in range(2, resposta.get("total_paginas", 1) + 1):
        resposta_pagina = obter_dados(
            tabela, pagina=pagina, tamanho_pagina=1000, base_url=base_url
        )
        registros.extend(resposta_pagina.get("registros") or [])
    return registros_para_dataframe(registros)


def _colunas_valor(df: pd.DataFrame) -> list:
    """Colunas numéricas utilizáveis como 'coluna de valor' nos gráficos."""
    coluna_ano = coluna_ano_disponivel(df)
    return [c for c in colunas_numericas(df) if c != coluna_ano]


def _aba_series(base_url: str) -> None:
    st.subheader("Séries Temporais")
    tabelas = [t["nome"] for t in listar_tabelas(base_url)]
    if not tabelas:
        st.warning("Nenhuma tabela gold encontrada na API.")
        return

    tabela = st.selectbox("Tabela gold", tabelas, key="serie_tabela")
    df = _carregar_tabela_completa(base_url, tabela)
    if df.empty:
        st.info("A tabela selecionada ainda não foi materializada no banco.")
        return

    colunas_valor = _colunas_valor(df)
    if not colunas_valor:
        st.info("A tabela selecionada não possui colunas numéricas para série temporal.")
        return

    coluna_valor = st.selectbox("Coluna (indicador)", colunas_valor, key="serie_coluna")
    agrupar = st.checkbox("Uma linha por Região Administrativa", value=True, key="serie_agrupar")

    try:
        fig = figura_serie_temporal(df, coluna_valor, agrupar_regiao=agrupar)
        st.plotly_chart(fig, use_container_width=True)
    except SemDadosParaGraficoError as exc:
        st.warning(str(exc))


def _aba_mapa(base_url: str) -> None:
    st.subheader("Mapa de Calor por RA")
    tabelas = [t["nome"] for t in listar_tabelas(base_url)]
    if not tabelas:
        st.warning("Nenhuma tabela gold encontrada na API.")
        return

    tabela = st.selectbox("Tabela gold", tabelas, key="mapa_tabela")
    df = _carregar_tabela_completa(base_url, tabela)
    if df.empty:
        st.info("A tabela selecionada ainda não foi materializada no banco.")
        return

    colunas_valor = _colunas_valor(df)
    if not colunas_valor:
        st.info("A tabela selecionada não possui colunas numéricas para o mapa de calor.")
        return
    coluna_valor = st.selectbox("Coluna (indicador)", colunas_valor, key="mapa_coluna")

    try:
        st.plotly_chart(figura_heatmap_ra_ano(df, coluna_valor), use_container_width=True)
    except SemDadosParaGraficoError as exc:
        st.warning(str(exc))

    if "regiao_administrativa" in df.columns:
        anos = sorted(df["ano"].dropna().unique()) if "ano" in df.columns else []
        ano = st.selectbox("Ano para o ranking", [None] + [int(a) for a in anos], key="mapa_ano")
        try:
            st.plotly_chart(figura_ranking_ra(df, coluna_valor, ano=ano), use_container_width=True)
        except SemDadosParaGraficoError as exc:
            st.warning(str(exc))


def _aba_previsoes(base_url: str) -> None:
    st.subheader("Previsão — Crimes contra a Mulher (Prophet + XGBoost)")
    horizonte = st.slider("Horizonte (anos)", min_value=1, max_value=10, value=5, key="prev_horizonte")

    try:
        payload = obter_previsao(horizonte_anos=horizonte, base_url=base_url)
    except ApiError as exc:
        st.error(f"Não foi possível obter a previsão: {exc}")
        return

    metricas = payload.get("metricas_residual") or {}
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Origem", payload.get("tabela_origem"))
    col2.metric("Fonte do modelo", payload.get("fonte_modelo") or "—")
    col3.metric("MAE", metricas.get("mae"))
    col4.metric("RMSE", metricas.get("rmse"))

    if payload.get("modelo_arquivo"):
        st.caption(f"Arquivo de modelo: {payload['modelo_arquivo']}")

    df_previsao = previsao_para_dataframe(payload)
    if df_previsao.empty:
        st.info("A resposta de previsão não contém pontos.")
        return

    st.plotly_chart(figura_previsao(payload), use_container_width=True)
    st.dataframe(df_previsao, use_container_width=True)

    st.divider()
    st.markdown("### Modelos persistidos")
    try:
        modelos = listar_modelos(base_url)
    except ApiError as exc:
        st.error(f"Não foi possível listar os modelos: {exc}")
        return
    if modelos:
        st.dataframe(modelos_para_dataframe(modelos), use_container_width=True)
    else:
        st.caption("Nenhum modelo persistido em models/ ainda.")


def _aba_tabelas(base_url: str) -> None:
    st.subheader("Explorar Tabelas Gold")
    tabelas = [t["nome"] for t in listar_tabelas(base_url)]
    if not tabelas:
        st.warning("Nenhuma tabela gold encontrada na API.")
        return

    tabela = st.selectbox("Tabela gold", tabelas, key="tab_tabela")

    try:
        resumo = obter_resumo(tabela, base_url=base_url)
    except ApiError as exc:
        st.error(str(exc))
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("Linhas", resumo.get("linhas"))
    col2.metric("Colunas", resumo.get("colunas"))
    col3.metric("Valores nulos", resumo.get("nulos_total"))

    df = _carregar_tabela_completa(base_url, tabela)
    if df.empty:
        st.info("A tabela selecionada ainda não foi materializada no banco.")
        return

    if "ano" in df.columns:
        anos = sorted(int(a) for a in df["ano"].dropna().unique())
        if len(anos) > 1:
            filtro = st.slider(
                "Intervalo de anos", min_value=anos[0], max_value=anos[-1],
                value=(anos[0], anos[-1]), key="tab_anos",
            )
            df = df[(df["ano"] >= filtro[0]) & (df["ano"] <= filtro[1])]

    if "regiao_administrativa" in df.columns:
        ras = sorted(df["regiao_administrativa"].dropna().astype(str).unique())
        ra = st.selectbox("Região Administrativa", [None] + ras, key="tab_ra")
        if ra is not None:
            df = df[df["regiao_administrativa"].astype(str) == ra]

    st.dataframe(df, use_container_width=True, height=420)


def main() -> None:
    st.set_page_config(page_title=TITULO, layout="wide", page_icon="📊")
    st.title(TITULO)

    with st.sidebar:
        st.header("Configuração")
        base_url = st.text_input("URL da API", value=DEFAULT_BASE_URL, key="base_url")
        if st.button("Verificar conexão", key="botao_health"):
            try:
                estado = health(base_url)
                st.success(f"API OK — banco: {estado.get('database')}")
            except ApiError as exc:
                st.error(str(exc))

    aba_series, aba_mapa, aba_previsoes, aba_tabelas = st.tabs(
        ["Séries Temporais", "Mapa de Calor", "Previsões", "Tabelas"]
    )

    try:
        with aba_series:
            _aba_series(base_url)
        with aba_mapa:
            _aba_mapa(base_url)
        with aba_previsoes:
            _aba_previsoes(base_url)
        with aba_tabelas:
            _aba_tabelas(base_url)
    except ApiError as exc:
        st.error(f"Falha ao acessar a API em '{base_url}': {exc}")
        st.caption("Confira se a API está no ar (uvicorn api.main:app --reload --port 8000).")


if __name__ == "__main__":
    main()
