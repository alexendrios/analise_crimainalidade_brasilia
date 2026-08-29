# dashboard/app.py
"""
Dashboard interativo (Streamlit) de Criminalidade Brasília/DF.

Consome a API FastAPI (`api/`) via `dashboard/api_client.py` e desenha
os gráficos com Plotly (`dashboard/visualizacoes.py`). Execução:

    streamlit run dashboard/app.py

A API deve estar no ar antes (uvicorn api.main:app --reload --port 8000).
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Evita ConnectionResetError (WinError 10054) do _ProactorBasePipeTransport ao
# usar o event loop Proactor padrão do Windows. O Selector loop não sofre desse
# problema quando o cliente derruba a conexão abruptamente.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import pandas as pd
import streamlit as st

from dashboard.api_client import (
    DEFAULT_BASE_URL,
    ApiError,
    health,
    listar_modelos,
    listar_tabelas,
    obter_anomalias,
    obter_classificacao,
    obter_correlacoes,
    obter_dados,
    obter_granger,
    obter_previsao,
    obter_resumo,
    obter_zonas_quentes,
)
from dashboard.contexto_ia import montar_contexto_ia
from dashboard.ia_client import (
    DEFAULT_BASE_URL as OLLAMA_BASE_URL_PADRAO,
    MODELO_PADRAO as OLLAMA_MODELO_PADRAO,
    OllamaError,
    gerar_resumo_ia,
    listar_modelos_ollama,
)
from dashboard.visualizacoes import (
    COLUNA_REGIAO,
    COLUNAS_IDADES,
    SemDadosParaGraficoError,
    anomalias_para_dataframes,
    classificacao_para_dataframe,
    coluna_ano_disponivel,
    colunas_categoricas,
    colunas_numericas,
    estatisticas_descritivas,
    figura_anomalias_mensal,
    figura_anomalias_painel,
    figura_boxplot,
    figura_granger,
    figura_heatmap_correlacoes,
    figura_heatmap_probabilidade,
    figura_heatmap_ra_ano,
    figura_historico_idades,
    figura_idosos_mensal,
    figura_idosos_ocorrencias,
    figura_idosos_por_ra,
    figura_idosos_por_sexo,
    figura_mancha_criminal,
    figura_desaparecidos_localizados,
    figura_desaparecidos_por_idade,
    figura_desaparecidos_por_ra,
    figura_desaparecidos_por_sexo,
    figura_pares_correlacionados,
    figura_previsao,
    figura_ranking_probabilidade,
    figura_ranking_ra,
    figura_serie_temporal,
    figura_serie_temporal_categorica,
    figura_zonas_quentes,
    granger_para_dataframe,
    matriz_confusao_para_dataframe,
    modelos_para_dataframe,
    odds_ratios_para_dataframe,
    outliers_iqr_para_dataframe,
    previsao_para_dataframe,
    registros_para_dataframe,
    rotulo_coluna,
    rotulo_tabela,
    zonas_quentes_para_dataframe,
)

TITULO = "Criminalidade em Brasília/DF — Dashboard Analítico"

TABELAS_EXCLUIDAS_VISAO_GERAL = {
    "identificacao_crimes_contra_mulher_gold",
    "desaparecidos_idade_sexo_gold",
    "desaparecidos_localizados_gold",
}

TABELAS_EXCLUIDAS_SERIES = {
    "violencia_idosos_gold",
    "violencia_idosos_mensais_gold",
    "violencia_idosos_sexo_gold",
    "desaparecidos_regiao_gold",
    "desaparecidos_idade_sexo_gold",
    "desaparecidos_localizados_gold",
}

TABELAS_EXCLUIDAS_MAPA = TABELAS_EXCLUIDAS_SERIES | {
    "identificacao_crimes_contra_mulher_gold",
    "violencia_idosos_ocorrencias_gold",
}

TABELA_IDENTIFICACAO_CRIMES = "identificacao_crimes_contra_mulher_gold"
TABELA_DESAPARECIDOS_IDADE_SEXO = "desaparecidos_idade_sexo_gold"
TABELA_DESAPARECIDOS_LOCALIZADOS = "desaparecidos_localizados_gold"
TABELA_DESAPARECIDOS_REGIAO = "desaparecidos_regiao_gold"
TABELA_IDOSOS_RESUMO = "violencia_idosos_gold"
TABELA_IDOSOS_OCORRENCIAS = "violencia_idosos_ocorrencias_gold"
TABELA_IDOSOS_MENSAIS = "violencia_idosos_mensais_gold"
TABELA_IDOSOS_SEXO = "violencia_idosos_sexo_gold"


@st.cache_data(ttl=600, show_spinner="Carregando dados da API...")
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


def _formatar_numero(valor: float) -> str:
    """Formata um número com separador de milhar no padrão pt-BR."""
    if pd.isna(valor):
        return "—"
    return f"{valor:,.0f}".replace(",", ".")


def _aba_visao_geral(base_url: str) -> None:
    st.subheader("Visão Geral")
    tabelas = [
        t["nome"]
        for t in listar_tabelas(base_url)
        if t["nome"] not in TABELAS_EXCLUIDAS_VISAO_GERAL
    ]
    if not tabelas:
        st.warning("Nenhuma tabela gold encontrada na API.")
        return

    tabela = st.selectbox(
        "Crimes", tabelas, key="vg_tabela", format_func=rotulo_tabela
    )
    df = _carregar_tabela_completa(base_url, tabela)
    if df.empty:
        st.info("A tabela selecionada ainda não foi materializada no banco.")
        return

    colunas = _colunas_valor(df)
    if not colunas:
        st.info("A tabela selecionada não possui indicadores numéricos para resumir.")
        return
    coluna = st.selectbox(
        "Indicador", colunas, key="vg_indicador", format_func=rotulo_coluna
    )

    coluna_ano = coluna_ano_disponivel(df)
    por_ano = (
        df.groupby(coluna_ano)[coluna].sum().sort_index()
        if coluna_ano is not None
        else pd.Series(dtype=float)
    )
    tem_serie = coluna_ano is not None and not por_ano.empty
    ano_recente = int(por_ano.index[-1]) if tem_serie else None

    base_critica = df[df[coluna_ano] == ano_recente] if tem_serie else df
    criticas = (
        base_critica.groupby(COLUNA_REGIAO)[coluna].sum().sort_values(ascending=False)
        if COLUNA_REGIAO in df.columns
        else pd.Series(dtype=float)
    )

    col1, col2, col3, col4 = st.columns(4)

    if tem_serie:
        delta = (
            float(por_ano.iloc[-1] - por_ano.iloc[-2]) if len(por_ano) >= 2 else None
        )
        col1.metric(
            f"{rotulo_coluna(coluna)} em {ano_recente}",
            _formatar_numero(float(por_ano.iloc[-1])),
            delta=delta,
            delta_color="inverse",
            help="Variação versus o ano anterior. Vermelho indica alta da criminalidade.",
        )
        col2.metric("Período coberto", f"{int(por_ano.index[0])}–{ano_recente}")
    else:
        col1.metric(rotulo_coluna(coluna), _formatar_numero(float(df[coluna].sum())))
        col2.metric("Período coberto", "—")

    if not criticas.empty:
        col3.metric(
            "RA mais crítica",
            str(criticas.index[0]),
            help=(
                f"{_formatar_numero(float(criticas.iloc[0]))} registros de "
                f"{str(rotulo_coluna(coluna)).lower()} "
                f"{'em ' + str(ano_recente) if tem_serie else 'no período'}."
            ),
        )
        col4.metric("RAs monitoradas", int(df[COLUNA_REGIAO].nunique()))
    else:
        col3.metric("RA mais crítica", "—")
        col4.metric("RAs monitoradas", "—")

    st.caption(f"Tabela: {rotulo_tabela(tabela)} • Indicador: {rotulo_coluna(coluna)}")

    st.markdown("### Estatísticas descritivas")
    try:
        estat = estatisticas_descritivas(df, coluna)
    except SemDadosParaGraficoError as exc:
        st.warning(str(exc))
        return

    c_media, c_mediana, c_min, c_max, c_dp = st.columns(5)
    c_media.metric("Média", _formatar_numero(estat["media"]))
    c_mediana.metric("Mediana", _formatar_numero(estat["mediana"]))
    c_min.metric("Mínimo", _formatar_numero(estat["minimo"]))
    c_max.metric("Máximo", _formatar_numero(estat["maximo"]))
    c_dp.metric(
        "Desvio padrão",
        _formatar_numero(estat["desvio_padrao"]),
        help="Dispersão dos totais por RA em torno da média.",
    )

    try:
        st.plotly_chart(figura_boxplot(df, coluna), width="stretch")
    except SemDadosParaGraficoError as exc:
        st.warning(str(exc))
        return

    if COLUNA_REGIAO in df.columns:
        outliers = outliers_iqr_para_dataframe(df, coluna)
        if outliers.empty:
            st.info(
                "Nenhuma RA destoante pelo critério de 1,5 × AIQ "
                "(intervalo interquartil) — sem outliers."
            )
        else:
            resumo = ", ".join(
                f"{str(registro[COLUNA_REGIAO]).title()} "
                f"({_formatar_numero(float(registro[coluna]))})"
                for _, registro in outliers.iterrows()
            )
            st.warning(f"Possíveis outliers (além de 1,5 × AIQ): {resumo}.")
        st.caption(
            "Estatísticas e box plot calculados sobre o total do indicador por RA "
            "no período completo; pontos além de 1,5 × AIQ são considerados outliers."
        )
    else:
        st.caption(
            "Estatísticas e box plot calculados sobre os registros da tabela "
            "(a tabela não possui coluna de RA)."
        )




def _aba_resumo_geral(base_url: str) -> None:
    st.subheader("Resumo Geral (IA)")
    st.caption(
        "Síntese executiva gerada por modelo local (Ollama) a partir dos "
        "dados expostos pela API de criminalidade."
    )

    col_url, col_modelo = st.columns([2, 1])
    url_ollama = col_url.text_input(
        "URL do Ollama", value=OLLAMA_BASE_URL_PADRAO, key="ia_url"
    )

    modelos_detectados = listar_modelos_ollama(url_ollama)
    opcoes_modelo = modelos_detectados or [OLLAMA_MODELO_PADRAO]
    modelo = col_modelo.selectbox("Modelo", opcoes_modelo, key="ia_modelo")

    if not st.button("Gerar resumo com IA", key="ia_botao"):
        st.info("Clique em 'Gerar resumo com IA' para produzir a síntese dos dados.")
        return

    with st.spinner("Coletando dados da API e consultando o modelo local..."):
        try:
            contexto = montar_contexto_ia(base_url)
            resposta = gerar_resumo_ia(contexto, base_url=url_ollama, modelo=modelo)
        except (ApiError, OllamaError) as exc:
            st.error(f"Não foi possível gerar o resumo: {exc}")
            return

    st.markdown(resposta)
    st.caption(
        "Conteúdo gerado automaticamente por IA local; confira os números "
        "nas demais abas antes de apoiar decisões."
    )


def _aba_series(base_url: str) -> None:
    st.subheader("Séries Temporais")
    tabelas = [
        t["nome"]
        for t in listar_tabelas(base_url)
        if t["nome"] not in TABELAS_EXCLUIDAS_SERIES
    ]
    if not tabelas:
        st.warning("Nenhuma tabela gold encontrada na API.")
        return

    tabela = st.selectbox("Crimes", tabelas, key="serie_tabela", format_func=rotulo_tabela)
    df = _carregar_tabela_completa(base_url, tabela)
    if df.empty:
        st.info("A tabela selecionada ainda não foi materializada no banco.")
        return

    modo = st.selectbox(
        "Modo de análise",
        ["Indicador numérico", "Contagem por categoria"],
        key="serie_modo",
    )
    categorico = modo == "Contagem por categoria"

    if categorico:
        colunas = colunas_categoricas(df)
        rotulo = "Categoria"
    else:
        colunas = [c for c in _colunas_valor(df) if c not in COLUNAS_IDADES]
        rotulo = "Coluna (indicador)"
    if not colunas:
        if categorico:
            st.info("A tabela selecionada não possui colunas categóricas para série temporal.")
        else:
            st.info("A tabela selecionada não possui colunas numéricas para série temporal.")
        return

    coluna = st.selectbox(rotulo, colunas, key="serie_coluna", format_func=rotulo_coluna)
    if "regiao_administrativa" in df.columns:
        ras_disponiveis = sorted(df["regiao_administrativa"].dropna().astype(str).unique())
        ras = st.multiselect(
            "Comparar RAs", ras_disponiveis, key="serie_ras",
        )
    else:
        ras = []
    janela = st.slider(
        "Média móvel (janela, 1 = desativada)",
        min_value=1, max_value=12, value=1, key="serie_media_movel",
    )

    try:
        if categorico:
            fig = figura_serie_temporal_categorica(
                df, coluna, ras=ras, janela_media_movel=janela
            )
        else:
            fig = figura_serie_temporal(
                df, coluna, ras=ras, janela_media_movel=janela
            )
        st.plotly_chart(fig, width="stretch")
    except SemDadosParaGraficoError as exc:
        st.warning(str(exc))


def _aba_mapa(base_url: str) -> None:
    st.subheader("Mapa de Calor por RA")
    tabelas = [
        t["nome"]
        for t in listar_tabelas(base_url)
        if t["nome"] not in TABELAS_EXCLUIDAS_MAPA
    ]
    if not tabelas:
        st.warning("Nenhuma tabela gold encontrada na API.")
        return

    tabela = st.selectbox("Crimes", tabelas, key="mapa_tabela", format_func=rotulo_tabela)
    df = _carregar_tabela_completa(base_url, tabela)
    if df.empty:
        st.info("A tabela selecionada ainda não foi materializada no banco.")
        return

    colunas_valor = _colunas_valor(df)
    if not colunas_valor:
        st.info("A tabela selecionada não possui colunas numéricas para o mapa de calor.")
        return
    coluna_valor = st.selectbox(
        "Coluna (indicador)", colunas_valor, key="mapa_coluna", format_func=rotulo_coluna
    )

    try:
        st.plotly_chart(figura_heatmap_ra_ano(df, coluna_valor), width="stretch")
    except SemDadosParaGraficoError as exc:
        st.warning(str(exc))

    if "regiao_administrativa" in df.columns:
        anos = sorted(df["ano"].dropna().unique()) if "ano" in df.columns else []
        ano = st.selectbox("Ano para o ranking", [None] + [int(a) for a in anos], key="mapa_ano")
        try:
            st.plotly_chart(figura_ranking_ra(df, coluna_valor, ano=ano), width="stretch")
        except SemDadosParaGraficoError as exc:
            st.warning(str(exc))


def _resumo_idades(df: pd.DataFrame, colunas: list) -> pd.DataFrame:
    """Resumo estatístico das idades válidas (vítima/autor)."""
    linhas = []
    for coluna in colunas:
        valores = pd.to_numeric(df[coluna], errors="coerce").dropna()
        valores = valores[(valores > 0) & (valores <= 120)]
        if valores.empty:
            linhas.append({"Atributo": rotulo_coluna(coluna), "Registros válidos": 0})
            continue
        linhas.append(
            {
                "Atributo": rotulo_coluna(coluna),
                "Registros válidos": int(valores.count()),
                "Média": round(float(valores.mean()), 1),
                "Mediana": float(valores.median()),
                "Mínimo": int(valores.min()),
                "Máximo": int(valores.max()),
            }
        )
    return pd.DataFrame(linhas)


def _aba_idades(base_url: str) -> None:
    st.subheader("Idades — Vítima × Autor (suspeito)")
    df = _carregar_tabela_completa(base_url, TABELA_IDENTIFICACAO_CRIMES)
    if df.empty:
        st.info("A tabela selecionada ainda não foi materializada no banco.")
        return

    colunas_idade = [col for col in COLUNAS_IDADES if col in df.columns]
    if not colunas_idade:
        st.info(
            "A tabela selecionada não possui as colunas de idade "
            "(idade_vitima / idade_autor)."
        )
        return

    bin_size = st.slider(
        "Largura dos bins (anos)", min_value=1, max_value=10, value=5, key="idades_bin"
    )

    try:
        st.plotly_chart(figura_historico_idades(df, bin_size=bin_size), width="stretch")
    except SemDadosParaGraficoError as exc:
        st.warning(str(exc))

    st.markdown("### Resumo")
    st.dataframe(_resumo_idades(df, colunas_idade), width="stretch")


def _aba_mancha_criminal(base_url: str) -> None:
    st.subheader("Mancha Criminal por RA")
    tabelas = [
        t["nome"]
        for t in listar_tabelas(base_url)
        if t["nome"] not in TABELAS_EXCLUIDAS_MAPA
    ]
    if not tabelas:
        st.warning("Nenhuma tabela gold encontrada na API.")
        return

    tabela = st.selectbox("Crimes", tabelas, key="mancha_tabela", format_func=rotulo_tabela)
    df = _carregar_tabela_completa(base_url, tabela)
    if df.empty:
        st.info("A tabela selecionada ainda não foi materializada no banco.")
        return

    colunas_valor = _colunas_valor(df)
    if not colunas_valor:
        st.info("A tabela selecionada não possui colunas numéricas para a mancha criminal.")
        return
    coluna_valor = st.selectbox(
        "Coluna (indicador)", colunas_valor, key="mancha_coluna", format_func=rotulo_coluna
    )

    ano = None
    if "ano" in df.columns:
        anos = sorted(int(a) for a in df["ano"].dropna().unique())
        ano = st.selectbox(
            "Recorte temporal",
            [None] + list(reversed(anos)),
            index=1 if anos else 0,
            format_func=lambda a: (
                "Todo o período"
                if a is None
                else f"{a} (mais recente)" if a == max(anos) else str(a)
            ),
            key="mancha_ano",
            help="Por padrão mostra o ano mais recente; escolha 'Todo o período' para somar todos os anos.",
        )

    try:
        st.plotly_chart(figura_mancha_criminal(df, coluna_valor, ano=ano), width="stretch")
    except SemDadosParaGraficoError as exc:
        st.warning(str(exc))
        return

    ranking = (
        df[df["ano"] == ano] if (ano is not None and "ano" in df.columns) else df
    )
    if COLUNA_REGIAO in ranking.columns and not ranking.empty:
        top_ras = (
            ranking.groupby(COLUNA_REGIAO)[coluna_valor]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )
        st.markdown("### RAs mais críticas")
        colunas_ranking = st.columns(min(5, len(top_ras)))
        for slot, (ra, total) in zip(colunas_ranking, top_ras.items()):
            slot.metric(str(ra).title(), _formatar_numero(float(total)))

    st.caption(
        "Densidade calculada sobre os centróides aproximados das Regiões "
        "Administrativas (WGS-84); quanto mais intensa a mancha, maior a "
        "concentração de ocorrências."
    )


def _aba_desaparecidos(base_url: str) -> None:
    st.subheader("Desaparecidos")
    df_idade_sexo = _carregar_tabela_completa(base_url, TABELA_DESAPARECIDOS_IDADE_SEXO)
    df_localizados = _carregar_tabela_completa(base_url, TABELA_DESAPARECIDOS_LOCALIZADOS)
    df_regiao = _carregar_tabela_completa(base_url, TABELA_DESAPARECIDOS_REGIAO)

    graficos = [
        (df_idade_sexo, figura_desaparecidos_por_sexo),
        (df_idade_sexo, figura_desaparecidos_por_idade),
        (df_localizados, figura_desaparecidos_localizados),
        (df_regiao, figura_desaparecidos_por_ra),
    ]
    if all(df.empty for df, _ in graficos):
        st.info("Nenhuma tabela de desaparecidos foi materializada no banco.")
        return

    linha1 = st.columns(2)
    linha2 = st.columns(2)
    slots = [linha1[0], linha1[1], linha2[0], linha2[1]]
    for slot, (df, figura) in zip(slots, graficos):
        with slot:
            if df.empty:
                st.info("A tabela ainda não foi materializada no banco.")
                continue
            try:
                st.plotly_chart(figura(df), width="stretch")
            except SemDadosParaGraficoError as exc:
                st.warning(str(exc))


def _aba_violencia_idosos(base_url: str) -> None:
    st.subheader("Violência contra Idosos")
    df_resumo = _carregar_tabela_completa(base_url, TABELA_IDOSOS_RESUMO)
    df_ocorrencias = _carregar_tabela_completa(base_url, TABELA_IDOSOS_OCORRENCIAS)
    df_mensal = _carregar_tabela_completa(base_url, TABELA_IDOSOS_MENSAIS)
    df_sexo = _carregar_tabela_completa(base_url, TABELA_IDOSOS_SEXO)

    graficos = [
        (df_resumo, figura_idosos_por_ra),
        (df_ocorrencias, figura_idosos_ocorrencias),
        (df_mensal, figura_idosos_mensal),
        (df_sexo, figura_idosos_por_sexo),
    ]
    if all(df.empty for df, _ in graficos):
        st.info("Nenhuma tabela de violência contra idosos foi materializada no banco.")
        return

    linha1 = st.columns(2)
    linha2 = st.columns(2)
    slots = [linha1[0], linha1[1], linha2[0], linha2[1]]
    for slot, (df, figura) in zip(slots, graficos):
        with slot:
            if df.empty:
                st.info("A tabela ainda não foi materializada no banco.")
                continue
            try:
                st.plotly_chart(figura(df), width="stretch")
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

    st.plotly_chart(figura_previsao(payload), width="stretch")
    st.dataframe(df_previsao, width="stretch")

    st.divider()
    st.markdown("### Modelos persistidos")
    try:
        modelos = listar_modelos(base_url)
    except ApiError as exc:
        st.error(f"Não foi possível listar os modelos: {exc}")
        return
    if modelos:
        st.dataframe(modelos_para_dataframe(modelos), width="stretch")
    else:
        st.caption("Nenhum modelo persistido em models/ ainda.")


def _aba_classificacao(base_url: str) -> None:
    st.subheader("Classificação — Criminalidade Letal por RA (Regressão Logística)")

    try:
        payload = obter_classificacao(base_url=base_url)
    except ApiError as exc:
        st.error(f"Não foi possível obter a classificação: {exc}")
        return

    periodo = payload.get("periodo") or []
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Fonte do modelo", payload.get("fonte_modelo") or "—")
    col2.metric("Registros analisados", payload.get("total_registros"))
    col3.metric("Regiões administrativas", payload.get("total_ras"))
    col4.metric(
        "Limiar (taxa/100 mil)",
        round(payload.get("limiar_taxa_mediana"), 2) if payload.get("limiar_taxa_mediana") else None,
    )

    detalhes = [f"Período: {periodo[0]}–{periodo[-1]}" if len(periodo) == 2 else ""]
    if payload.get("modelo_arquivo"):
        detalhes.append(f"Arquivo de modelo: {payload['modelo_arquivo']}")
    legenda = " • ".join(d for d in detalhes if d)
    if legenda:
        st.caption(legenda)

    df_class = classificacao_para_dataframe(payload)
    if df_class.empty:
        st.info("A resposta de classificação não contém classificações.")
        return

    anos = sorted(int(a) for a in df_class["ano"].dropna().unique())
    ano = st.selectbox(
        "Ano do ranking", list(reversed(anos)), key="classe_ano",
        format_func=lambda a: f"{a} (mais recente)" if anos and a == max(anos) else str(a),
    )

    try:
        st.plotly_chart(figura_ranking_probabilidade(payload, ano=ano), width="stretch")
    except SemDadosParaGraficoError as exc:
        st.warning(str(exc))

    try:
        st.plotly_chart(figura_heatmap_probabilidade(payload), width="stretch")
    except SemDadosParaGraficoError as exc:
        st.warning(str(exc))

    st.markdown("### Classificações por RA e ano")
    st.dataframe(df_class.rename(columns=rotulo_coluna), width="stretch", height=420)

    st.divider()
    st.markdown("### Avaliação do modelo")
    metricas = payload.get("metricas") or {}
    auc_media = metricas.get("cv_roc_auc_media")
    auc_std = metricas.get("cv_roc_auc_std")
    roc_holdout = metricas.get("holdout_roc_auc")
    f1_holdout = metricas.get("holdout_f1")
    col1, col2, col3 = st.columns(3)
    col1.metric(
        "CV ROC-AUC",
        f"{auc_media:.3f} ± {auc_std:.3f}" if auc_media is not None and auc_std is not None else None,
    )
    col2.metric(
        "Holdout ROC-AUC", round(roc_holdout, 3) if roc_holdout is not None else None
    )
    col3.metric("Holdout F1", round(f1_holdout, 3) if f1_holdout is not None else None)

    col_esq, col_dir = st.columns(2)
    with col_esq:
        st.markdown("**Odds ratios (exp coeficientes)**")
        st.dataframe(odds_ratios_para_dataframe(payload), width="stretch")
    with col_dir:
        st.markdown("**Matriz de confusão**")
        try:
            st.dataframe(matriz_confusao_para_dataframe(payload), width="stretch")
        except SemDadosParaGraficoError as exc:
            st.warning(str(exc))


def _aba_analises(base_url: str) -> None:
    st.subheader("Análises Executivas")
    st.caption(
        "Correlações multivariadas, causalidade de Granger, anomalias "
        "(Isolation Forest) e zonas quentes calculadas sobre as tabelas gold."
    )

    aba_corr, aba_granger, aba_anomalias, aba_zonas = st.tabs(
        ["Correlações", "Granger", "Anomalias", "Zonas Quentes"]
    )

    with aba_corr:
        col1, col2 = st.columns(2)
        metodo = col1.selectbox(
            "Método", ["pearson", "spearman"], key="an_corr_metodo"
        )
        top_n = col2.slider(
            "Pares destaque", min_value=3, max_value=15, value=5, key="an_corr_topn"
        )
        try:
            payload = obter_correlacoes(metodo=metodo, top_n=top_n, base_url=base_url)
        except ApiError as exc:
            st.error(f"Não foi possível obter as correlações: {exc}")
            return

        periodo = payload.get("periodo") or []
        met1, met2 = st.columns(2)
        met1.metric(
            "Período consolidado",
            f"{periodo[0]}–{periodo[1]}" if len(periodo) == 2 else "—",
        )
        met2.metric("Indicadores", len(payload.get("indicadores") or []))

        try:
            st.plotly_chart(figura_heatmap_correlacoes(payload), width="stretch")
            st.plotly_chart(figura_pares_correlacionados(payload, top_n=top_n), width="stretch")
        except SemDadosParaGraficoError as exc:
            st.warning(str(exc))

        insights = payload.get("insights") or []
        if insights:
            st.markdown("### Insights")
            for texto in insights:
                st.markdown(f"- {texto}")

    with aba_granger:
        col1, col2 = st.columns(2)
        max_lag = col1.slider(
            "Defasagem máxima (anos)", min_value=1, max_value=3, value=1, key="an_gr_lag"
        )
        apenas_significantes = col2.checkbox(
            "Somente pares significantes (p < 0,05)", value=True, key="an_gr_signif"
        )
        try:
            payload = obter_granger(
                max_lag=max_lag,
                apenas_significantes=apenas_significantes,
                base_url=base_url,
            )
        except ApiError as exc:
            st.error(f"Não foi possível obter a causalidade de Granger: {exc}")
            return

        met1, met2 = st.columns(2)
        met1.metric("Pares retornados", payload.get("total_pares"))
        met2.metric(
            "Pares significantes (total testado)",
            f"{payload.get('total_significantes')} / {payload.get('total_pares')}",
        )

        try:
            st.plotly_chart(figura_granger(payload), width="stretch")
        except SemDadosParaGraficoError as exc:
            st.warning(str(exc))

        df_granger = granger_para_dataframe(payload)
        if df_granger.empty:
            st.info(
                "Nenhum par retornado — ajuste a defasagem ou desmarque o filtro de significância."
            )
        else:
            st.dataframe(df_granger.rename(columns=rotulo_coluna), width="stretch")
        st.caption(
            "Leitura exploratória: as séries anuais são curtas (~10 observações); "
            "Granger indica antecipação temporal, não causalidade definitiva."
        )

    with aba_anomalias:
        try:
            payload = obter_anomalias(base_url=base_url)
        except ApiError as exc:
            st.error(f"Não foi possível obter as anomalias: {exc}")
            return

        met1, met2 = st.columns(2)
        met1.metric("Anomalias no painel RA × ano", payload.get("total_painel"))
        met2.metric("Anomalias na série mensal (idosos)", payload.get("total_mensal"))

        col_esq, col_dir = st.columns(2)
        with col_esq:
            try:
                st.plotly_chart(figura_anomalias_painel(payload), width="stretch")
            except SemDadosParaGraficoError as exc:
                st.warning(str(exc))
        with col_dir:
            try:
                st.plotly_chart(figura_anomalias_mensal(payload), width="stretch")
            except SemDadosParaGraficoError as exc:
                st.warning(str(exc))

        df_painel, df_mensal = anomalias_para_dataframes(payload)
        col_esq, col_dir = st.columns(2)
        with col_esq:
            st.markdown("**Detalhe — painel RA × ano**")
            if df_painel.empty:
                st.caption("Nenhuma anomalia detectada.")
            else:
                st.dataframe(df_painel.rename(columns=rotulo_coluna), width="stretch")
        with col_dir:
            st.markdown("**Detalhe — série mensal (idosos)**")
            if df_mensal.empty:
                st.caption("Nenhuma anomalia detectada.")
            else:
                st.dataframe(df_mensal.rename(columns=rotulo_coluna), width="stretch")

    with aba_zonas:
        col1, col2 = st.columns(2)
        tamanho_celula = col1.slider(
            "Tamanho da célula (km)",
            min_value=0.5, max_value=5.0, value=1.5, step=0.5, key="an_zq_celula",
        )
        top_n = col2.slider(
            "Células no ranking", min_value=5, max_value=50, value=20, key="an_zq_topn"
        )
        try:
            payload = obter_zonas_quentes(
                tamanho_celula_km=tamanho_celula, top_n=top_n, base_url=base_url
            )
        except ApiError as exc:
            st.error(f"Não foi possível obter as zonas quentes: {exc}")
            return

        met1, met2 = st.columns(2)
        met1.metric("Ano de referência", payload.get("ano_referencia"))
        met2.metric(
            "Células com ocorrências",
            _formatar_numero(payload.get("celulas_com_ocorrencias") or 0),
        )

        try:
            st.plotly_chart(figura_zonas_quentes(payload), width="stretch")
        except SemDadosParaGraficoError as exc:
            st.warning(str(exc))

        df_zonas = zonas_quentes_para_dataframe(payload)
        if not df_zonas.empty:
            st.dataframe(df_zonas.rename(columns=rotulo_coluna), width="stretch")
        st.caption(
            "Ocorrências patrimoniais do último ano distribuídas para os centróides "
            "das RAs sobre uma malha regular do DF."
        )


def _aba_tabelas(base_url: str) -> None:
    st.subheader("Explorar Tabelas Gold")
    tabelas = [t["nome"] for t in listar_tabelas(base_url)]
    if not tabelas:
        st.warning("Nenhuma tabela gold encontrada na API.")
        return

    tabela = st.selectbox("Crimes", tabelas, key="tab_tabela", format_func=rotulo_tabela)

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

    st.dataframe(df, width="stretch", height=420)


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

    aba_visao, aba_series, aba_mapa, aba_mancha, aba_identificacao, aba_desaparecidos, aba_idosos, aba_previsoes, aba_classificacao, aba_analises, aba_resumo, aba_tabelas = st.tabs(
        [
            "Visão Geral",
            "Séries Temporais",
            "Mapa de Calor",
            "Mancha Criminal",
            "Identificação crimes",
            "Desaparecidos",
            "Violência contra idosos",
            "Previsões",
            "Classificação",
            "Análises",
            "Resumo Geral",
            "Tabelas",
        ]
    )

    try:
        with aba_visao:
            _aba_visao_geral(base_url)
        with aba_series:
            _aba_series(base_url)
        with aba_mapa:
            _aba_mapa(base_url)
        with aba_mancha:
            _aba_mancha_criminal(base_url)
        with aba_identificacao:
            _aba_idades(base_url)
        with aba_desaparecidos:
            _aba_desaparecidos(base_url)
        with aba_idosos:
            _aba_violencia_idosos(base_url)
        with aba_previsoes:
            _aba_previsoes(base_url)
        with aba_classificacao:
            _aba_classificacao(base_url)
        with aba_analises:
            _aba_analises(base_url)
        with aba_resumo:
            _aba_resumo_geral(base_url)
        with aba_tabelas:
            _aba_tabelas(base_url)
    except ApiError as exc:
        st.error(f"Falha ao acessar a API em '{base_url}': {exc}")
        st.caption("Confira se a API está no ar (uvicorn api.main:app --reload --port 8000).")


if __name__ == "__main__":
    main()
