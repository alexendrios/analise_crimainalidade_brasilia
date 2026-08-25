# dashboard/visualizacoes.py
"""
Funções puras de visualização do dashboard.

Transformam os payloads JSON da API em `DataFrame`s do pandas e
figuras do Plotly. Não dependem do Streamlit, o que permite testes
unitários diretos sem servidor.
"""

import math
import re
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.graph_objects as go

CORES_SEXO = {"masculino": "#5dade2", "feminino": "#ec7063"}
CORES_STATUS = {"localizados": "#58d68d", "ainda desaparecidos": "#e74c3c"}

COLUNA_ANO_PREFERIDA = "ano"
COLUNA_REGIAO = "regiao_administrativa"

TEMA_PLOTLY = "plotly_dark"
COR_FUNDO_TRANSPARENTE = "rgba(0,0,0,0)"
FUNDO_HOVER = "#262730"
TEXTO_HOVER = "#fafafa"

ROTULOS_COLUNAS = {
    "ano": "Ano",
    "regiao_administrativa": "Região administrativa",
    "crimes_contra_mulher": "Crimes contra a mulher",
    "casos_feminicidios": "Casos de feminicídio",
    "idade_vitima": "Idade da vítima",
    "idade_autor": "Idade do autor (suspeito)",
    "meio_utilizado": "Meio utilizado",
    "local": "Local",
    "motivacao": "Motivação",
    "data_do_crime": "Data do crime",
    "probabilidade_alta": "P(alta criminalidade)",
    "classe_prevista": "Classe prevista",
    "rotulo_previsto": "Rótulo previsto",
    # features da Regressão Logística (odds ratios)
    "taxa_homicidio": "Taxa de homicídio",
    "taxa_latrocinio": "Taxa de latrocínio",
    "taxa_lesao_morte": "Taxa de lesão seguida de morte",
    "log_populacao": "Log da população",
    "ano_num": "Ano (numérico)",
    # indicadores consolidados das análises executivas (/analise)
    "violencia_mulher": "Violência contra a mulher",
    "feminicidio": "Feminicídio",
    "roubo_pedestre": "Roubo a pedestre",
    "roubo_comercio": "Roubo a comércio",
    "roubo_transporte": "Roubo no transporte coletivo",
    "roubo_veiculo": "Roubo de veículo",
    "furto_veiculo": "Furto de veículo",
    "homicidio": "Homicídio",
    "latrocinio": "Latrocínio",
    "lesao_morte": "Lesão seguida de morte",
    "racismo": "Racismo",
    "injuria_racial": "Injúria racial",
    # colunas das análises executivas
    "origem": "Origem",
    "destino": "Destino",
    "melhor_lag": "Melhor defasagem (anos)",
    "p_valor": "p-valor",
    "lag_1": "Valor do período anterior",
    "diff_1": "Variação versus período anterior",
    "media_movel_3": "Média móvel (3 períodos)",
}


def rotulo_coluna(coluna: str) -> str:
    """Rótulo amigável (pt-BR) para exibir no dashboard, em vez do nome cru."""
    if coluna in ROTULOS_COLUNAS:
        return ROTULOS_COLUNAS[coluna]
    return coluna.replace("_", " ").capitalize()


ROTULOS_TABELAS = {
    "violencia_contra_mulher_gold": "Violência contra mulher",
    "identificacao_crimes_contra_mulher_gold": "Identificação crimes contra mulher",
    "violencia_idosos_gold": "Violência contra idosos",
    "violencia_idosos_ocorrencias_gold": "Violência contra idosos — ocorrências",
    "violencia_idosos_mensais_gold": "Violência contra idosos — série mensal",
    "violencia_idosos_sexo_gold": "Violência contra idosos — por sexo",
    "crimes_roubo_furto_gold": "Crimes patrimoniais (roubo/furto)",
    "crimes_letais_gold": "Crimes letais",
    "crimes_discriminatorios_gold": "Crimes discriminatórios",
    "desaparecidos_idade_sexo_gold": "Desaparecidos — por idade e sexo",
    "desaparecidos_localizados_gold": "Desaparecidos — localizados",
    "desaparecidos_regiao_gold": "Desaparecidos — por RA",
}


def rotulo_tabela(tabela: str) -> str:
    """Rótulo amigável (pt-BR) de uma tabela gold, em vez do nome cru."""
    return ROTULOS_TABELAS.get(tabela, tabela.replace("_", " ").replace(" gold", "").title())


class SemDadosParaGraficoError(ValueError):
    """Levantada quando os dados não suportam o gráfico solicitado."""


def registros_para_dataframe(registros: List[Dict[str, Any]]) -> pd.DataFrame:
    """Converte a lista de registros da API em DataFrame."""
    return pd.DataFrame(registros or [])


def colunas_numericas(df: pd.DataFrame) -> List[str]:
    """Retorna as colunas numéricas (int/float) do DataFrame."""
    return [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]


def coluna_ano_disponivel(df: pd.DataFrame) -> Optional[str]:
    """Retorna a coluna de ano preferida, se presente no DataFrame."""
    if COLUNA_ANO_PREFERIDA in df.columns:
        return COLUNA_ANO_PREFERIDA
    for col in df.columns:
        if "ano" in str(col).lower():
            return col
    return None


COLUNAS_IDADES = ("idade_vitima", "idade_autor")


def colunas_valor_indicadores(df: pd.DataFrame) -> List[str]:
    """Colunas numéricas utilizáveis como 'indicador', excluindo colunas de idade."""
    return [col for col in colunas_numericas(df) if col not in COLUNAS_IDADES]


def colunas_categoricas(df: pd.DataFrame) -> List[str]:
    """Colunas não numéricas utilizáveis como 'categoria' na série temporal."""
    coluna_ano = coluna_ano_disponivel(df)
    return [
        col
        for col in df.columns
        if col != COLUNA_REGIAO
        and col != coluna_ano
        and not pd.api.types.is_numeric_dtype(df[col])
    ]


def figura_serie_temporal(
    df: pd.DataFrame,
    coluna_valor: str,
    ras: Optional[List[str]] = None,
    janela_media_movel: int = 0,
) -> go.Figure:
    """
    Gráfico de linha da evolução de `coluna_valor` ao longo dos anos.

    Por padrão, desenha o total consolidado (soma de todas as RAs) por
    ano. Quando `ras` é informado, uma linha é sobreposta para cada RA
    selecionada. Se `janela_media_movel > 1`, uma linha suavizada com a
    média móvel da janela é sobreposta a cada série.
    """
    coluna_ano = coluna_ano_disponivel(df)
    if coluna_ano is None:
        raise SemDadosParaGraficoError(
            "Não há coluna de ano disponível para construir a série temporal."
        )

    fig = go.Figure()

    def _adicionar_traces(x, y, nome: str) -> None:
        fig.add_trace(go.Scatter(x=x, y=y, mode="lines+markers", name=nome))
        if janela_media_movel > 1:
            media = pd.Series(y).rolling(janela_media_movel, min_periods=1).mean()
            fig.add_trace(
                go.Scatter(
                    x=x,
                    y=media,
                    mode="lines",
                    name=f"{nome} — média móvel ({janela_media_movel})",
                    line=dict(dash="dot"),
                )
            )

    total = df.groupby(coluna_ano)[coluna_valor].sum().sort_index()
    _adicionar_traces(total.index, total.values, "Total")

    if ras and COLUNA_REGIAO in df.columns:
        for regiao in ras:
            grupo = (
                df[df[COLUNA_REGIAO] == regiao]
                .groupby(coluna_ano)[coluna_valor]
                .sum()
                .sort_index()
            )
            if not grupo.empty:
                _adicionar_traces(grupo.index, grupo.values, str(regiao))

    fig.update_layout(
        title=f"Evolução de {rotulo_coluna(coluna_valor)} por ano",
        xaxis_title=str(coluna_ano),
        yaxis_title=rotulo_coluna(coluna_valor),
        legend_title=COLUNA_REGIAO.capitalize(),
        height=480,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
        hovermode="x unified",
    )
    return fig


def figura_serie_temporal_categorica(
    df: pd.DataFrame,
    coluna_categorica: str,
    ras: Optional[List[str]] = None,
    janela_media_movel: int = 0,
) -> go.Figure:
    """
    Série temporal da contagem de ocorrências por ano, segmentada por categoria.

    Uma linha é desenhada por categoria (ex.: `meio_utilizado`, `motivacao`).
    Quando `janela_media_movel > 1`, a média móvel é sobreposta a cada série.
    """
    coluna_ano = coluna_ano_disponivel(df)
    if coluna_ano is None:
        raise SemDadosParaGraficoError(
            "Não há coluna de ano disponível para construir a série temporal."
        )
    if coluna_categorica not in df.columns:
        raise SemDadosParaGraficoError(
            "A tabela selecionada não possui a coluna categórica solicitada."
        )

    dados = df.copy()
    if ras and COLUNA_REGIAO in dados.columns:
        dados = dados[dados[COLUNA_REGIAO].isin(ras)]
    dados = dados[dados[coluna_categorica].notna()]
    dados[coluna_categorica] = dados[coluna_categorica].astype(str).str.strip()
    dados = dados[dados[coluna_categorica] != ""]

    if dados.empty:
        raise SemDadosParaGraficoError(
            "Não há registros com a categoria preenchida para construir a série temporal."
        )

    tabela = (
        dados.groupby([coluna_ano, coluna_categorica])
        .size()
        .unstack(fill_value=0)
        .sort_index()
    )

    fig = go.Figure()
    for categoria in tabela.columns:
        fig.add_trace(
            go.Scatter(
                x=tabela.index,
                y=tabela[categoria],
                mode="lines+markers",
                name=str(categoria),
            )
        )
        if janela_media_movel > 1:
            media = tabela[categoria].rolling(janela_media_movel, min_periods=1).mean()
            fig.add_trace(
                go.Scatter(
                    x=tabela.index,
                    y=media,
                    mode="lines",
                    name=f"{categoria} — média móvel ({janela_media_movel})",
                    line=dict(dash="dot"),
                )
            )

    fig.update_layout(
        title=f"Ocorrências por ano — {rotulo_coluna(coluna_categorica)}",
        xaxis_title=str(coluna_ano),
        yaxis_title="Número de ocorrências",
        legend_title=rotulo_coluna(coluna_categorica),
        height=480,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
        hovermode="x unified",
    )
    return fig


def figura_heatmap_ra_ano(df: pd.DataFrame, coluna_valor: str) -> go.Figure:
    """Mapa de calor RA × ano da soma de `coluna_valor`."""
    coluna_ano = coluna_ano_disponivel(df)
    if coluna_ano is None or COLUNA_REGIAO not in df.columns:
        raise SemDadosParaGraficoError(
            "Este gráfico exige as colunas 'ano' e 'regiao_administrativa'."
        )

    tabela = (
        df.groupby([COLUNA_REGIAO, coluna_ano], as_index=False)[coluna_valor]
        .sum()
        .pivot(index=COLUNA_REGIAO, columns=coluna_ano, values=coluna_valor)
        .fillna(0)
    )
    tabela = tabela.sort_index()

    fig = go.Figure(
        go.Heatmap(
            z=tabela.values,
            x=[int(v) for v in tabela.columns],
            y=[str(ra) for ra in tabela.index],
            colorscale="YlOrRd",
            hovertemplate="RA: %{y}<br>Ano: %{x}<br>Valor: %{z}<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"Mapa de calor — {rotulo_coluna(coluna_valor)} por RA e ano",
        xaxis_title="Ano",
        yaxis_title="Região Administrativa",
        height=520,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
        hoverlabel=dict(bgcolor=FUNDO_HOVER, font_color=TEXTO_HOVER),
    )
    return fig


def figura_ranking_ra(df: pd.DataFrame, coluna_valor: str, ano: Optional[int] = None) -> go.Figure:
    """Ranking das RAs por soma de `coluna_valor` (opcionalmente em um ano)."""
    if COLUNA_REGIAO not in df.columns:
        raise SemDadosParaGraficoError(
            "Este gráfico exige a coluna 'regiao_administrativa'."
        )

    if ano is not None:
        coluna_ano = coluna_ano_disponivel(df)
        if coluna_ano is None:
            raise SemDadosParaGraficoError("Não há coluna de ano para filtrar o ano solicitado.")
        df = df[df[coluna_ano] == ano]

    ranking = (
        df.groupby(COLUNA_REGIAO, as_index=False)[coluna_valor]
        .sum()
        .sort_values(coluna_valor, ascending=True)
    )

    fig = go.Figure(
        go.Bar(
            x=ranking[coluna_valor],
            y=[str(ra) for ra in ranking[COLUNA_REGIAO]],
            orientation="h",
            marker=dict(
                color=ranking[coluna_valor],
                colorscale="YlOrRd",
                showscale=False,
            ),
            hovertemplate="RA: %{y}<br>Valor: %{x}<extra></extra>",
        )
    )
    titulo = f"Ranking de {rotulo_coluna(coluna_valor)} por RA"
    if ano is not None:
        titulo += f" — ano {ano}"
    fig.update_layout(
        title=titulo,
        xaxis_title=rotulo_coluna(coluna_valor),
        yaxis_title="Região Administrativa",
        height=520,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


def _idades_validas(df: pd.DataFrame, coluna: str) -> pd.Series:
    """Idades válidas (> 0 e até 120 anos) de uma coluna de idade."""
    valores = pd.to_numeric(df[coluna], errors="coerce").dropna()
    return valores[(valores > 0) & (valores <= 120)]


def figura_historico_idades(
    df: pd.DataFrame,
    coluna_vitima: str = "idade_vitima",
    coluna_autor: str = "idade_autor",
    bin_size: int = 5,
) -> go.Figure:
    """Histograma sobreposto das idades da vítima e do autor (suspeito).

    Idades iguais a 0 (preenchidas quando o valor era desconhecido) e
    superiores a 120 anos são descartadas do cálculo.
    """
    existentes = [col for col in (coluna_vitima, coluna_autor) if col in df.columns]
    if not existentes:
        raise SemDadosParaGraficoError(
            "A tabela selecionada não possui as colunas de idade "
            "(idade_vitima / idade_autor)."
        )

    fig = go.Figure()
    for coluna in existentes:
        valores = _idades_validas(df, coluna)
        if valores.empty:
            continue
        fig.add_trace(
            go.Histogram(
                x=valores,
                name=rotulo_coluna(coluna),
                opacity=0.6,
                xbins=dict(size=int(bin_size)),
            )
        )

    if len(fig.data) == 0:
        raise SemDadosParaGraficoError(
            "Não há idades válidas (maiores que 0 e até 120 anos) para construir o histograma."
        )

    fig.update_layout(
        title="Distribuição de idades — vítima × autor (suspeito)",
        xaxis_title="Idade",
        yaxis_title="Número de ocorrências",
        barmode="overlay",
        legend_title="Grupo",
        height=480,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


def previsao_para_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    """Converte a lista de pontos de previsão em DataFrame."""
    return pd.DataFrame(payload.get("previsao") or [])


def figura_previsao(payload: Dict[str, Any]) -> go.Figure:
    """Gráfico de linhas da previsão: valor final, componente Prophet e resíduo."""
    df = previsao_para_dataframe(payload)
    if df.empty:
        raise SemDadosParaGraficoError("A resposta de previsão não contém pontos.")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=df["ano"], y=df["valor_previsto"], mode="lines+markers", name="Valor previsto")
    )
    fig.add_trace(
        go.Scatter(x=df["ano"], y=df["componente_prophet"], mode="lines", name="Componente Prophet")
    )
    fig.add_trace(
        go.Scatter(
            x=df["ano"],
            y=df["residual_log_aplicado"],
            mode="lines",
            name="Resíduo log aplicado",
            yaxis="y2",
        )
    )

    fig.update_layout(
        title=f"Previsão de {payload.get('coluna_alvo', 'crimes_contra_mulher')} "
        f"({payload.get('horizonte_anos', 5)} anos à frente)",
        xaxis_title="Ano",
        yaxis_title="Valor previsto",
        yaxis2=dict(title="Resíduo log", overlaying="y", side="right", showgrid=False),
        height=480,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


def modelos_para_dataframe(modelos: List[Dict[str, Any]]) -> pd.DataFrame:
    """Converte a lista de modelos persistidos em DataFrame resumido."""
    return pd.DataFrame(
        [
            {
                "arquivo": modelo.get("arquivo"),
                "criado_em": modelo.get("criado_em"),
                "tipo_modelo": modelo.get("tipo_modelo"),
                "formato_artefato": modelo.get("formato_artefato"),
                "mae": (modelo.get("metricas") or {}).get("mae"),
                "rmse": (modelo.get("metricas") or {}).get("rmse"),
            }
            for modelo in modelos
        ]
    )


# =========================================================
# Classificação — criminalidade letal por RA (Regressão Logística)
# =========================================================

CORES_CLASSE = {1: "#e74c3c", 0: "#7f8c8d"}


def classificacao_para_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    """Converte a lista de classificações da API em DataFrame."""
    return pd.DataFrame(payload.get("classificacoes") or [])


def figura_ranking_probabilidade(payload: Dict[str, Any], ano: Optional[int] = None) -> go.Figure:
    """
    Ranking das RAs pela probabilidade prevista de alta criminalidade letal
    em `ano` (padrão: o mais recente da resposta). Barras coloridas pela
    classe prevista e linha vertical na fronteira de decisão (p = 0,50).
    """
    df = classificacao_para_dataframe(payload)
    if df.empty:
        raise SemDadosParaGraficoError("A resposta de classificação não contém registros.")

    if ano is None:
        ano = int(df["ano"].max())
    dados = df[df["ano"] == ano]
    if dados.empty:
        raise SemDadosParaGraficoError(f"Não há classificações para o ano {ano}.")

    dados = dados.sort_values("probabilidade_alta", ascending=True)
    cores = [CORES_CLASSE.get(int(c), "#7f8c8d") for c in dados["classe_prevista"]]

    fig = go.Figure(
        go.Bar(
            x=dados["probabilidade_alta"],
            y=[str(ra) for ra in dados["regiao_administrativa"]],
            orientation="h",
            marker_color=cores,
            customdata=list(zip(dados["rotulo_previsto"], dados["classe_prevista"])),
            hovertemplate=(
                "RA: %{y}<br>P(alta): %{x:.3f}"
                "<br>Classe: %{customdata[0]} (%{customdata[1]})"
                "<extra></extra>"
            ),
        )
    )
    fig.add_vline(x=0.5, line_dash="dot", line_color="#95a5a6", annotation_text="fronteira de decisão")
    fig.update_layout(
        title=f"P(alta criminalidade letal) por RA — ano {ano}",
        xaxis_title="P(alta criminalidade)",
        xaxis_range=[0, 1],
        yaxis_title="Região Administrativa",
        height=560,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


def figura_heatmap_probabilidade(payload: Dict[str, Any]) -> go.Figure:
    """Mapa de calor RA × ano da probabilidade prevista de alta criminalidade."""
    df = classificacao_para_dataframe(payload)
    if df.empty:
        raise SemDadosParaGraficoError("A resposta de classificação não contém registros.")

    tabela = (
        df.groupby(["regiao_administrativa", "ano"], as_index=False)["probabilidade_alta"]
        .mean()
        .pivot(index="regiao_administrativa", columns="ano", values="probabilidade_alta")
    )
    tabela = tabela.sort_index()

    fig = go.Figure(
        go.Heatmap(
            z=tabela.values,
            x=[int(v) for v in tabela.columns],
            y=[str(ra) for ra in tabela.index],
            colorscale="RdBu_r",
            zmin=0,
            zmax=1,
            hovertemplate="RA: %{y}<br>Ano: %{x}<br>P(alta): %{z:.3f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Probabilidade de alta criminalidade letal por RA e ano",
        xaxis_title="Ano",
        yaxis_title="Região Administrativa",
        height=520,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
        hoverlabel=dict(bgcolor=FUNDO_HOVER, font_color=TEXTO_HOVER),
    )
    return fig


def odds_ratios_para_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    """Converte os odds ratios do payload em DataFrame ordenado (desc)."""
    ratios = payload.get("odds_ratios") or {}
    return pd.DataFrame(
        [
            {"Indicador": rotulo_coluna(feature), "Odds ratio": round(float(valor), 3)}
            for feature, valor in sorted(ratios.items(), key=lambda item: item[1], reverse=True)
        ]
    )


def matriz_confusao_para_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    """Matriz de confusão 2×2 rotulada como DataFrame; erro se o formato for inesperado."""
    matriz = payload.get("matriz_confusao") or []
    if len(matriz) != 2 or any(len(linha) != 2 for linha in matriz):
        raise SemDadosParaGraficoError("A matriz de confusão da resposta tem formato inesperado.")
    return pd.DataFrame(
        matriz,
        index=["real: baixa", "real: alta"],
        columns=["previsto: baixa", "previsto: alta"],
    )


# =========================================================
# Desaparecidos — gráficos de barra
# =========================================================

def _barra_por_categoria(
    df: pd.DataFrame,
    coluna_categoria: str,
    titulo: str,
    eixo_x: str,
    cores_por_categoria: Optional[Dict[str, str]] = None,
    ordenacao: Optional[List[str]] = None,
) -> go.Figure:
    if coluna_categoria not in df.columns or "quantidade" not in df.columns:
        raise SemDadosParaGraficoError(
            f"A tabela não possui as colunas '{coluna_categoria}' e 'quantidade'."
        )
    dados = df[df[coluna_categoria].notna()].copy()
    dados[coluna_categoria] = dados[coluna_categoria].astype(str).str.strip()
    dados = dados[dados[coluna_categoria] != ""]
    if dados.empty:
        raise SemDadosParaGraficoError("Não há registros com a categoria preenchida.")

    totais = dados.groupby(coluna_categoria)["quantidade"].sum()
    if ordenacao is not None:
        totais = totais.reindex([c for c in ordenacao if c in totais.index])

    categorias = [str(c) for c in totais.index]
    if cores_por_categoria:
        cores = [
            cores_por_categoria.get(categoria.lower(), "#7f8c8d")
            for categoria in categorias
        ]
    else:
        cores = None

    fig = go.Figure(
        go.Bar(x=categorias, y=totais.values, marker_color=cores)
    )
    fig.update_layout(
        title=titulo,
        xaxis_title=eixo_x,
        yaxis_title="Quantidade",
        height=460,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


def _ordem_faixa_etaria(faixas: List[str]) -> List[str]:
    """Ordena faixas etárias pelo número inicial do rótulo; sem número vai para o fim."""
    def chave(faixa: str):
        m = re.match(r"\s*(\d+)", faixa)
        return (0, int(m.group(1))) if m else (1, 0)

    return sorted(faixas, key=chave)


def figura_desaparecidos_por_sexo(df: pd.DataFrame) -> go.Figure:
    """Barra com o total de desaparecidos por sexo."""
    return _barra_por_categoria(
        df,
        "sexo",
        "Desaparecidos por sexo",
        "Sexo",
        cores_por_categoria=CORES_SEXO,
    )


def figura_desaparecidos_por_idade(df: pd.DataFrame) -> go.Figure:
    """Barra com o total de desaparecidos por faixa etária."""
    if "faixa_etaria" not in df.columns:
        raise SemDadosParaGraficoError("A tabela não possui a coluna 'faixa_etaria'.")
    ordem = _ordem_faixa_etaria(
        df["faixa_etaria"].dropna().astype(str).str.strip().unique().tolist()
    )
    return _barra_por_categoria(
        df,
        "faixa_etaria",
        "Desaparecidos por faixa etária",
        "Faixa etária",
        ordenacao=ordem,
    )


def figura_desaparecidos_localizados(df: pd.DataFrame) -> go.Figure:
    """Barra comparando localizados e ainda desaparecidos."""
    return _barra_por_categoria(
        df,
        "status",
        "Localizados × ainda desaparecidos",
        "Status",
        cores_por_categoria=CORES_STATUS,
    )


def _barras_agrupadas_por_ano(
    df: pd.DataFrame,
    colunas_valor: List[str],
    nomes: List[str],
    cores: Optional[List[str]] = None,
) -> go.Figure:
    """Barras agrupadas por ano a partir de uma coluna numérica por série."""
    coluna_ano = coluna_ano_disponivel(df)
    if coluna_ano is None:
        raise SemDadosParaGraficoError(
            "Não há coluna de ano disponível para construir o gráfico."
        )
    ausentes = [c for c in colunas_valor if c not in df.columns]
    if ausentes:
        raise SemDadosParaGraficoError(
            f"A tabela não possui as colunas {', '.join(ausentes)}."
        )

    tabela = df.groupby(coluna_ano)[colunas_valor].sum().sort_index()
    anos = [str(int(a)) for a in tabela.index]

    fig = go.Figure()
    for indice, (coluna, nome) in enumerate(zip(colunas_valor, nomes)):
        cor = cores[indice] if cores else None
        fig.add_trace(
            go.Bar(
                x=anos,
                y=tabela[coluna],
                name=nome,
                marker_color=cor,
            )
        )

    fig.update_layout(
        legend_title="Série",
        height=460,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


def figura_idosos_por_ra(df: pd.DataFrame) -> go.Figure:
    """Barras agrupadas por RA comparando jan–ago de 2016 e 2017."""
    colunas_necessarias = ["regiao_administrativa", "jan_ago_2016", "jan_ago_2017"]
    if any(c not in df.columns for c in colunas_necessarias):
        raise SemDadosParaGraficoError(
            "A tabela exige as colunas 'regiao_administrativa', "
            "'jan_ago_2016' e 'jan_ago_2017'."
        )

    ranking = (
        df.groupby("regiao_administrativa")[["jan_ago_2016", "jan_ago_2017"]]
        .sum()
        .sort_values(["jan_ago_2017", "jan_ago_2016"], ascending=True)
    )

    fig = go.Figure()
    for coluna, nome, cor in [
        ("jan_ago_2016", "2016", "#5dade2"),
        ("jan_ago_2017", "2017", "#e74c3c"),
    ]:
        fig.add_trace(
            go.Bar(
                y=[str(ra) for ra in ranking.index],
                x=ranking[coluna],
                name=nome,
                orientation="h",
                marker_color=cor,
            )
        )

    fig.update_layout(
        title="Violência contra idosos — ocorrências por RA (jan–ago)",
        barmode="group",
        xaxis_title="Ocorrências",
        yaxis_title="Região Administrativa",
        legend_title="Ano",
        height=max(420, 26 * len(ranking)),
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


def figura_idosos_ocorrencias(df: pd.DataFrame) -> go.Figure:
    """Barras agrupadas por ano: total de ocorrências × dentro de casa."""
    fig = _barras_agrupadas_por_ano(
        df,
        ["ocorrencias", "violencia_dentro_de_casa"],
        ["Ocorrências", "Violência dentro de casa"],
        cores=["#e74c3c", "#7f8c8d"],
    )
    fig.update_layout(
        title="Violência contra idosos — ocorrências por ano",
        xaxis_title="Ano",
        yaxis_title="Ocorrências",
    )
    return fig


def figura_idosos_mensal(df: pd.DataFrame) -> go.Figure:
    """Barras da série mensal de fatos registrados contra idosos."""
    colunas_necessarias = ["ano", "mes_num", "fato"]
    if any(c not in df.columns for c in colunas_necessarias):
        raise SemDadosParaGraficoError(
            "A tabela exige as colunas 'ano', 'mes_num' e 'fato'."
        )

    dados = df.sort_values(["ano", "mes_num"]).copy()
    rotulo_mes = (
        dados["mes"].astype(str).str.title() + "/" + dados["ano"].astype(int).astype(str)
        if "mes" in df.columns
        else dados["mes_num"].astype(int).astype(str) + "/" + dados["ano"].astype(int).astype(str)
    )

    series = [("fato", "Fatos", "#e74c3c")]
    if "registro" in df.columns:
        series.append(("registro", "Registros", "#5dade2"))

    fig = go.Figure()
    for coluna, nome, cor in series:
        fig.add_trace(
            go.Bar(x=rotulo_mes.tolist(), y=dados[coluna], name=nome, marker_color=cor)
        )

    fig.update_layout(
        title="Violência contra idosos — série mensal",
        xaxis_title="Mês",
        yaxis_title="Ocorrências",
        barmode="group",
        legend_title="Série",
        height=460,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


def figura_idosos_por_sexo(df: pd.DataFrame) -> go.Figure:
    """Barras agrupadas por ano comparando vítimas do sexo masculino × feminino."""
    fig = _barras_agrupadas_por_ano(
        df,
        ["masculino", "feminino"],
        ["Masculino", "Feminino"],
        cores=[CORES_SEXO["masculino"], CORES_SEXO["feminino"]],
    )
    fig.update_layout(
        title="Violência contra idosos — vítimas por sexo",
        xaxis_title="Ano",
        yaxis_title="Vítimas",
    )
    return fig


def figura_desaparecidos_por_ra(df: pd.DataFrame) -> go.Figure:
    """Barras agrupadas por RA comparando ocorrências de 2020 e 2021."""
    colunas_necessarias = ["regiao_administrativa", "ocorrencias_2020", "ocorrencias_2021"]
    if any(c not in df.columns for c in colunas_necessarias):
        raise SemDadosParaGraficoError(
            "A tabela exige as colunas 'regiao_administrativa', "
            "'ocorrencias_2020' e 'ocorrencias_2021'."
        )

    ranking = (
        df.groupby("regiao_administrativa")[["ocorrencias_2020", "ocorrencias_2021"]]
        .sum()
        .sort_values(["ocorrencias_2021", "ocorrencias_2020"], ascending=True)
    )

    fig = go.Figure()
    for coluna, nome, cor in [
        ("ocorrencias_2020", "2020", "#7f8c8d"),
        ("ocorrencias_2021", "2021", "#e74c3c"),
    ]:
        fig.add_trace(
            go.Bar(
                y=[str(ra) for ra in ranking.index],
                x=ranking[coluna],
                name=nome,
                orientation="h",
                marker_color=cor,
            )
        )

    fig.update_layout(
        title="Desaparecimentos por RA — 2020 × 2021",
        barmode="group",
        legend_title="Ano",
        height=max(420, 26 * len(ranking)),
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


# =========================================================
# Análises executivas — correlações (/analise/correlacoes)
# =========================================================

def correlacoes_para_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    """Converte os pares destaque da resposta de correlações em DataFrame."""
    return pd.DataFrame(payload.get("pares_destaque") or [])


def figura_heatmap_correlacoes(payload: Dict[str, Any]) -> go.Figure:
    """Mapa de calor da matriz de correlação entre os indicadores gold."""
    matriz = payload.get("matriz_correlacao") or {}
    if not matriz:
        raise SemDadosParaGraficoError(
            "A resposta de correlações não contém a matriz de correlação."
        )

    indicadores = list(payload.get("indicadores") or matriz.keys())
    tabela = (
        pd.DataFrame(matriz)
        .reindex(index=indicadores, columns=indicadores)
        .astype(float)
    )

    rotulos = [rotulo_coluna(indicador) for indicador in indicadores]
    fig = go.Figure(
        go.Heatmap(
            z=tabela.values,
            x=rotulos,
            y=rotulos,
            colorscale="RdBu_r",
            zmin=-1,
            zmax=1,
            hovertemplate="%{y} × %{x}<br>Correlação: %{z:.3f}<extra></extra>",
        )
    )

    periodo = payload.get("periodo") or []
    recorte = f" ({periodo[0]}–{periodo[1]})" if len(periodo) == 2 else ""
    fig.update_layout(
        title=f"Correlação entre indicadores — {payload.get('metodo', 'pearson')}{recorte}",
        height=max(420, 34 * len(indicadores)),
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
        hoverlabel=dict(bgcolor=FUNDO_HOVER, font_color=TEXTO_HOVER),
    )
    return fig


def figura_pares_correlacionados(payload: Dict[str, Any], top_n: Optional[int] = None) -> go.Figure:
    """
    Barras horizontais divergentes dos pares mais correlacionados
    (vermelho = positiva, azul = negativa).
    """
    df = correlacoes_para_dataframe(payload)
    if df.empty:
        raise SemDadosParaGraficoError(
            "A resposta de correlações não contém pares destaque."
        )
    if top_n is not None:
        df = df.head(top_n)
    df = df.sort_values("correlacao", ascending=True)

    rotulos = [
        f"{rotulo_coluna(a)} × {rotulo_coluna(b)}"
        for a, b in zip(df["indicador_a"], df["indicador_b"])
    ]
    cores = ["#e74c3c" if valor > 0 else "#5dade2" for valor in df["correlacao"]]

    fig = go.Figure(
        go.Bar(
            x=df["correlacao"],
            y=rotulos,
            orientation="h",
            marker_color=cores,
            hovertemplate="%{y}<br>Correlação: %{x:.3f}<extra></extra>",
        )
    )
    fig.add_vline(x=0, line_color="#95a5a6")
    fig.update_layout(
        title="Pares de indicadores mais correlacionados",
        xaxis_title="Correlação",
        xaxis_range=[-1, 1],
        height=max(380, 40 * len(rotulos)),
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


# =========================================================
# Análises executivas — causalidade de Granger (/analise/granger)
# =========================================================

def granger_para_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    """Converte os pares da resposta de Granger em DataFrame."""
    return pd.DataFrame(payload.get("pares") or [])


def figura_granger(payload: Dict[str, Any]) -> go.Figure:
    """
    Barras horizontais da força da relação de Granger (-log10 do p-valor)
    por par origem → destino, com a linha do limiar de significância.
    """
    df = granger_para_dataframe(payload)
    dados = df.dropna(subset=["p_valor"]).copy() if not df.empty else pd.DataFrame()
    if dados.empty:
        raise SemDadosParaGraficoError("A resposta de Granger não contém pares avaliáveis.")

    alpha = float(payload.get("alpha") or 0.05)
    dados["forca"] = dados["p_valor"].map(lambda p: -math.log10(max(float(p), 1e-12)))
    dados = dados.sort_values("forca", ascending=True)

    rotulos = [
        f"{rotulo_coluna(origem)} → {rotulo_coluna(destino)}"
        for origem, destino in zip(dados["origem"], dados["destino"])
    ]

    fig = go.Figure(
        go.Bar(
            x=dados["forca"],
            y=rotulos,
            orientation="h",
            marker_color="#e74c3c",
            customdata=list(zip(dados["p_valor"], dados["melhor_lag"])),
            hovertemplate=(
                "%{y}<br>p-valor: %{customdata[0]:.4f}"
                "<br>Defasagem: %{customdata[1]} ano(s)"
                "<extra></extra>"
            ),
        )
    )
    fig.add_vline(
        x=-math.log10(alpha),
        line_dash="dot",
        line_color="#95a5a6",
        annotation_text=f"limiar (α = {alpha:g})",
    )
    fig.update_layout(
        title=f"Força da causalidade de Granger (max_lag = {payload.get('max_lag', 1)})",
        xaxis_title="-log10(p-valor)",
        yaxis_title="Par (origem → destino)",
        height=max(380, 40 * len(rotulos)),
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


# =========================================================
# Análises executivas — anomalias Isolation Forest (/analise/anomalias)
# =========================================================

def anomalias_para_dataframes(payload: Dict[str, Any]) -> tuple:
    """Converte as listas de anomalias (painel e mensal) em DataFrames."""
    return (
        pd.DataFrame(payload.get("painel") or []),
        pd.DataFrame(payload.get("mensal") or []),
    )


def figura_anomalias_painel(payload: Dict[str, Any]) -> go.Figure:
    """Dispersão das anomalias do painel RA × ano (roubo a pedestre)."""
    df_painel, _ = anomalias_para_dataframes(payload)
    if df_painel.empty:
        raise SemDadosParaGraficoError("Não há anomalias no painel RA × ano.")

    coluna_valor = next(
        (c for c in df_painel.columns if c.startswith("ocorrencia")),
        None,
    )
    if coluna_valor is None or "ano" not in df_painel.columns:
        raise SemDadosParaGraficoError(
            "As anomalias do painel não possuem as colunas 'ano' e de ocorrências."
        )

    tem_regiao = COLUNA_REGIAO in df_painel.columns
    grupos = (
        df_painel.groupby(COLUNA_REGIAO)
        if tem_regiao
        else [(None, df_painel)]
    )
    fig = go.Figure()
    for ra, grupo in grupos:
        nome_ra = str(ra) if tem_regiao else "Total"
        contexto = list(grupo[COLUNA_REGIAO].astype(str)) if tem_regiao else ["—"] * len(grupo)
        fig.add_trace(
            go.Scatter(
                x=grupo["ano"],
                y=grupo[coluna_valor],
                mode="markers",
                name=nome_ra,
                customdata=contexto,
                hovertemplate=(
                    "RA: %{customdata}<br>Ano: %{x}"
                    f"<br>{rotulo_coluna(coluna_valor)}: %{{y}}<extra></extra>"
                ),
                marker=dict(size=12, color="#e74c3c", symbol="diamond"),
            )
        )

    fig.update_layout(
        title="Anomalias detectadas no painel RA × ano (roubo a pedestre)",
        xaxis_title="Ano",
        yaxis_title=rotulo_coluna(coluna_valor),
        legend_title="Região Administrativa",
        height=480,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


def figura_anomalias_mensal(payload: Dict[str, Any]) -> go.Figure:
    """Barras dos meses anômalos na série mensal de violência contra idosos."""
    _, df_mensal = anomalias_para_dataframes(payload)
    if df_mensal.empty:
        raise SemDadosParaGraficoError("Não há anomalias na série mensal.")
    if "fato" not in df_mensal.columns or "ano" not in df_mensal.columns:
        raise SemDadosParaGraficoError(
            "As anomalias mensais não possuem as colunas 'ano' e 'fato'."
        )

    ordenado = df_mensal.sort_values(["ano", "mes_num"]).copy()
    if "mes" in ordenado.columns:
        rotulos = (
            ordenado["mes"].astype(str).str.title()
            + "/"
            + ordenado["ano"].astype(int).astype(str)
        )
    else:
        rotulos = (
            ordenado["mes_num"].astype(int).astype(str)
            + "/"
            + ordenado["ano"].astype(int).astype(str)
        )

    fig = go.Figure(
        go.Bar(x=rotulos.tolist(), y=ordenado["fato"], marker_color="#e74c3c")
    )
    fig.update_layout(
        title="Meses anômalos — violência contra idosos",
        xaxis_title="Mês",
        yaxis_title="Fatos registrados",
        height=480,
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig


# =========================================================
# Análises executivas — zonas quentes (/analise/zonas-quentes)
# =========================================================

def zonas_quentes_para_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    """Converte as células da resposta de zonas quentes em DataFrame."""
    return pd.DataFrame(payload.get("zonas") or [])


def figura_zonas_quentes(payload: Dict[str, Any]) -> go.Figure:
    """Ranking das células da malha com mais ocorrências patrimoniais."""
    df = zonas_quentes_para_dataframe(payload)
    if df.empty:
        raise SemDadosParaGraficoError("A resposta de zonas quentes não contém células.")

    coluna_valor = next((c for c in df.columns if c.startswith("ocorrencia")), None)
    if coluna_valor is None or "celula_id" not in df.columns:
        raise SemDadosParaGraficoError(
            "As zonas quentes não possuem as colunas 'celula_id' e de ocorrências."
        )

    dados = df.sort_values(coluna_valor, ascending=True)
    ano = payload.get("ano_referencia")

    fig = go.Figure(
        go.Bar(
            y=dados["celula_id"].astype(str),
            x=dados[coluna_valor],
            orientation="h",
            marker=dict(color=dados[coluna_valor], colorscale="YlOrRd", showscale=False),
            hovertemplate="Célula: %{y}<br>Ocorrências: %{x}<extra></extra>",
        )
    )
    recorte = f" — ano {int(ano)}" if ano is not None else ""
    fig.update_layout(
        title=f"Zonas quentes — células com mais ocorrências patrimoniais{recorte}",
        xaxis_title="Ocorrências (roubo a pedestre)",
        yaxis_title="Célula da malha",
        height=max(420, 26 * len(dados)),
        template=TEMA_PLOTLY,
        paper_bgcolor=COR_FUNDO_TRANSPARENTE,
        plot_bgcolor=COR_FUNDO_TRANSPARENTE,
    )
    return fig
