# dashboard/visualizacoes.py
"""
Funções puras de visualização do dashboard.

Transformam os payloads JSON da API em `DataFrame`s do pandas e
figuras do Plotly. Não dependem do Streamlit, o que permite testes
unitários diretos sem servidor.
"""

from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.graph_objects as go

COLUNA_ANO_PREFERIDA = "ano"
COLUNA_REGIAO = "regiao_administrativa"

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
        template="plotly_white",
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
        template="plotly_white",
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
        template="plotly_white",
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
            marker_color="firebrick",
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
        template="plotly_white",
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
        template="plotly_white",
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
        template="plotly_white",
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
