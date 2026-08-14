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


def _agregar_por_ano(
    df: pd.DataFrame, coluna_valor: str, coluna_ano: Optional[str], agrupar_regiao: bool
) -> pd.DataFrame:
    chaves = [coluna_ano] if coluna_ano else []
    if agrupar_regiao and COLUNA_REGIAO in df.columns:
        chaves.append(COLUNA_REGIAO)
    if not chaves:
        raise SemDadosParaGraficoError(
            "Não há coluna de ano disponível para construir a série temporal."
        )

    agrupado = (
        df.groupby(chaves, as_index=False)[coluna_valor].sum().sort_values(chaves)
    )
    return agrupado


def figura_serie_temporal(
    df: pd.DataFrame,
    coluna_valor: str,
    agrupar_regiao: bool = True,
) -> go.Figure:
    """
    Gráfico de linha da evolução de `coluna_valor` ao longo dos anos.
    Quando `agrupar_regiao=True` e o DataFrame tem a coluna
    `regiao_administrativa`, uma linha é desenhada por RA; caso
    contrário, uma única linha com o total consolidado.
    """
    coluna_ano = coluna_ano_disponivel(df)
    agregado = _agregar_por_ano(df, coluna_valor, coluna_ano, agrupar_regiao)
    eixo_x = coluna_ano if coluna_ano else "indice"

    if COLUNA_REGIAO in agregado.columns and agrupar_regiao:
        fig = go.Figure()
        for regiao, grupo in agregado.groupby(COLUNA_REGIAO):
            fig.add_trace(
                go.Scatter(
                    x=grupo[eixo_x],
                    y=grupo[coluna_valor],
                    mode="lines+markers",
                    name=str(regiao),
                )
            )
    else:
        fig = go.Figure(
            go.Scatter(
                x=agregado[eixo_x],
                y=agregado[coluna_valor],
                mode="lines+markers",
                name=coluna_valor,
            )
        )

    fig.update_layout(
        title=f"Evolução de {coluna_valor} por ano",
        xaxis_title=str(eixo_x),
        yaxis_title=coluna_valor,
        legend_title=COLUNA_REGIAO.capitalize(),
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
        title=f"Mapa de calor — {coluna_valor} por RA e ano",
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
    titulo = f"Ranking de {coluna_valor} por RA"
    if ano is not None:
        titulo += f" — ano {ano}"
    fig.update_layout(
        title=titulo,
        xaxis_title=coluna_valor,
        yaxis_title="Região Administrativa",
        height=520,
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
