import pandas as pd
import plotly.graph_objects as go
import pytest

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
    rotulo_coluna,
    rotulo_tabela,
)


def _df_base():
    return pd.DataFrame(
        {
            "ano": [2020, 2020, 2021, 2021],
            "regiao_administrativa": ["Taguatinga", "Ceilândia", "Taguatinga", "Ceilândia"],
            "crimes_contra_mulher": [10, 20, 15, 25],
        }
    )


def test_registros_para_dataframe_vazio():
    df = registros_para_dataframe([])
    assert df.empty


def test_registros_para_dataframe_converte_dicts():
    df = registros_para_dataframe([{"ano": 2020, "valor": 1}])
    assert list(df.columns) == ["ano", "valor"]


def test_colunas_numericas_filtra_nao_numericas():
    df = pd.DataFrame({"ano": [2020], "ra": ["x"], "valor": [1.5]})
    assert colunas_numericas(df) == ["ano", "valor"]


def test_coluna_ano_preferida():
    df = pd.DataFrame({"ano": [2020], "ano_fato": [2020]})
    assert coluna_ano_disponivel(df) == "ano"


def test_coluna_ano_fallback_pela_semantica():
    df = pd.DataFrame({"ano_fato": [2020]})
    assert coluna_ano_disponivel(df) == "ano_fato"


def test_coluna_ano_ausente():
    assert coluna_ano_disponivel(pd.DataFrame({"valor": [1]})) is None


def test_rotulo_coluna_mapeia_idade_da_vitima_e_autor():
    assert rotulo_coluna("idade_vitima") == "Idade da vítima"
    assert rotulo_coluna("idade_autor") == "Idade do autor (suspeito)"


def test_rotulo_coluna_fallback_legivel():
    assert rotulo_coluna("crimes_contra_mulher") == "Crimes contra a mulher"
    assert rotulo_coluna("coluna_desconhecida") == "Coluna desconhecida"


def test_rotulo_tabela_mapeia_nome_gold():
    assert rotulo_tabela("identificacao_crimes_contra_mulher_gold") == "Identificação crimes contra mulher"
    assert rotulo_tabela("violencia_contra_mulher_gold") == "Violência contra mulher"


def test_rotulo_tabela_fallback_legivel():
    assert rotulo_tabela("tabela_desconhecida_gold") == "Tabela Desconhecida"


def test_figura_serie_temporal_total_linha_unica():
    fig = figura_serie_temporal(_df_base(), "crimes_contra_mulher")
    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 1  # apenas a linha do total consolidado
    assert fig.data[0].name == "Total"


def test_figura_serie_temporal_compara_ras_selecionadas():
    fig = figura_serie_temporal(_df_base(), "crimes_contra_mulher", ras=["Taguatinga"])
    nomes = [t.name for t in fig.data]
    assert "Total" in nomes
    assert "Taguatinga" in nomes
    assert "Ceilândia" not in nomes


def test_figura_serie_temporal_sem_ano_levanta_erro():
    with pytest.raises(SemDadosParaGraficoError, match="coluna de ano"):
        figura_serie_temporal(pd.DataFrame({"valor": [1]}), "valor")


def test_figura_serie_temporal_media_movel_dobra_traces():
    fig = figura_serie_temporal(
        _df_base(), "crimes_contra_mulher", ras=["Taguatinga", "Ceilândia"], janela_media_movel=3
    )
    assert len(fig.data) == 6  # total + 2 RAs, cada uma com sua média móvel
    nomes = [t.name for t in fig.data]
    assert any("média móvel" in nome for nome in nomes)


def test_figura_serie_temporal_media_movel_janela_um_nao_adiciona():
    fig = figura_serie_temporal(_df_base(), "crimes_contra_mulher", janela_media_movel=1)
    assert len(fig.data) == 1


def test_figura_serie_temporal_media_movel_valor_correto():
    fig = figura_serie_temporal(_df_base(), "crimes_contra_mulher", janela_media_movel=2)
    trace_media = fig.data[1]
    assert list(trace_media.y) == [30.0, 35.0]  # média móvel de [30, 40]


def test_figura_heatmap_ra_ano_generica():
    fig = figura_heatmap_ra_ano(_df_base(), "crimes_contra_mulher")
    assert isinstance(fig.data[0], go.Heatmap)
    assert fig.data[0].z.shape == (2, 2)


def test_figura_heatmap_sem_regiao_levanta_erro():
    df = _df_base().drop(columns=["regiao_administrativa"])
    with pytest.raises(SemDadosParaGraficoError, match="regiao_administrativa"):
        figura_heatmap_ra_ano(df, "crimes_contra_mulher")


def test_figura_ranking_ra_sem_regiao_levanta_erro():
    with pytest.raises(SemDadosParaGraficoError, match="regiao_administrativa"):
        figura_ranking_ra(pd.DataFrame({"valor": [1]}), "valor")


def test_figura_ranking_ra_com_filtro_de_ano():
    fig = figura_ranking_ra(_df_base(), "crimes_contra_mulher", ano=2020)
    assert isinstance(fig.data[0], go.Bar)
    assert "2020" in fig.layout.title.text


def test_figura_ranking_ra_sem_ano_filtra_tudo():
    df = _df_base().drop(columns=["ano"])
    with pytest.raises(SemDadosParaGraficoError, match="coluna de ano"):
        figura_ranking_ra(df, "crimes_contra_mulher", ano=2020)


def test_previsao_para_dataframe_converte_pontos():
    payload = {"previsao": [{"ano": 2027, "valor_previsto": 100.0}]}
    df = previsao_para_dataframe(payload)
    assert df.iloc[0]["ano"] == 2027


def test_figura_previsao_gera_tres_traces():
    payload = {
        "coluna_alvo": "crimes_contra_mulher",
        "horizonte_anos": 3,
        "previsao": [
            {"ano": 2027, "valor_previsto": 100.0, "componente_prophet": 95.0, "residual_log_aplicado": 0.05},
            {"ano": 2028, "valor_previsto": 105.0, "componente_prophet": 98.0, "residual_log_aplicado": 0.07},
        ],
    }
    fig = figura_previsao(payload)
    assert len(fig.data) == 3


def test_figura_previsao_vazia_levanta_erro():
    with pytest.raises(SemDadosParaGraficoError, match="pontos"):
        figura_previsao({"previsao": []})


def test_modelos_para_dataframe_achata_metricas():
    modelos = [
        {
            "arquivo": "x.pkl",
            "criado_em": "2026-01-01T00:00:00",
            "tipo_modelo": "XGBRegressor",
            "formato_artefato": "bundle",
            "metricas": {"mae": 0.1, "rmse": 0.2},
        }
    ]
    df = modelos_para_dataframe(modelos)
    assert df.iloc[0]["mae"] == 0.1
    assert df.iloc[0]["rmse"] == 0.2


def test_modelos_para_dataframe_sem_metricas_nao_quebra():
    df = modelos_para_dataframe([{"arquivo": "x.pkl"}])
    assert df.iloc[0]["mae"] is None
