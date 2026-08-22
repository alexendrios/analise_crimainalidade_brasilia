import pandas as pd
import plotly.graph_objects as go
import pytest

from dashboard.visualizacoes import (
    COLUNAS_IDADES,
    SemDadosParaGraficoError,
    classificacao_para_dataframe,
    coluna_ano_disponivel,
    colunas_categoricas,
    colunas_numericas,
    colunas_valor_indicadores,
    figura_desaparecidos_localizados,
    figura_desaparecidos_por_idade,
    figura_desaparecidos_por_ra,
    figura_desaparecidos_por_sexo,
    figura_heatmap_probabilidade,
    figura_heatmap_ra_ano,
    figura_historico_idades,
    figura_previsao,
    figura_ranking_probabilidade,
    figura_ranking_ra,
    figura_serie_temporal,
    figura_serie_temporal_categorica,
    matriz_confusao_para_dataframe,
    modelos_para_dataframe,
    odds_ratios_para_dataframe,
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


def test_colunas_valor_indicadores_exclui_idades():
    df = pd.DataFrame(
        {"ano": [2020], "idade_vitima": [30], "idade_autor": [35], "crimes": [5]}
    )
    assert colunas_valor_indicadores(df) == ["ano", "crimes"]


def test_figura_historico_idades_gera_dois_traces():
    df = pd.DataFrame(
        {
            "idade_vitima": [0, 25, 30, 40, 45],
            "idade_autor": [0, 30, 35, 50, 0],
        }
    )
    fig = figura_historico_idades(df)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 2
    nomes = {t.name for t in fig.data}
    assert nomes == {"Idade da vítima", "Idade do autor (suspeito)"}


def test_figura_historico_idades_descarta_idade_zero():
    df = pd.DataFrame({"idade_vitima": [0, 0, 0], "idade_autor": [0, 0, 0]})
    with pytest.raises(SemDadosParaGraficoError, match="idades válidas"):
        figura_historico_idades(df)


def test_figura_historico_idades_sem_colunas_levanta_erro():
    with pytest.raises(SemDadosParaGraficoError, match="colunas de idade"):
        figura_historico_idades(pd.DataFrame({"ano": [2020]}))


def test_figura_historico_idades_apenas_coluna_de_vitima():
    df = pd.DataFrame({"idade_vitima": [25, 30], "idade_autor": [0, 0]})
    fig = figura_historico_idades(df)
    assert len(fig.data) == 1
    assert fig.data[0].name == "Idade da vítima"


def test_colunas_categoricas_identifica_nao_numericas():
    df = pd.DataFrame(
        {
            "ano": [2020, 2020],
            "regiao_administrativa": ["Taguatinga", "Ceilândia"],
            "meio_utilizado": ["ARMA DE FOGO", "FISICA"],
            "motivacao": ["CIUME", "CIUME"],
            "crimes": [1, 2],
        }
    )
    assert colunas_categoricas(df) == ["meio_utilizado", "motivacao"]


def test_figura_serie_temporal_categorica_conta_por_ano_e_categoria():
    df = pd.DataFrame(
        {
            "ano": [2020, 2020, 2021, 2021, 2021],
            "meio_utilizado": ["ARMA DE FOGO", "FISICA", "ARMA DE FOGO", "ARMA DE FOGO", "FISICA"],
        }
    )
    fig = figura_serie_temporal_categorica(df, "meio_utilizado")
    assert isinstance(fig, go.Figure)
    assert {t.name for t in fig.data} == {"ARMA DE FOGO", "FISICA"}
    trace_arma = next(t for t in fig.data if t.name == "ARMA DE FOGO")
    assert list(trace_arma.y) == [1, 2]  # contagem por ano (2020, 2021)


def test_figura_serie_temporal_categorica_filtra_ra():
    df = pd.DataFrame(
        {
            "ano": [2020, 2020, 2021],
            "regiao_administrativa": ["Taguatinga", "Ceilândia", "Ceilândia"],
            "motivacao": ["CIUME", "DISCUSSAO", "DISCUSSAO"],
        }
    )
    fig = figura_serie_temporal_categorica(df, "motivacao", ras=["Ceilândia"])
    nomes = [t.name for t in fig.data]
    assert "CIUME" not in nomes
    assert "DISCUSSAO" in nomes


def test_figura_serie_temporal_categorica_sem_ano_levanta_erro():
    with pytest.raises(SemDadosParaGraficoError, match="coluna de ano"):
        figura_serie_temporal_categorica(pd.DataFrame({"meio_utilizado": ["x"]}), "meio_utilizado")


def test_figura_serie_temporal_categorica_sem_coluna_levanta_erro():
    with pytest.raises(SemDadosParaGraficoError, match="coluna categórica"):
        figura_serie_temporal_categorica(pd.DataFrame({"ano": [2020]}), "motivacao")


def test_figura_serie_temporal_categorica_sem_registros_levanta_erro():
    with pytest.raises(SemDadosParaGraficoError, match="categoria preenchida"):
        figura_serie_temporal_categorica(pd.DataFrame({"ano": [2020], "motivacao": [None]}), "motivacao")


def test_figura_serie_temporal_categorica_media_movel_dobra_traces():
    df = pd.DataFrame(
        {
            "ano": [2020, 2020, 2021, 2021],
            "meio_utilizado": ["ARMA DE FOGO", "ARMA DE FOGO", "FISICA", "FISICA"],
        }
    )
    fig = figura_serie_temporal_categorica(df, "meio_utilizado", janela_media_movel=2)
    assert len(fig.data) == 4  # 2 categorias + 2 médias móveis
    assert any("média móvel" in t.name for t in fig.data)


# =========================================================
# Classificação — criminalidade letal por RA (Regressão Logística)
# =========================================================

def _payload_classificacao():
    return {
        "classificacoes": [
            {"regiao_administrativa": "Taguatinga", "ano": 2024,
             "classe_prevista": 1, "rotulo_previsto": "alta", "probabilidade_alta": 0.92},
            {"regiao_administrativa": "Ceilândia", "ano": 2024,
             "classe_prevista": 0, "rotulo_previsto": "baixa", "probabilidade_alta": 0.21},
            {"regiao_administrativa": "Taguatinga", "ano": 2023,
             "classe_prevista": 1, "rotulo_previsto": "alta", "probabilidade_alta": 0.88},
            {"regiao_administrativa": "Ceilândia", "ano": 2023,
             "classe_prevista": 0, "rotulo_previsto": "baixa", "probabilidade_alta": 0.35},
        ],
        "odds_ratios": {
            "ano_num": 0.88,
            "taxa_homicidio": 199.4,
            "log_populacao": 0.9,
        },
        "matriz_confusao": [[40, 2], [1, 42]],
        "limiar_taxa_mediana": 10.66,
    }


def test_classificacao_para_dataframe_vazio():
    assert classificacao_para_dataframe({"classificacoes": []}).empty
    assert classificacao_para_dataframe({}).empty


def test_classificacao_para_dataframe_converte_registros():
    df = classificacao_para_dataframe(_payload_classificacao())
    assert list(df.columns) == [
        "regiao_administrativa", "ano", "classe_prevista",
        "rotulo_previsto", "probabilidade_alta",
    ]
    assert len(df) == 4


def test_figura_ranking_probabilidade_usa_ultimo_ano_por_padrao():
    fig = figura_ranking_probabilidade(_payload_classificacao())

    assert isinstance(fig, go.Figure)
    assert len(fig.data[0].y) == 2  # apenas o ano mais recente (2024)
    assert set(fig.data[0].y) == {"Taguatinga", "Ceilândia"}
    # ordenado crescente para barras horizontais: menor probabilidade embaixo
    assert list(fig.data[0].x) == sorted(fig.data[0].x)


def test_figura_ranking_probabilidade_com_ano_especifico():
    fig = figura_ranking_probabilidade(_payload_classificacao(), ano=2023)

    assert len(fig.data[0].y) == 2


def test_figura_ranking_probabilidade_sem_dados_levanta_erro():
    with pytest.raises(SemDadosParaGraficoError, match="não contém registros"):
        figura_ranking_probabilidade({})

    with pytest.raises(SemDadosParaGraficoError, match="ano 1999"):
        figura_ranking_probabilidade(_payload_classificacao(), ano=1999)


def test_figura_heatmap_probabilidade_monta_matriz_ra_ano():
    fig = figura_heatmap_probabilidade(_payload_classificacao())

    assert isinstance(fig, go.Figure)
    z = fig.data[0].z
    assert len(z) == 2  # 2 RAs
    assert all(len(linha) == 2 for linha in z)  # 2 anos
    assert list(fig.data[0].x) == [2023, 2024]


def test_figura_heatmap_probabilidade_sem_dados_levanta_erro():
    with pytest.raises(SemDadosParaGraficoError, match="não contém registros"):
        figura_heatmap_probabilidade({})


def test_odds_ratios_para_dataframe_ordenado_e_rotulado():
    df = odds_ratios_para_dataframe(_payload_classificacao())

    assert list(df.columns) == ["Indicador", "Odds ratio"]
    valores = list(df["Odds ratio"])
    assert valores == sorted(valores, reverse=True)
    assert df.iloc[0]["Indicador"] == "Taxa de homicídio"


def test_odds_ratios_para_dataframe_vazio():
    assert odds_ratios_para_dataframe({}).empty


def test_matriz_confusao_para_dataframe_rotulada():
    df = matriz_confusao_para_dataframe(_payload_classificacao())

    assert list(df.columns) == ["previsto: baixa", "previsto: alta"]
    assert list(df.index) == ["real: baixa", "real: alta"]
    assert df.iloc[0, 0] == 40 and df.iloc[1, 1] == 42


def test_matriz_confusao_malformada_levanta_erro():
    with pytest.raises(SemDadosParaGraficoError, match="formato inesperado"):
        matriz_confusao_para_dataframe({"matriz_confusao": [[1, 2, 3], [4, 5, 6]]})

    with pytest.raises(SemDadosParaGraficoError, match="formato inesperado"):
        matriz_confusao_para_dataframe({})


def _df_desaparecidos_idade_sexo():
    return pd.DataFrame(
        {
            "ano": [2020, 2020, 2020, 2020],
            "faixa_etaria": ["18 A 29 ANOS", "0 A 17 ANOS", "18 A 29 ANOS", "0 A 17 ANOS"],
            "sexo": ["MASCULINO", "MASCULINO", "FEMININO", "FEMININO"],
            "quantidade": [10, 6, 8, 4],
        }
    )


def _df_desaparecidos_localizados():
    return pd.DataFrame(
        {
            "ano": [2021, 2021],
            "faixa_etaria": ["0 A 17 ANOS", "0 A 17 ANOS"],
            "status": ["AINDA DESAPARECIDOS", "LOCALIZADOS"],
            "quantidade": [12, 30],
        }
    )


def _df_desaparecidos_regiao():
    return pd.DataFrame(
        {
            "regiao_administrativa": ["Taguatinga", "Ceilândia"],
            "ocorrencias_2020": [50, 100],
            "ocorrencias_2021": [60, 120],
        }
    )


def test_figura_desaparecidos_por_sexo_soma_por_categoria():
    fig = figura_desaparecidos_por_sexo(_df_desaparecidos_idade_sexo())

    assert isinstance(fig, go.Figure)
    assert list(fig.data[0].x) == ["FEMININO", "MASCULINO"]
    assert list(fig.data[0].y) == [12, 16]


def test_figura_desaparecidos_por_idade_ordena_faixas():
    fig = figura_desaparecidos_por_idade(_df_desaparecidos_idade_sexo())

    assert list(fig.data[0].x) == ["0 A 17 ANOS", "18 A 29 ANOS"]
    assert list(fig.data[0].y) == [10, 18]


def test_figura_desaparecidos_por_idade_sem_numero_vai_para_o_fim():
    df = pd.DataFrame(
        {"faixa_etaria": ["NÃO INFORMADO", "0 A 17 ANOS"], "quantidade": [3, 5]}
    )
    fig = figura_desaparecidos_por_idade(df)

    assert list(fig.data[0].x) == ["0 A 17 ANOS", "NÃO INFORMADO"]


def test_figura_desaparecidos_localizados_compara_status():
    fig = figura_desaparecidos_localizados(_df_desaparecidos_localizados())

    assert sorted(fig.data[0].x) == ["AINDA DESAPARECIDOS", "LOCALIZADOS"]
    assert sorted(fig.data[0].y) == [12, 30]


def test_figura_desaparecidos_por_ra_agrupa_os_dois_anos():
    fig = figura_desaparecidos_por_ra(_df_desaparecidos_regiao())

    assert len(fig.data) == 2
    nomes = {trace.name for trace in fig.data}
    assert nomes == {"2020", "2021"}
    assert list(fig.data[1].y) == ["Taguatinga", "Ceilândia"]
    assert list(fig.data[1].x) == [60, 120]


@pytest.mark.parametrize(
    "figura",
    [
        figura_desaparecidos_por_sexo,
        figura_desaparecidos_por_idade,
        figura_desaparecidos_localizados,
        figura_desaparecidos_por_ra,
    ],
)
def test_figuras_desaparecidos_sem_colunas_levanta_erro(figura):
    with pytest.raises(SemDadosParaGraficoError):
        figura(pd.DataFrame({"outra": [1]}))
