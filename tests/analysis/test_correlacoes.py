import numpy as np
import pandas as pd
import pytest

from analysis.correlacoes import (
    construir_matriz_indicadores,
    correlacao_idosos_patrimoniais,
    causalidade_granger,
    insights_correlacao,
    matriz_correlacao,
    pares_mais_correlacionados,
)

INDICADORES_ESPERADOS = {
    "violencia_mulher",
    "feminicidio",
    "roubo_pedestre",
    "roubo_comercio",
    "roubo_transporte",
    "roubo_veiculo",
    "furto_veiculo",
    "homicidio",
    "latrocinio",
    "lesao_morte",
    "racismo",
    "injuria_racial",
}


def test_construir_matriz_indicadores_consolida_as_tabelas(dados_gold):
    matriz = construir_matriz_indicadores(dados_gold)

    assert list(matriz.columns) == sorted(INDICADORES_ESPERADOS)
    assert matriz.index.name == "ano"
    assert len(matriz) == 10  # 2015..2024
    assert not matriz.isna().any().any()


def test_construir_matriz_sem_tabelas_validas_levanta_erro():
    with pytest.raises(ValueError, match="Nenhuma tabela gold"):
        construir_matriz_indicadores({})


def test_tabela_ausente_e_sinalizada_mas_nao_impede(dados_gold):
    dados = dict(dados_gold)
    del dados["crimes_letais_gold"]

    matriz = construir_matriz_indicadores(dados)

    assert "homicidio" not in matriz.columns
    assert "violencia_mulher" in matriz.columns


def test_matriz_correlacao_diagonal_unitaria_e_simetrica(dados_gold):
    correlacao = matriz_correlacao(construir_matriz_indicadores(dados_gold))

    assert (np.diag(correlacao) == 1).all()
    assert np.allclose(correlacao, correlacao.T)
    assert ((correlacao >= -1) & (correlacao <= 1)).all().all()


def test_matriz_correlacao_descarta_colunas_com_poucas_obs(dados_gold):
    matriz = construir_matriz_indicadores(dados_gold)
    matriz["raro"] = [np.nan] * 9 + [1.0]

    correlacao = matriz_correlacao(matriz)

    assert "raro" not in correlacao.columns


def test_pares_mais_correlacionados_ordena_por_modulo(dados_gold):
    correlacao = matriz_correlacao(construir_matriz_indicadores(dados_gold))

    pares = pares_mais_correlacionados(correlacao, top_n=4)

    assert len(pares) == 4
    absolutos = pares["correlacao"].abs().to_numpy()
    assert (absolutos[:-1] >= absolutos[1:]).all()
    # nenhum par contém o mesmo indicador duas vezes
    assert all(linha["indicador_a"] != linha["indicador_b"] for _, linha in pares.iterrows())


def test_granger_produz_grid_completo_com_schema_valido(dados_gold):
    matriz = construir_matriz_indicadores(
        {k: v for k, v in dados_gold.items() if k in ("crimes_letais_gold", "crimes_discriminatorios_gold")}
    )

    granger = causalidade_granger(matriz, max_lag=1)

    n_indicadores = matriz.shape[1]
    assert len(granger) == n_indicadores * (n_indicadores - 1)
    assert set(granger.columns) == {"origem", "destino", "melhor_lag", "p_valor", "significante"}
    assert granger["p_valor"].dropna().between(0, 1).all()
    assert granger["significante"].dtype == bool
    # ordenado por p-valor crescente
    ps = granger["p_valor"].fillna(1.0).to_numpy()
    assert (ps[:-1] <= ps[1:]).all()


def test_granger_exclui_series_constantes(dados_gold):
    matriz = construir_matriz_indicadores({k: v for k, v in dados_gold.items() if k == "crimes_discriminatorios_gold"})
    matriz["constante"] = 7.0

    granger = causalidade_granger(matriz)

    envolvendo_constante = granger.query("origem == 'constante' or destino == 'constante'")
    assert envolvendo_constante.empty


def test_granger_lida_com_falha_no_teste(dados_gold, monkeypatch):
    from analysis import correlacoes as modulo

    def quebrado(*args, **kwargs):
        raise ValueError("falha simulada")

    monkeypatch.setattr(modulo, "grangercausalitytests", quebrado)
    matriz = construir_matriz_indicadores({k: v for k, v in dados_gold.items() if k == "crimes_letais_gold"})

    granger = causalidade_granger(matriz, max_lag=1)

    assert granger["p_valor"].isna().all()
    assert not granger["significante"].any()


def test_correlacao_idosos_patrimonial_cross_section_perfeita():
    idosos = pd.DataFrame(
        {
            "regiao_administrativa": ["Brasília", "Gama", "Taguatinga"],
            "jan_ago_2016": [10, 20, 30],
        }
    )
    patrimonial = pd.DataFrame(
        {
            "regiao_administrativa": ["BRASILIA", "GAMA", "TAGUATINGA"],
            "ano": [2016, 2016, 2016],
            "ocorrencia_roubo_pedestre": [100, 200, 300],
            "ocorrencia_furto_em_veiculo": [1, 2, 3],
        }
    )

    resultado = correlacao_idosos_patrimoniais(idosos, patrimonial, ano_patrimonial=2016)

    assert resultado["n_ra"] == 3
    assert resultado["pearson"] == pytest.approx(1.0)
    assert resultado["spearman"] == pytest.approx(1.0)


def test_correlacao_idosos_patrimonial_normaliza_acentos():
    idosos = pd.DataFrame(
        {
            "regiao_administrativa": ["Aguas Claras", "Gama", "Taguatinga"],
            "jan_ago_2016": [5, 15, 10],
        }
    )
    patrimonial = pd.DataFrame(
        {
            "regiao_administrativa": ["ÁGUAS CLARAS", "GAMA", "TAGUATINGA"],
            "ano": [2016, 2016, 2016],
            "ocorrencia_roubo_pedestre": [50, 150, 100],
        }
    )

    resultado = correlacao_idosos_patrimoniais(idosos, patrimonial)

    assert resultado["n_ra"] == 3
    assert resultado["pearson"] == pytest.approx(1.0)


def test_correlacao_idosos_patrimonial_amostra_minima_retorna_nan():
    idosos = pd.DataFrame(
        {"regiao_administrativa": ["Gama", "Taguatinga"], "jan_ago_2016": [5, 15]}
    )
    patrimonial = pd.DataFrame(
        {
            "regiao_administrativa": ["GAMA", "TAGUATINGA"],
            "ano": [2016, 2016],
            "ocorrencia_roubo_pedestre": [50, 150],
        }
    )

    resultado = correlacao_idosos_patrimoniais(idosos, patrimonial)

    assert resultado["n_ra"] == 2
    assert pd.isna(resultado["pearson"])


def test_correlacao_idosos_patrimonial_coluna_inexistente_levanta_erro():
    idosos = pd.DataFrame({"regiao_administrativa": ["Gama"], "jan_ago_2020": [1]})
    patrimonial = pd.DataFrame({"regiao_administrativa": ["Gama"], "ano": [2016],
                                "ocorrencia_roubo_pedestre": [10]})

    with pytest.raises(ValueError, match="jan_ago_2016"):
        correlacao_idosos_patrimoniais(idosos, patrimonial)


def test_insights_descreve_top_pares(dados_gold):
    correlacao = matriz_correlacao(construir_matriz_indicadores(dados_gold))

    insights = insights_correlacao(correlacao, None, top_n=2)

    assert len(insights) == 2
    assert all(("positiva" in i or "negativa" in i) for i in insights)


def test_insights_avisa_quando_nao_ha_granger_significante():
    correlacao = pd.DataFrame(
        {"a": [1.0, 0.2], "b": [0.2, 1.0]}, index=["a", "b"]
    )
    granger = pd.DataFrame(
        [{"origem": "a", "destino": "b", "melhor_lag": 1, "p_valor": 0.8, "significante": False}]
    )

    insights = insights_correlacao(correlacao, granger)

    assert any("Nenhuma relação de Granger" in i for i in insights)
