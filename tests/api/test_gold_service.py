import pandas as pd
import pytest
from unittest.mock import patch

from api.services import gold_service


def test_listar_tabelas_gold_marca_disponiveis():
    with patch(
        "api.services.gold_service.listar_tabelas",
        return_value=["violencia_contra_mulher_gold", "outra_tabela_qualquer"],
    ):
        resultado = gold_service.listar_tabelas_gold()

    assert resultado["total"] == len(gold_service.TABELAS_GOLD)
    info = {t["nome"]: t for t in resultado["tabelas"]}
    assert info["violencia_contra_mulher_gold"]["disponivel_no_banco"] is True
    assert info["crimes_letais_gold"]["disponivel_no_banco"] is False


def test_listar_tabelas_gold_banco_indisponivel_nao_quebra():
    with patch(
        "api.services.gold_service.listar_tabelas", side_effect=Exception("sem conexão")
    ):
        resultado = gold_service.listar_tabelas_gold()

    assert resultado["total"] == len(gold_service.TABELAS_GOLD)
    assert all(t["disponivel_no_banco"] is False for t in resultado["tabelas"])


def test_obter_resumo_tabela_invalida():
    with pytest.raises(gold_service.TabelaInvalidaError):
        gold_service.obter_resumo_tabela("tabela_inexistente")


def test_obter_resumo_tabela_sucesso():
    resumo_fake = {
        "tabela": "violencia_contra_mulher_gold",
        "linhas": 10,
        "colunas": 3,
        "nulos_total": 0,
        "colunas_com_nulos": 0,
        "tempo_execucao_s": 0.01,
    }
    with patch("api.services.gold_service.analisar_tabela", return_value=resumo_fake):
        resultado = gold_service.obter_resumo_tabela("violencia_contra_mulher_gold")

    assert resultado == resumo_fake


def test_obter_resumo_tabela_erro_no_banco():
    with patch(
        "api.services.gold_service.analisar_tabela", side_effect=Exception("timeout")
    ):
        with pytest.raises(gold_service.TabelaNaoEncontradaError):
            gold_service.obter_resumo_tabela("violencia_contra_mulher_gold")


def test_obter_dados_tabela_invalida():
    with pytest.raises(gold_service.TabelaInvalidaError):
        gold_service.obter_dados_tabela("tabela_inexistente")


def test_obter_dados_tabela_nao_materializada():
    with patch("api.services.gold_service.Repository.load", return_value=None):
        with pytest.raises(gold_service.TabelaNaoEncontradaError):
            gold_service.obter_dados_tabela("violencia_contra_mulher_gold")


def test_obter_dados_tabela_paginacao_e_filtros():
    df = pd.DataFrame(
        {
            "ano": [2020, 2021, 2022, 2023],
            "regiao_administrativa": ["CEILANDIA", "CEILANDIA", "TAGUATINGA", "CEILANDIA"],
            "crimes_contra_mulher": [10, 12, 5, 8],
        }
    )

    with patch("api.services.gold_service.Repository.load", return_value=df):
        resultado = gold_service.obter_dados_tabela(
            "violencia_contra_mulher_gold",
            pagina=1,
            tamanho_pagina=2,
            ano_min=2021,
            regiao_administrativa="ceilandia",
        )

    assert resultado["total_linhas"] == 2  # 2021 e 2023, CEILANDIA
    assert resultado["tamanho_pagina"] == 2
    assert resultado["pagina"] == 1
    assert len(resultado["registros"]) == 2
    anos = {r["ano"] for r in resultado["registros"]}
    assert anos == {2021, 2023}


def test_obter_dados_tabela_pagina_fora_do_intervalo_e_ajustada():
    df = pd.DataFrame({"ano": [2020, 2021, 2022], "crimes_contra_mulher": [1, 2, 3]})

    with patch("api.services.gold_service.Repository.load", return_value=df):
        resultado = gold_service.obter_dados_tabela(
            "violencia_contra_mulher_gold", pagina=99, tamanho_pagina=10
        )

    assert resultado["pagina"] == 1
    assert resultado["total_paginas"] == 1
    assert len(resultado["registros"]) == 3


def test_obter_dados_tabela_filtro_somente_ano_max():
    df = pd.DataFrame({"ano": [2020, 2021, 2022], "crimes_contra_mulher": [1, 2, 3]})

    with patch("api.services.gold_service.Repository.load", return_value=df):
        resultado = gold_service.obter_dados_tabela(
            "violencia_contra_mulher_gold", ano_max=2021
        )

    anos = {r["ano"] for r in resultado["registros"]}
    assert anos == {2020, 2021}


def test_obter_dados_tabela_sem_filtros_retorna_tudo():
    df = pd.DataFrame({"ano": [2020, 2021], "crimes_contra_mulher": [1, 2]})

    with patch("api.services.gold_service.Repository.load", return_value=df):
        resultado = gold_service.obter_dados_tabela("violencia_contra_mulher_gold")

    assert resultado["total_linhas"] == 2


def test_obter_dados_tabela_tabela_sem_coluna_de_ano_ignora_filtro_de_ano():
    """
    Tabelas fora de COLUNA_ANO_POR_TABELA (ex.: violencia_idosos_gold) não
    têm coluna de ano mapeada -> o filtro ano_min/ano_max deve ser
    simplesmente ignorado, sem erro.
    """
    df = pd.DataFrame({"regiao_administrativa": ["CEILANDIA", "GAMA"], "valor": [1, 2]})

    with patch("api.services.gold_service.Repository.load", return_value=df):
        resultado = gold_service.obter_dados_tabela(
            "violencia_idosos_gold", ano_min=2020, ano_max=2021
        )

    assert resultado["total_linhas"] == 2
