# tests/integracao/test_repository_gold_grade_integracao.py
"""
Matriz de integração do repositório (database.repository.repository) com o
PostgreSQL real, parametrizada pelas 12 tabelas gold: carga/leitura,
FULL REFRESH, análise, resumo, tabelas vazias, nulos, ordem de colunas e
controle temporal UTC.
"""

import pandas as pd
import pytest

from api.config import TABELAS_GOLD
from database.repository import repository as repo
from tests.integracao.dados_gold import df_da_gold, df_vazia

pytestmark = pytest.mark.integracao

TABELAS = list(TABELAS_GOLD)

CASOS_TIPOS = pytest.mark.parametrize(
    "coluna",
    [
        {"inteiro": [10, -3, 42]},
        {"float": [1.5, -0.25, 99.0]},
        {"unicode": ["Taguatinga", "Ceilândia/DF", "São Sebastião"]},
        {"booleano": [True, False, True]},
        {"nulo": [None, 7, 10.5]},
        {"nulo_texto": ["texto", None, "fim"]},
        {"data": pd.to_datetime(["2021-05-01", "2022-12-31", "2020-01-01"]).tolist()},
    ],
)


@pytest.mark.parametrize("tabela", TABELAS)
def test_repo_insert_load_por_tabela(engine, tabela):
    repo.inserir_dados(df_da_gold(tabela), tabela)

    df = repo.carregar_tabela(tabela)

    assert df is not None
    assert len(df) == len(df_da_gold(tabela))
    assert set(df.columns) >= set(df_da_gold(tabela).columns)


@pytest.mark.parametrize("tabela", TABELAS)
def test_repo_full_refresh_por_tabela(engine, tabela):
    repo.inserir_dados(df_da_gold(tabela), tabela)
    repo.inserir_dados(df_da_gold(tabela).iloc[:1], tabela)

    df = repo.carregar_tabela(tabela)

    assert len(df) == 1
    pd.testing.assert_frame_equal(
        df[df_da_gold(tabela).columns].reset_index(drop=True),
        df_da_gold(tabela).iloc[:1].reset_index(drop=True),
    )


@pytest.mark.parametrize("tabela", TABELAS)
def test_repo_analisar_por_tabela(engine, tabela):
    repo.inserir_dados(df_da_gold(tabela), tabela)

    resumo = repo.analisar_tabela(tabela)

    assert resumo["tabela"] == tabela
    assert resumo["linhas"] == len(df_da_gold(tabela))
    assert resumo["nulos_total"] == 0
    assert resumo["colunas_com_nulos"] == 0


@pytest.mark.parametrize("tabela", TABELAS)
def test_repo_resumo_tabelas_por_tabela(engine, tabela):
    repo.inserir_dados(df_da_gold(tabela), tabela)

    resumo = repo.resumo_tabelas()

    linha = resumo[resumo["tabela"] == tabela]
    assert len(linha) == 1
    assert int(linha.iloc[0]["linhas"]) == len(df_da_gold(tabela))


@pytest.mark.parametrize("tabela", TABELAS)
def test_repo_dataframe_vazio_por_tabela(engine, tabela):
    df_vazio = df_vazia(df_da_gold(tabela).columns)

    repo.inserir_dados(df_vazio, tabela)
    df = repo.carregar_tabela(tabela)

    assert len(df) == 0
    assert set(df.columns) >= set(df_da_gold(tabela).columns)


@pytest.mark.parametrize("tabela", TABELAS)
def test_repo_detecta_nulos_por_tabela(engine, tabela):
    df = df_da_gold(tabela).copy()
    coluna = df.columns[0]
    df[coluna] = df[coluna].astype(object)
    df.iat[0, 0] = None

    repo.inserir_dados(df, tabela)
    resumo = repo.analisar_tabela(tabela)

    assert resumo["nulos_total"] == 1
    assert resumo["colunas_com_nulos"] == 1


@pytest.mark.parametrize("tabela", TABELAS)
def test_repo_preserva_ordem_de_colunas_por_tabela(engine, tabela):
    repo.inserir_dados(df_da_gold(tabela), tabela)

    df = repo.carregar_tabela(tabela)

    esperado = list(df_da_gold(tabela).columns) + ["inserido_em"]
    assert list(df.columns) == esperado


@pytest.mark.parametrize("tabela", TABELAS)
def test_repo_inserido_em_utc_timezone_aware_por_tabela(engine, tabela):
    repo.inserir_dados(df_da_gold(tabela), tabela)

    df = repo.carregar_tabela(tabela)

    assert pd.api.types.is_datetime64_any_dtype(df["inserido_em"])
    assert df["inserido_em"].dt.tz is not None


@CASOS_TIPOS
def test_repo_roundtrip_de_tipos_por_grade(engine, coluna):
    nome, valores = next(iter(coluna.items()))
    df = pd.DataFrame({"chave": [1, 2, 3], "valor": valores})

    repo.inserir_dados(df, f"grade_tipos_{nome}")
    df_carregado = repo.carregar_tabela(f"grade_tipos_{nome}")

    assert list(df_carregado["chave"]) == [1, 2, 3]
    carregado = [None if pd.isna(v) else v for v in df_carregado["valor"].tolist()]
    assert carregado == valores


def test_repo_nome_invalido_nao_cria_tabela_grade():
    repo.inserir_dados(df_da_gold(TABELAS[0]), "tabela;com-invalida")

    assert "tabela;com-invalida" not in repo.listar_tabelas()


def test_repo_nome_invalido_carrega_none_grade():
    assert repo.carregar_tabela("tabela.invalida") is None