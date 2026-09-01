# tests/integracao/test_repository_integracao.py
"""
Integração do repositório (database.repository.repository) com um
PostgreSQL real: FULL REFRESH, carga/leitura, listagem, análise e resumo
das tabelas materializadas no banco de verdade.
"""

import pandas as pd
import pytest

from database.repository import repository as repo

pytestmark = pytest.mark.integracao

TABELA = "crimes_letais_gold"


def _df_crimes():
    return pd.DataFrame(
        {
            "ano": [2020, 2021],
            "regiao_administrativa": ["Taguatinga", "Ceilândia"],
            "crimes": [10, 20],
        }
    )


def test_conectar_banco_consulta_banco_real():
    engine = repo.conectar_banco()

    with engine.connect() as conn:
        resultado = conn.exec_driver_sql("SELECT 1").scalar()

    assert resultado == 1


def test_inserir_e_carregar_tabela_no_banco_real():
    repo.inserir_dados(_df_crimes(), TABELA)

    df = repo.carregar_tabela(TABELA)

    assert df is not None
    assert len(df) == 2
    assert set(df["regiao_administrativa"]) == {"Taguatinga", "Ceilândia"}
    assert list(df["crimes"].sort_values()) == [10, 20]
    # inserir_dados adiciona controle temporal UTC
    assert pd.api.types.is_datetime64_any_dtype(df["inserido_em"])


def test_inserir_dados_full_refresh_recria_tabela():
    repo.inserir_dados(_df_crimes(), TABELA)
    repo.inserir_dados(_df_crimes().iloc[:1], TABELA)

    df = repo.carregar_tabela(TABELA)

    assert len(df) == 1
    assert df.iloc[0]["regiao_administrativa"] == "Taguatinga"


def test_listar_tabelas_inclui_tabela_inserida():
    repo.inserir_dados(_df_crimes(), TABELA)

    tabelas = repo.listar_tabelas()

    assert TABELA in tabelas


def test_analisar_tabela_retorna_metricas_reais():
    repo.inserir_dados(_df_crimes(), TABELA)

    resumo = repo.analisar_tabela(TABELA)

    assert resumo["tabela"] == TABELA
    assert resumo["linhas"] == 2
    assert resumo["colunas"] >= 3
    assert resumo["colunas_com_nulos"] == 0
    assert resumo["nulos_total"] == 0
    assert resumo["tempo_execucao_s"] >= 0


def test_carregar_tabela_inexistente_retorna_none():
    assert repo.carregar_tabela("tabela_que_nao_existe_xyz") is None


def test_resumo_tabelas_inclui_tabela_real():
    repo.inserir_dados(_df_crimes(), TABELA)

    resumo = repo.resumo_tabelas()

    assert TABELA in set(resumo["tabela"])
    linha = resumo[resumo["tabela"] == TABELA].iloc[0]
    assert linha["linhas"] == 2
    assert linha["colunas"] >= 3


CASOS_TIPOS = pytest.mark.parametrize(
    "valores",
    [
        [10, -3, 42],
        [1.5, -0.25, 99.0],
        ["Taguatinga", "Ceilândia/DF", "São Sebastião"],
        [True, False, True],
        [None, 7, 10.5],
        ["texto", None, "fim"],
        pd.to_datetime(["2021-05-01", "2022-12-31", "2020-01-01"]).tolist(),
    ],
    ids=["inteiro", "float", "unicode", "booleano", "nulo_numerico", "nulo_texto", "data"],
)


@CASOS_TIPOS
def test_inserir_dados_roundtrip_de_tipos_no_banco_real(valores):
    df = pd.DataFrame({"chave": [1, 2, 3], "valor": valores})

    repo.inserir_dados(df, TABELA)
    df_carregado = repo.carregar_tabela(TABELA)

    assert list(df_carregado["chave"]) == [1, 2, 3]
    # NULL do banco volta como NaN no pandas (semântica SQL/pandas)
    carregado = [None if pd.isna(v) else v for v in df_carregado["valor"].tolist()]
    assert carregado == valores


def test_inserir_dados_suporta_volume_acima_do_chunksize():
    n = 5_001
    df = pd.DataFrame(
        {
            "ano": list(range(n)),
            "regiao_administrativa": [f"RA-{i % 31}" for i in range(n)],
            "ocorrencia_homicidio": [i % 7 for i in range(n)],
        }
    )

    repo.inserir_dados(df, TABELA)
    df_carregado = repo.carregar_tabela(TABELA)

    assert len(df_carregado) == n
    assert df_carregado["ocorrencia_homicidio"].sum() == sum(i % 7 for i in range(n))


def test_inserir_dados_dataframe_vazio_cria_tabela_sem_linhas():
    df = pd.DataFrame({"ano": pd.Series(dtype="int64"), "regiao_administrativa": pd.Series(dtype="object")})

    repo.inserir_dados(df, TABELA)
    df_carregado = repo.carregar_tabela(TABELA)

    assert df_carregado is not None
    assert len(df_carregado) == 0
    assert {"ano", "regiao_administrativa"} <= set(df_carregado.columns)


def test_inserir_dados_preserva_ordem_de_colunas():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})

    repo.inserir_dados(df, TABELA)
    df_carregado = repo.carregar_tabela(TABELA)

    assert list(df_carregado.columns) == ["a", "b", "c", "inserido_em"]


def test_analisar_tabela_detecta_nulos_reais():
    df = pd.DataFrame({"a": [1, None, 3], "b": ["x", "y", None]})

    repo.inserir_dados(df, TABELA)
    resumo = repo.analisar_tabela(TABELA)

    assert resumo["nulos_total"] == 2
    assert resumo["colunas_com_nulos"] == 2


def test_conectar_banco_retorna_singleton():
    primeiro = repo.conectar_banco()
    segundo = repo.conectar_banco()

    assert primeiro is segundo


def test_inserir_dados_nome_invalido_nao_cria_tabela():
    repo.inserir_dados(_df_crimes(), "tabela inválida com espaço")
    repo.inserir_dados(_df_crimes(), "1tabela-invalida")

    assert "tabela inválida com espaço" not in repo.listar_tabelas()


def test_carregar_tabela_nome_invalido_retorna_none():
    assert repo.carregar_tabela("tabela;DROP TABLE") is None
    assert repo.carregar_tabela("tabela.invalida") is None