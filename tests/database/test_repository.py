import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError

import database.repository.repository as repo

@pytest.fixture(autouse=True)
def reset_engine():
    repo._ENGINE = None

def test_conectar_banco_cria_engine_uma_vez():
    fake_engine = MagicMock()

    with patch("database.repository.repository.obter_engine", return_value=fake_engine):
        engine1 = repo.conectar_banco()
        engine2 = repo.conectar_banco()

    assert engine1 is fake_engine
    assert engine2 is fake_engine

def test_carregar_tabela_sucesso():
    df_mock = pd.DataFrame({"id": [1, 2]})
    engine_mock = MagicMock()

    with (
        patch(
            "database.repository.repository.conectar_banco", return_value=engine_mock
        ),
        patch("database.repository.repository.pd.read_sql", return_value=df_mock),
    ):
        result = repo.carregar_tabela("clientes")

    assert result.equals(df_mock)


def test_carregar_tabela_nome_invalido():
    result = repo.carregar_tabela("clientes-drop")

    assert result is None

def test_carregar_tabela_sqlalchemy_error():
    engine_mock = MagicMock()

    with (
        patch(
            "database.repository.repository.conectar_banco", return_value=engine_mock
        ),
        patch(
            "database.repository.repository.pd.read_sql",
            side_effect=SQLAlchemyError,
        ),
    ):
        result = repo.carregar_tabela("clientes")

    assert result is None

def test_carregar_tabela_exception_generica():
    engine_mock = MagicMock()

    with (
        patch(
            "database.repository.repository.conectar_banco", return_value=engine_mock
        ),
        patch(
            "database.repository.repository.pd.read_sql",
            side_effect=Exception,
        ),
    ):
        result = repo.carregar_tabela("clientes")

    assert result is None

def test_inserir_dados_sucesso():
    df = pd.DataFrame({"id": [1, 2]})
    engine_mock = MagicMock()

    with (
        patch(
            "database.repository.repository.conectar_banco", return_value=engine_mock
        ),
        patch("pandas.DataFrame.to_sql") as to_sql_mock,
    ):  # << patch global
        repo.inserir_dados(df, "clientes")

        # Apenas verificar se foi chamado pelo menos uma vez
        assert to_sql_mock.call_count == 1

        # Você pode checar argumentos mínimos
        args, kwargs = to_sql_mock.call_args
        assert args[0] == "clientes"  # nome da tabela
        assert "index" in kwargs
        assert kwargs["if_exists"] == "replace"
        assert kwargs["method"] == "multi"


def test_inserir_dados_nome_invalido():
    df = pd.DataFrame({"id": [1]})

    with patch("pandas.DataFrame.to_sql") as to_sql_mock:
        try:
            repo.inserir_dados(df, "clientes-drop")
        except ValueError:
            pass

        to_sql_mock.assert_not_called()


def test_inserir_dados_sqlalchemy_error():
    df = pd.DataFrame({"id": [1]})

    with (
        patch("database.repository.repository.conectar_banco"),
        patch("pandas.DataFrame.to_sql", side_effect=SQLAlchemyError),
    ):
        import pytest

        with pytest.raises(SQLAlchemyError):
            repo.inserir_dados(df, "clientes")


def test_inserir_dados_exception_generica_a():
    df = pd.DataFrame({"id": [1]})
    engine_mock = MagicMock()

    with (
        patch(
            "database.repository.repository.conectar_banco", return_value=engine_mock
        ),
        patch.object(
            df,
            "to_sql",
            side_effect=Exception,
        ),
    ):
        repo.inserir_dados(df, "clientes")


def test_inserir_dados_exception_generica_b():
    df = pd.DataFrame({"id": [1]})

    # Mock global do to_sql para lançar uma Exception genérica
    with (
        patch(
            "database.repository.repository.conectar_banco", return_value=MagicMock()
        ),
        patch("pandas.DataFrame.to_sql", side_effect=Exception("Erro genérico")),
    ):
        # Verifica se a Exception genérica é propagada
        with pytest.raises(Exception) as exc_info:
            repo.inserir_dados(df, "clientes")

        assert str(exc_info.value) == "Erro genérico"

# ============================================================
# listar_tabelas
# ============================================================
def test_listar_tabelas_sucesso():
    engine_mock = MagicMock()
    inspector_mock = MagicMock()
    inspector_mock.get_table_names.return_value = ["tabela_a", "tabela_b"]

    with (
        patch("database.repository.repository.obter_engine", return_value=engine_mock),
        patch("database.repository.repository.inspect", return_value=inspector_mock),
    ):
        resultado = repo.listar_tabelas()

    assert resultado == ["tabela_a", "tabela_b"]
    engine_mock.dispose.assert_called_once()


def test_listar_tabelas_erro_propaga_e_fecha_engine():
    engine_mock = MagicMock()

    with (
        patch("database.repository.repository.obter_engine", return_value=engine_mock),
        patch("database.repository.repository.inspect", side_effect=Exception("falha")),
    ):
        with pytest.raises(Exception, match="falha"):
            repo.listar_tabelas()

    engine_mock.dispose.assert_called_once()


def test_listar_tabelas_erro_sem_engine_nao_quebra_finally():
    """Se obter_engine falhar antes de criar o engine, o finally não deve tentar dispose."""
    with patch(
        "database.repository.repository.obter_engine", side_effect=Exception("sem conexão")
    ):
        with pytest.raises(Exception, match="sem conexão"):
            repo.listar_tabelas()


# ============================================================
# analisar_tabela
# ============================================================
def test_analisar_tabela_sucesso():
    df_mock = pd.DataFrame({"a": [1, 2, None], "b": ["x", "y", "z"]})

    with patch("database.repository.repository.carregar_tabela", return_value=df_mock):
        resultado = repo.analisar_tabela("clientes")

    assert resultado["tabela"] == "clientes"
    assert resultado["linhas"] == 3
    assert resultado["colunas"] == 2
    assert resultado["nulos_total"] == 1
    assert resultado["colunas_com_nulos"] == 1
    assert "tempo_execucao_s" in resultado


def test_analisar_tabela_describe_falha_nao_quebra():
    df_mock = pd.DataFrame({"a": [1, 2]})

    with (
        patch("database.repository.repository.carregar_tabela", return_value=df_mock),
        patch.object(pd.DataFrame, "describe", side_effect=Exception("describe falhou")),
    ):
        resultado = repo.analisar_tabela("clientes")

    assert resultado["tabela"] == "clientes"


def test_analisar_tabela_erro_propaga():
    with patch(
        "database.repository.repository.carregar_tabela",
        side_effect=Exception("erro ao carregar"),
    ):
        with pytest.raises(Exception, match="erro ao carregar"):
            repo.analisar_tabela("clientes")


# ============================================================
# resumo_tabelas
# ============================================================
def test_resumo_tabelas_sucesso():
    df_a = pd.DataFrame({"x": [1, 2]})
    df_b = pd.DataFrame({"y": [1, None, 3]})

    with (
        patch(
            "database.repository.repository.listar_tabelas",
            return_value=["tabela_a", "tabela_b"],
        ),
        patch(
            "database.repository.repository.carregar_tabela",
            side_effect=[df_a, df_b],
        ),
    ):
        resultado = repo.resumo_tabelas()

    assert len(resultado) == 2
    assert set(resultado["tabela"]) == {"tabela_a", "tabela_b"}
    linha_b = resultado[resultado["tabela"] == "tabela_b"].iloc[0]
    assert linha_b["valores_nulos_total"] == 1


def test_resumo_tabelas_erro_em_uma_tabela_nao_interrompe_as_demais():
    df_ok = pd.DataFrame({"x": [1]})

    with (
        patch(
            "database.repository.repository.listar_tabelas",
            return_value=["tabela_quebrada", "tabela_ok"],
        ),
        patch(
            "database.repository.repository.carregar_tabela",
            side_effect=[Exception("erro"), df_ok],
        ),
    ):
        resultado = repo.resumo_tabelas()

    assert len(resultado) == 1
    assert resultado.iloc[0]["tabela"] == "tabela_ok"


def test_resumo_tabelas_erro_geral_propaga():
    with patch(
        "database.repository.repository.listar_tabelas",
        side_effect=Exception("falha geral"),
    ):
        with pytest.raises(Exception, match="falha geral"):
            repo.resumo_tabelas()
