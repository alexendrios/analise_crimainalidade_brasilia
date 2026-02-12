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