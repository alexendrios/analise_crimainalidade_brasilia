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

def test_salvar_tabela_sucesso():
    df = pd.DataFrame({"id": [1, 2]})
    engine_mock = MagicMock()

    with (
        patch(
            "database.repository.repository.conectar_banco", return_value=engine_mock
        ),
        patch.object(df, "to_sql") as to_sql_mock,
    ):
        repo.salvar_tabela(df, "clientes")

        to_sql_mock.assert_called_once_with(
            "clientes",
            engine_mock,
            if_exists="replace",
            index=False,
            method="multi",
        )

def test_salvar_tabela_nome_invalido():
    df = pd.DataFrame({"id": [1]})

    with patch.object(df, "to_sql") as to_sql_mock:
        repo.salvar_tabela(df, "clientes-drop")

    to_sql_mock.assert_not_called()

def test_salvar_tabela_sqlalchemy_error():
    df = pd.DataFrame({"id": [1]})
    engine_mock = MagicMock()

    with (
        patch(
            "database.repository.repository.conectar_banco", return_value=engine_mock
        ),
        patch.object(
            df,
            "to_sql",
            side_effect=SQLAlchemyError,
        ),
    ):
        repo.salvar_tabela(df, "clientes")

def test_salvar_tabela_exception_generica():
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
        repo.salvar_tabela(df, "clientes")


