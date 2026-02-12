import pandas as pd
from unittest.mock import patch, MagicMock
from database.load_csvs import salvar_tabela


def test_salvar_tabela_parser_error():
    # Mock do dicionário de arquivos para simular CSV
    arquivos_mock = {"arquivo_invalido.csv": "clientes"}

    with (
        patch("database.load_csvs.arquivos", arquivos_mock),
        patch("pandas.read_csv", side_effect=pd.errors.ParserError("Erro de parsing")),
        patch("database.load_csvs.inserir_dados") as inserir_mock,
    ):
        salvar_tabela()

        inserir_mock.assert_not_called()

def test_salvar_tabela_exception_generica():
    arquivos_mock = {"arquivo_errado.csv": "clientes"}

    with (
        patch("database.load_csvs.arquivos", arquivos_mock),
        patch("pandas.read_csv", side_effect=Exception("Erro genérico")),
        patch("database.load_csvs.inserir_dados") as inserir_mock,
        patch("database.load_csvs.logger") as logger_mock,
    ):
        salvar_tabela()

        inserir_mock.assert_not_called()

        logger_mock.error.assert_called()

        assert "Erro inesperado ao processar arquivo_errado.csv" in str(
            logger_mock.error.call_args
        )