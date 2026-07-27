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

def test_salvar_tabela_sucesso():
    arquivos_mock = {"arquivo_valido.csv": "clientes", "outro_arquivo.csv": "produtos"}
    df_mock = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    with (
        patch("database.load_csvs.arquivos", arquivos_mock),
        patch("pandas.read_csv", return_value=df_mock),
        patch("database.load_csvs.inserir_dados") as inserir_mock,
    ):
        salvar_tabela()

    assert inserir_mock.call_count == 2
    nomes_tabelas = {call.args[1] for call in inserir_mock.call_args_list}
    assert nomes_tabelas == {"clientes", "produtos"}
