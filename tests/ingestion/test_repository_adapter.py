from unittest.mock import patch

import pandas as pd

from ingestion.repository_adapter import Repository


@patch("ingestion.repository_adapter.carregar_tabela")
def test_repository_load_delega_para_carregar_tabela(mock_carregar):
    mock_carregar.return_value = pd.DataFrame({"a": [1]})

    resultado = Repository.load("minha_tabela")

    mock_carregar.assert_called_once_with("minha_tabela")
    assert resultado.equals(pd.DataFrame({"a": [1]}))


@patch("ingestion.repository_adapter.inserir_dados")
def test_repository_save_delega_para_inserir_dados(mock_inserir):
    df = pd.DataFrame({"a": [1]})

    Repository.save(df, "minha_tabela")

    mock_inserir.assert_called_once_with(df, "minha_tabela")
