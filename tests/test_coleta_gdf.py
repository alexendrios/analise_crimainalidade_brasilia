import pytest
from unittest.mock import patch, MagicMock
from src.coleta_gdf import coleta_dados


# ----------------------------------------------------------------------
# TESTE PRINCIPAL DE coleta_dados
# ----------------------------------------------------------------------
@patch("src.coleta_gdf.download_arquivo")
@patch("src.coleta_gdf.gerar_urls_rotas")
@patch("src.coleta_gdf.limpar_diretorios")
@patch("src.coleta_gdf.logger")
def test_coleta_dados_fluxo_basico(mock_logger, mock_limpar, mock_gerar, mock_download):
    
    mock_gerar.return_value = [
        ("http://url1", "file1"),
        ("http://url2", "file2"),
        ("http://url3", "file3")
    ]

    mock_download.return_value = "/fake/path"

    coleta_dados()

    mock_limpar.assert_called_once()
    mock_gerar.assert_called_once()
    assert mock_download.call_count == 3
    assert mock_logger.info.call_count > 0


# ----------------------------------------------------------------------
@patch("src.coleta_gdf.download_arquivo")
@patch("src.coleta_gdf.gerar_urls_rotas")
@patch("src.coleta_gdf.limpar_diretorios")
@patch("src.coleta_gdf.logger")
def test_coleta_dados_urls_desalinhadas(mock_logger, mock_limpar, mock_gerar, mock_download):

    mock_gerar.return_value = [("http://url_unica", "arq1")]

    coleta_dados()

    mock_logger.warning.assert_called_once()


# ----------------------------------------------------------------------
@patch("src.coleta_gdf.download_arquivo")
@patch("src.coleta_gdf.gerar_urls_rotas")
@patch("src.coleta_gdf.limpar_diretorios")
@patch("src.coleta_gdf.logger")
def test_coleta_dados_urls_excedentes(mock_logger, mock_limpar, mock_gerar, mock_download):

    mock_gerar.return_value = [(f"http://url{i}", f"a{i}") for i in range(25)]

    coleta_dados()

    chamadas = mock_download.call_args_list

    assert chamadas[-1].kwargs["nome_arquivo"] == "arquivo_25"


# ----------------------------------------------------------------------
@patch("src.coleta_gdf.download_arquivo")
@patch("src.coleta_gdf.gerar_urls_rotas")
@patch("src.coleta_gdf.limpar_diretorios")
@patch("src.coleta_gdf.logger")
def test_coleta_dados_nomes_de_planilhas(mock_logger, mock_limpar, mock_gerar, mock_download):

    mock_gerar.return_value = [
        ("http://url1", "a1"),
        ("http://url2", "a2")
    ]

    coleta_dados()

    chamadas = mock_download.call_args_list

    assert chamadas[0].kwargs["nome_arquivo"] == "roubo-a-transeunte"
    assert chamadas[1].kwargs["nome_arquivo"] == "roubo-de-veiculo"
