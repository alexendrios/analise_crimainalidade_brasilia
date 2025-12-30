import pytest
from unittest.mock import patch, MagicMock
from src.coleta_gdf import coleta_dados


# ============================================================
# TESTE: fluxo básico
# ============================================================


@patch("src.coleta_gdf.download_arquivo")
@patch("src.coleta_gdf.gerar_urls_rotas")
@patch("src.coleta_gdf.logger")
def test_coleta_dados_fluxo_basico(mock_logger, mock_gerar, mock_download):
    mock_gerar.return_value = [
        ("http://url1", "file1"),
        ("http://url2", "file2"),
        ("http://url3", "file3"),
    ]

    coleta_dados(
        url_="http://base",
        lista_nomes=["a", "b", "c"],
        rotas=["r1", "r2", "r3"],
    )

    mock_gerar.assert_called_once_with("http://base", ["r1", "r2", "r3"])
    assert mock_download.call_count == 3
    assert mock_logger.info.call_count > 0


# ============================================================
# TESTE: URLs e nomes desalinhados → warning
# ============================================================


@patch("src.coleta_gdf.download_arquivo")
@patch("src.coleta_gdf.gerar_urls_rotas")
@patch("src.coleta_gdf.logger")
def test_coleta_dados_urls_desalinhadas(mock_logger, mock_gerar, mock_download):
    mock_gerar.return_value = [
        ("http://url_unica", "arq1"),
        ("http://url_extra", "arq2"),
    ]

    coleta_dados(
        url_="http://base",
        lista_nomes=["apenas_um_nome"],
        rotas=["r1", "r2"],
    )

    mock_logger.warning.assert_called_once()


# ============================================================
# TESTE: URLs excedentes → fallback arquivo_N
# ============================================================


@patch("src.coleta_gdf.download_arquivo")
@patch("src.coleta_gdf.gerar_urls_rotas")
@patch("src.coleta_gdf.logger")
def test_coleta_dados_urls_excedentes(mock_logger, mock_gerar, mock_download):
    mock_gerar.return_value = [(f"http://url{i}", f"a{i}") for i in range(25)]

    coleta_dados(
        url_="http://base",
        lista_nomes=[f"nome{i}" for i in range(10)],
        rotas=[f"r{i}" for i in range(25)],
    )

    chamadas = mock_download.call_args_list

    assert chamadas[-1].kwargs["nome_arquivo"] == "arquivo_25"


# ============================================================
# TESTE: nomes de planilhas respeitados
# ============================================================


@patch("src.coleta_gdf.download_arquivo")
@patch("src.coleta_gdf.gerar_urls_rotas")
@patch("src.coleta_gdf.logger")
def test_coleta_dados_nomes_de_planilhas(mock_logger, mock_gerar, mock_download):
    mock_gerar.return_value = [
        ("http://url1", "a1"),
        ("http://url2", "a2"),
    ]

    coleta_dados(
        url_="http://base",
        lista_nomes=[
            "roubo-a-transeunte",
            "roubo-de-veiculo",
        ],
        rotas=["r1", "r2"],
    )

    chamadas = mock_download.call_args_list

    assert chamadas[0].kwargs["nome_arquivo"] == "roubo-a-transeunte"
    assert chamadas[1].kwargs["nome_arquivo"] == "roubo-de-veiculo"
