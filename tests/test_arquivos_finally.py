from unittest.mock import MagicMock, patch

from util.arquivos import download_arquivo


@patch("util.arquivos.requests.get")
def test_download_arquivo_cobre_bloco_finally(mock_get):
    """
    Garante execução completa do bloco finally
    (tempo_total + logs finais).
    """

    response = MagicMock()
    response.headers = {
        "Content-Type": "text/csv",
        "content-length": "3",
    }
    response.raise_for_status.return_value = None
    response.iter_content.return_value = [b"abc"]

    mock_get.return_value = response

    resultado = download_arquivo("http://exemplo.com", "arquivo_finally")

    # Apenas garante que a função executou até o final
    assert resultado is not None
