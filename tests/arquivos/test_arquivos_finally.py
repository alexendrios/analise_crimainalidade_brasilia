from unittest.mock import MagicMock, mock_open, patch

from util.arquivos import download_arquivo

# NOTA: `download_arquivo` usa `requests.Session()` por tentativa, não
# `requests.get` diretamente — ver nota detalhada em test_arquivos.py.
# Corrigido para usar o fixture `mock_session_factory` (tests/conftest.py)
# e mockar filesystem (makedirs/replace/open) para não depender de I/O real.


@patch("util.arquivos.os.replace")
@patch("util.arquivos.os.makedirs")
def test_download_arquivo_cobre_bloco_finally(
    mock_makedirs, mock_replace, mock_session_factory
):
    """
    Garante execução completa do bloco finally
    (tempo_total + logs finais).
    """

    response = MagicMock()
    response.url = "http://exemplo.com/download"
    response.headers = {
        "Content-Type": "text/csv",
        "Content-Length": "3",
        "Content-Disposition": "",
    }
    response.raise_for_status.return_value = None
    response.iter_content.return_value = [b"abc"]

    mock_session_cls = mock_session_factory(response=response)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
        patch("util.arquivos.open", mock_open(), create=True),
    ):
        resultado = download_arquivo("http://exemplo.com", "arquivo_finally")

    # Apenas garante que a função executou até o final
    assert resultado is not None
    mock_replace.assert_called_once()
