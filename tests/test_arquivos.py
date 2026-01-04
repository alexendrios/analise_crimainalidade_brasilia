import pytest
from unittest.mock import patch, MagicMock, mock_open
from util.arquivos import limpar_diretorios, detectar_extensao, download_arquivo

# ============================================================
#  TESTES detectar_extensao (aceita CSV, Excel e ZIP)
# ============================================================


@pytest.mark.parametrize(
    "content_type, esperado",
    [
        ("text/csv", (".csv", "./data/csv")),
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            (".xlsx", "./data/planilha"),
        ),
        ("application/vnd.ms-excel", (".xls", "./data/planilha")),
        ("application/zip", (".zip", "./data/zip")),
        ("application/octet-stream", (".zip", "./data/zip")),
        ("application/json", (".bin", "./data/outros")),
    ],
)
def test_detectar_extensao(content_type, esperado):
    assert detectar_extensao(content_type) == esperado


# ============================================================
#  TESTES limpar_diretorios
# ============================================================


@patch("util.arquivos.os.remove")
@patch("util.arquivos.glob.glob")
@patch("util.arquivos.fechar_loggers")
@patch("util.arquivos.logs")
def test_limpar_diretorios_remove_arquivos(
    mock_logs, mock_fechar, mock_glob, mock_remove
):
    mock_glob.side_effect = [
        ["./data/csv/a.csv"],
        ["./data/planilha/b.xlsx"],
        [],
        [],
        [],
        [],
        ["./logs/c.log"],
    ]

    mock_logger = MagicMock()
    mock_logs.return_value = mock_logger

    logger = limpar_diretorios()

    assert mock_remove.call_count == 3
    mock_fechar.assert_called_once()
    mock_logs.assert_called()
    assert logger == mock_logger


@patch("util.arquivos.glob.glob", return_value=[])
@patch("util.arquivos.fechar_loggers")
@patch("util.arquivos.logs")
def test_limpar_diretorios_sem_arquivos(mock_logs, mock_fechar, mock_glob):
    logger = limpar_diretorios()

    mock_fechar.assert_called_once()
    mock_logs.assert_called()
    assert logger is not None


# ============================================================
#  TESTES download_arquivo — Caminhos felizes
# ============================================================


@pytest.mark.parametrize(
    "content_type, extensao",
    [
        ("text/csv", ".csv"),
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xlsx",
        ),
        ("application/vnd.ms-excel", ".xls"),
        ("application/zip", ".zip"),
        ("application/octet-stream", ".zip"),
    ],
)
@patch("util.arquivos.requests.get")
@patch("util.arquivos.tqdm")
@patch("util.arquivos.os.makedirs")
def test_download_arquivo_sucesso(
    mock_makedirs, mock_tqdm, mock_get, content_type, extensao
):
    response = MagicMock()
    response.status_code = 200
    response.headers = {
        "Content-Type": content_type,
        "content-length": "6",
    }
    response.iter_content.return_value = [b"abc", b"def"]
    response.raise_for_status = lambda: None

    mock_get.return_value = response
    mock_tqdm.return_value.__enter__.return_value = MagicMock()

    with patch("util.arquivos.open", mock_open(), create=True):
        caminho = download_arquivo("http://teste.com", "arquivo")

    assert caminho.endswith(extensao)


# ============================================================
#  TESTES download_arquivo — Tipo não suportado
# ============================================================


@patch("util.arquivos.requests.get")
def test_download_arquivo_tipo_invalido(mock_get):
    response = MagicMock()
    response.headers = {"Content-Type": "text/html"}
    response.raise_for_status = lambda: None
    mock_get.return_value = response

    resultado = download_arquivo("http://teste.com", "arquivo_html")
    assert resultado is None


# ============================================================
#  TESTES download_arquivo — Arquivo vazio
# ============================================================


@patch("util.arquivos.requests.get")
@patch("util.arquivos.os.remove")
def test_download_arquivo_vazio(mock_remove, mock_get):
    response = MagicMock()
    response.headers = {
        "Content-Type": "text/csv",
        "content-length": "10",
    }
    response.iter_content.return_value = []
    response.raise_for_status = lambda: None
    mock_get.return_value = response

    with patch("util.arquivos.open", mock_open(), create=True):
        resultado = download_arquivo("http://teste.com", "arquivo_vazio")

    assert resultado is None
    mock_remove.assert_not_called()


# ============================================================
#  TESTES download_arquivo — Exceções
# ============================================================


@patch("util.arquivos.requests.get", side_effect=Exception("Erro"))
def test_download_arquivo_exception(mock_get):
    assert download_arquivo("http://erro.com", "falha") is None


@patch("util.arquivos.requests.get")
def test_download_arquivo_raise_for_status(mock_get):
    response = MagicMock()
    response.raise_for_status.side_effect = Exception("HTTP Error")
    mock_get.return_value = response

    assert download_arquivo("http://teste.com", "erro_http") is None


@patch("util.arquivos.requests.get")
@patch("util.arquivos.os.remove")
@patch("util.arquivos.os.path.exists", return_value=True)
def test_download_arquivo_excecao_apos_abrir(mock_exists, mock_remove, mock_get):
    response = MagicMock()
    response.headers = {
        "Content-Type": "text/csv",
        "content-length": "10",
    }

    def iter_chunks(_=1024):
        yield b"abc"
        raise Exception("Erro durante stream")

    response.iter_content.side_effect = iter_chunks
    response.raise_for_status = lambda: None
    mock_get.return_value = response

    with patch("util.arquivos.open", mock_open(), create=True):
        resultado = download_arquivo("http://teste.com", "quebra")

    assert resultado is None
    mock_remove.assert_called_once()
