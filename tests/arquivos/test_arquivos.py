import pytest
from unittest.mock import patch, MagicMock, mock_open
from util.arquivos import limpar_diretorios, detectar_extensao, download_arquivo

# ============================================================
#  TESTES detectar_extensao (aceita CSV, Excel e ZIP)
# ============================================================


@pytest.mark.parametrize(
    "content_type, esperado",
    [
        ("text/csv", (".csv", "./data/bronze/csv")),
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            (".xlsx", "./data/bronze/planilha"),
        ),
        ("application/vnd.ms-excel", (".xls", "./data/bronze/planilha")),
        ("application/zip", (".zip", "./data/bronze/zip")),
        ("application/octet-stream", (".zip", "./data/bronze/zip")),
        ("application/json", (".bin", "./data/bronze/outros")),
    ],
)
def test_detectar_extensao(content_type, esperado):
    # detectar_extensao(response, nome_arquivo) hoje resolve a extensão nesta
    # ordem: nome_arquivo -> URL final -> Content-Disposition -> Content-Type.
    # Para testar isoladamente a resolução por Content-Type, garantimos que
    # as 3 primeiras fontes não tenham nenhuma extensão.
    response = MagicMock()
    response.url = "http://teste.com/download"  # sem extensão no path
    response.headers = {"Content-Type": content_type, "Content-Disposition": ""}

    assert detectar_extensao(response, "arquivo_sem_extensao") == esperado


def test_detectar_extensao_prioriza_nome_arquivo():
    """Quando nome_arquivo já tem extensão, ela prevalece sobre o Content-Type."""
    response = MagicMock()
    response.url = "http://teste.com/download"
    response.headers = {"Content-Type": "application/json", "Content-Disposition": ""}

    assert detectar_extensao(response, "relatorio.csv") == (
        ".csv",
        "./data/bronze/csv",
    )


def test_detectar_extensao_prioriza_url_quando_nome_sem_extensao():
    """Sem extensão no nome, cai para a extensão da URL final."""
    response = MagicMock()
    response.url = "http://teste.com/arquivos/planilha.xlsx"
    response.headers = {"Content-Type": "application/json", "Content-Disposition": ""}

    assert detectar_extensao(response, "arquivo_sem_extensao") == (
        ".xlsx",
        "./data/bronze/planilha",
    )


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
        ["./data/bronze/csv/a.csv"],
        ["./data/bronze/planilha/b.xlsx"],
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
    response.url = "http://teste.com/download"  # string real p/ urlparse não quebrar
    response.headers = {
        "Content-Type": content_type,
        "content-length": "6",
        "Content-Disposition": "",
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
@patch("util.arquivos.os.path.getsize", return_value=0)
@patch("util.arquivos.os.path.exists", return_value=True)
def test_download_arquivo_excecao_apos_abrir(
    mock_exists, mock_getsize, mock_remove, mock_get
):
    response = MagicMock()
    response.url = "http://teste.com/download"
    response.headers = {
        "Content-Type": "text/csv",
        "content-length": "10",
        "Content-Disposition": "",
    }

    # chunk_size é passado por keyword em download_arquivo (iter_content(chunk_size=8192))
    def iter_chunks(chunk_size=8192):
        yield b"abc"
        raise Exception("Erro durante stream")

    response.iter_content.side_effect = iter_chunks
    response.raise_for_status = lambda: None
    mock_get.return_value = response

    with patch("util.arquivos.open", mock_open(), create=True):
        resultado = download_arquivo("http://teste.com", "quebra")

    assert resultado is None
    # os.path.exists+getsize==0 é checado no finally -> deve remover o arquivo parcial
    mock_remove.assert_called_once()
