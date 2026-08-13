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


# ------------------------------------------------------------------
# NOTA IMPORTANTE sobre mocking de rede nestes testes:
#
# `download_arquivo` NÃO chama `requests.get` diretamente. Ele abre uma
# `requests.Session()` por tentativa (`with requests.Session() as session:
# session.get(...)`). Um mock de `requests.get` (como era feito
# anteriormente nesta suíte) portanto NUNCA intercepta a chamada real —
# o teste acaba batendo na rede de verdade, retornando resultados não
# determinísticos e tornando a suíte extremamente lenta (os 5 retries
# com backoff de `sleep()` chegam a levar ~1 minuto por teste).
#
# A correção é usar o fixture `mock_session_factory` (definido em
# tests/conftest.py), que substitui `requests.Session` por um mock cujo
# `.get()` retorna a `response` combinada — e sempre em conjunto com
# `patch("util.arquivos.sleep")` para eliminar os `sleep()` reais de
# retry/backoff.
# ------------------------------------------------------------------


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
@patch("util.arquivos.os.replace")
@patch("util.arquivos.os.makedirs")
@patch("util.arquivos.tqdm")
def test_download_arquivo_sucesso(
    mock_tqdm, mock_makedirs, mock_replace, content_type, extensao, mock_session_factory
):
    response = MagicMock()
    response.status_code = 200
    response.url = "http://teste.com/download"  # string real p/ urlparse não quebrar
    response.headers = {
        "Content-Type": content_type,
        "Content-Length": "6",
        "Content-Disposition": "",
    }
    response.iter_content.return_value = [b"abc", b"def"]
    response.raise_for_status = lambda: None

    mock_tqdm.return_value.__enter__.return_value = MagicMock()
    mock_session_cls = mock_session_factory(response=response)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
        patch("util.arquivos.open", mock_open(), create=True),
    ):
        caminho = download_arquivo("http://teste.com", "arquivo")

    assert caminho is not None
    assert caminho.endswith(extensao)
    # `open()` está mockado (não cria arquivo real), então sem mockar
    # também `os.replace` a função falharia ao tentar renomear um
    # arquivo temporário que nunca existiu de fato no disco.
    mock_replace.assert_called_once()


# ============================================================
#  TESTES download_arquivo — Tipo não suportado
# ============================================================


def test_download_arquivo_tipo_invalido(mock_session_factory):
    response = MagicMock()
    response.url = "http://teste.com/download"
    response.headers = {"Content-Type": "text/html", "Content-Disposition": ""}
    response.raise_for_status = lambda: None

    mock_session_cls = mock_session_factory(response=response)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
    ):
        resultado = download_arquivo("http://teste.com", "arquivo_html")

    assert resultado is None


# ============================================================
#  TESTES download_arquivo — Arquivo vazio
# ============================================================


@patch("util.arquivos.os.makedirs")
@patch("util.arquivos.os.remove")
def test_download_arquivo_vazio(mock_remove, mock_makedirs, mock_session_factory):
    response = MagicMock()
    response.url = "http://teste.com/download"
    response.headers = {
        "Content-Type": "text/csv",
        "Content-Length": "10",
        "Content-Disposition": "",
    }
    response.iter_content.return_value = []
    response.raise_for_status = lambda: None

    mock_session_cls = mock_session_factory(response=response)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
        patch("util.arquivos.open", mock_open(), create=True),
    ):
        resultado = download_arquivo("http://teste.com", "arquivo_vazio")

    assert resultado is None
    mock_remove.assert_not_called()


# ============================================================
#  TESTES download_arquivo — Exceções
# ============================================================


def test_download_arquivo_exception(mock_session_factory):
    mock_session_cls = mock_session_factory(get_side_effect=Exception("Erro"))

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
    ):
        resultado = download_arquivo("http://erro.com", "falha")

    assert resultado is None


def test_download_arquivo_raise_for_status(mock_session_factory):
    response = MagicMock()
    response.url = "http://teste.com/download"
    response.headers = {"Content-Type": "text/csv", "Content-Disposition": ""}
    response.raise_for_status.side_effect = Exception("HTTP Error")

    mock_session_cls = mock_session_factory(response=response)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
    ):
        resultado = download_arquivo("http://teste.com", "erro_http")

    assert resultado is None


@patch("util.arquivos.os.makedirs")
@patch("util.arquivos.os.remove")
@patch("util.arquivos.os.path.getsize", return_value=0)
@patch("util.arquivos.os.path.exists", return_value=True)
def test_download_arquivo_excecao_apos_abrir(
    mock_exists, mock_getsize, mock_remove, mock_makedirs, mock_session_factory
):
    response = MagicMock()
    response.url = "http://teste.com/download"
    response.headers = {
        "Content-Type": "text/csv",
        "Content-Length": "10",
        "Content-Disposition": "",
    }

    # chunk_size é passado por keyword em download_arquivo (iter_content(chunk_size=8192))
    def iter_chunks(chunk_size=8192):
        yield b"abc"
        raise Exception("Erro durante stream")

    response.iter_content.side_effect = iter_chunks
    response.raise_for_status = lambda: None

    mock_session_cls = mock_session_factory(response=response)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
        patch("util.arquivos.open", mock_open(), create=True),
    ):
        resultado = download_arquivo("http://teste.com", "quebra")

    assert resultado is None
    # Com os.path.exists sempre True, o os.remove acaba sendo chamado em
    # várias etapas do fluxo (limpeza prévia por tentativa, finally do
    # streaming, e limpeza final de arquivo de 0 bytes) — não apenas uma
    # vez. O que importa aqui é que o arquivo parcial É removido.
    assert mock_remove.called
