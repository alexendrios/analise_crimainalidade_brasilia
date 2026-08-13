from unittest.mock import patch, MagicMock, mock_open
import requests

from util.arquivos import download_arquivo, detectar_extensao

# ------------------------------------------------------------------
# NOTA: `download_arquivo` usa `requests.Session()` por tentativa, não
# `requests.get` diretamente. Ver nota detalhada em test_arquivos.py.
# Os testes abaixo usam o fixture `mock_session_factory` (tests/conftest.py)
# e mockam `util.arquivos.sleep` para não pagar o backoff real entre
# tentativas.
# ------------------------------------------------------------------


# ============================================================
# download_arquivo - resposta JSON (erro da API)
# ============================================================
def test_download_arquivo_resposta_json_retorna_none(mock_session_factory):
    response = MagicMock()
    response.status_code = 200
    response.url = "http://teste.com/api"
    response.headers = {"Content-Type": "application/json", "Content-Disposition": ""}
    response.text = '{"erro": "não autorizado"}'
    response.raise_for_status = lambda: None

    mock_session_cls = mock_session_factory(response=response)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
        patch("util.arquivos.logger.error") as mock_error,
    ):
        resultado = download_arquivo("http://teste.com", "arquivo")

    assert resultado is None
    # mensagem real do código-fonte (util/arquivos.py) para esse ramo
    mock_error.assert_any_call(f"Resposta inesperada em JSON da API: {response.text}")


# ============================================================
# download_arquivo - nome_arquivo sem base válida
# ============================================================
@patch("util.arquivos.os.replace")
@patch("util.arquivos.os.makedirs")
def test_download_arquivo_nome_sem_base_usa_nome_completo(
    mock_makedirs, mock_replace, mock_session_factory
):
    response = MagicMock()
    response.status_code = 200
    response.url = "http://teste.com/download"
    response.headers = {
        "Content-Type": "text/csv",
        "Content-Length": "3",
        "Content-Disposition": "",
    }
    response.iter_content.return_value = [b"abc"]
    response.raise_for_status = lambda: None

    mock_session_cls = mock_session_factory(response=response)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
        patch("util.arquivos.open", mock_open(), create=True),
    ):
        # nome_arquivo vazio -> os.path.splitext("") = ("", "") -> base cai no ramo "not base"
        caminho = download_arquivo("http://teste.com", "")

    assert caminho is not None
    mock_replace.assert_called_once()


# ============================================================
# download_arquivo - download vazio (0 bytes)
# ============================================================
@patch("util.arquivos.os.makedirs")
@patch("util.arquivos.os.path.exists", return_value=True)
@patch("util.arquivos.os.path.getsize", return_value=0)
@patch("util.arquivos.os.remove")
def test_download_arquivo_vazio_remove_arquivo_parcial(
    mock_remove, mock_getsize, mock_exists, mock_makedirs, mock_session_factory
):
    response = MagicMock()
    response.status_code = 200
    response.url = "http://teste.com/download"
    response.headers = {
        "Content-Type": "text/csv",
        "Content-Length": "0",
        "Content-Disposition": "",
    }
    response.iter_content.return_value = []  # nenhum chunk retornado
    response.raise_for_status = lambda: None

    mock_session_cls = mock_session_factory(response=response)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
        patch("util.arquivos.open", mock_open(), create=True),
    ):
        resultado = download_arquivo("http://teste.com", "arquivo")

    assert resultado is None
    mock_remove.assert_called()


# ============================================================
# download_arquivo - HTTPError específico
# ============================================================
def test_download_arquivo_http_error_especifico(mock_session_factory):
    response = MagicMock()
    response.status_code = 404
    response.url = "http://teste.com/download"
    response.text = "Not Found"
    response.headers = {"Content-Type": "text/csv", "Content-Disposition": ""}
    response.raise_for_status.side_effect = requests.HTTPError("404")

    mock_session_cls = mock_session_factory(response=response)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
        patch("util.arquivos.logger.error") as mock_error,
    ):
        resultado = download_arquivo("http://teste.com", "arquivo")

    assert resultado is None
    # confirma que o ramo específico de HTTPError (com status_code/text) foi usado
    assert any("Erro HTTP" in str(call) for call in mock_error.call_args_list)


# ============================================================
# download_arquivo - finally fecha file_obj que ficou aberto após exceção
# ============================================================
@patch("util.arquivos.os.makedirs")
@patch("util.arquivos.os.path.exists", return_value=False)
def test_download_arquivo_fecha_file_obj_aberto_no_finally(
    mock_exists, mock_makedirs, mock_session_factory
):
    response = MagicMock()
    response.status_code = 200
    response.url = "http://teste.com/download"
    response.headers = {
        "Content-Type": "text/csv",
        "Content-Length": "10",
        "Content-Disposition": "",
    }
    response.raise_for_status = lambda: None

    def iter_chunks(chunk_size=8192):
        yield b"abc"
        raise Exception("erro no meio do stream")

    response.iter_content.side_effect = iter_chunks

    mock_session_cls = mock_session_factory(response=response)

    # `download_arquivo` usa `open(...)` como context manager
    # (`with open(temp_file_path, "wb") as f, tqdm(...) as progress:`), e
    # é o `__exit__` do arquivo real quem fecha o descritor ao sair do
    # bloco `with` (com ou sem exceção) — o código-fonte não chama
    # `f.close()` explicitamente em nenhum lugar. Um MagicMock genérico
    # como `open()` não simula isso sozinho (seu `__exit__` autogerado só
    # registra a chamada e, por ser "truthy", chegaria a SUPRIMIR a
    # exceção). Por isso configuramos o mock manualmente para reproduzir
    # o comportamento real: `__enter__` retorna o próprio mock, e
    # `__exit__` chama `.close()` e retorna False (não suprime a exceção).
    mock_file = MagicMock()
    mock_file.__enter__.return_value = mock_file

    def fake_exit(exc_type, exc_val, exc_tb):
        mock_file.close()
        return False

    mock_file.__exit__.side_effect = fake_exit

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
        patch("util.arquivos.open", return_value=mock_file, create=True),
    ):
        resultado = download_arquivo("http://teste.com", "arquivo")

    assert resultado is None
    mock_file.close.assert_called()


# ============================================================
# detectar_extensao - Content-Disposition com filename
# ============================================================
def test_detectar_extensao_via_content_disposition():
    response = MagicMock()
    response.url = "http://teste.com/download"  # sem extensão na URL
    response.headers = {
        "Content-Type": "application/json",  # não deveria ser usado (tem prioridade menor)
        "Content-Disposition": 'attachment; filename="relatorio.pdf"',
    }

    ext, pasta = detectar_extensao(response, "arquivo_sem_extensao")

    assert ext == ".pdf"
    assert pasta == "./data/bronze/outros"


# ============================================================
# detectar_extensao - Content-Type pdf (último recurso)
# ============================================================
def test_detectar_extensao_content_type_pdf():
    response = MagicMock()
    response.url = "http://teste.com/download"
    response.headers = {"Content-Type": "application/pdf", "Content-Disposition": ""}

    ext, pasta = detectar_extensao(response, "arquivo_sem_extensao")

    assert ext == ".pdf"
    assert pasta == "./data/bronze/outros"
