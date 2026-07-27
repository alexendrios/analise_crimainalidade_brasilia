from unittest.mock import patch, MagicMock, mock_open
import requests

from util.arquivos import download_arquivo, detectar_extensao


# ============================================================
# download_arquivo - resposta JSON (erro da API)
# ============================================================
@patch("util.arquivos.requests.get")
def test_download_arquivo_resposta_json_retorna_none(mock_get):
    response = MagicMock()
    response.status_code = 200
    response.url = "http://teste.com/api"
    response.headers = {"Content-Type": "application/json"}
    response.text = '{"erro": "não autorizado"}'
    response.raise_for_status = lambda: None

    mock_get.return_value = response

    with patch("util.arquivos.logger.error") as mock_error:
        resultado = download_arquivo("http://teste.com", "arquivo")

    assert resultado is None
    mock_error.assert_any_call(f"Resposta da API: {response.text}")


# ============================================================
# download_arquivo - nome_arquivo sem base válida
# ============================================================
@patch("util.arquivos.requests.get")
@patch("util.arquivos.os.makedirs")
def test_download_arquivo_nome_sem_base_usa_nome_completo(mock_makedirs, mock_get):
    response = MagicMock()
    response.status_code = 200
    response.url = "http://teste.com/download"
    response.headers = {"Content-Type": "text/csv", "content-length": "3"}
    response.iter_content.return_value = [b"abc"]
    response.raise_for_status = lambda: None

    mock_get.return_value = response

    with patch("util.arquivos.open", mock_open(), create=True):
        # nome_arquivo vazio -> os.path.splitext("") = ("", "") -> base cai no ramo "not base"
        caminho = download_arquivo("http://teste.com", "")

    assert caminho is not None


# ============================================================
# download_arquivo - download vazio (0 bytes)
# ============================================================
@patch("util.arquivos.requests.get")
@patch("util.arquivos.os.makedirs")
@patch("util.arquivos.os.path.exists", return_value=True)
@patch("util.arquivos.os.path.getsize", return_value=0)
@patch("util.arquivos.os.remove")
def test_download_arquivo_vazio_remove_arquivo_parcial(
    mock_remove, mock_getsize, mock_exists, mock_makedirs, mock_get
):
    response = MagicMock()
    response.status_code = 200
    response.url = "http://teste.com/download"
    response.headers = {"Content-Type": "text/csv", "content-length": "0"}
    response.iter_content.return_value = []  # nenhum chunk retornado
    response.raise_for_status = lambda: None

    mock_get.return_value = response

    with patch("util.arquivos.open", mock_open(), create=True):
        resultado = download_arquivo("http://teste.com", "arquivo")

    assert resultado is None
    mock_remove.assert_called()


# ============================================================
# download_arquivo - HTTPError específico
# ============================================================
@patch("util.arquivos.requests.get")
def test_download_arquivo_http_error_especifico(mock_get):
    response = MagicMock()
    response.status_code = 404
    response.text = "Not Found"
    response.raise_for_status.side_effect = requests.HTTPError("404")

    mock_get.return_value = response

    with patch("util.arquivos.logger.error") as mock_error:
        resultado = download_arquivo("http://teste.com", "arquivo")

    assert resultado is None
    # confirma que o ramo específico de HTTPError (com status_code/text) foi usado
    assert any("Erro HTTP" in str(call) for call in mock_error.call_args_list)


# ============================================================
# download_arquivo - finally fecha file_obj que ficou aberto após exceção
# ============================================================
@patch("util.arquivos.requests.get")
@patch("util.arquivos.os.makedirs")
@patch("util.arquivos.os.path.exists", return_value=False)
def test_download_arquivo_fecha_file_obj_aberto_no_finally(
    mock_exists, mock_makedirs, mock_get
):
    response = MagicMock()
    response.status_code = 200
    response.url = "http://teste.com/download"
    response.headers = {"Content-Type": "text/csv", "content-length": "10"}
    response.raise_for_status = lambda: None

    def iter_chunks(chunk_size=8192):
        yield b"abc"
        raise Exception("erro no meio do stream")

    response.iter_content.side_effect = iter_chunks
    mock_get.return_value = response

    mock_file = MagicMock()
    mock_file.closed = False  # simula arquivo real ainda aberto

    with patch("util.arquivos.open", return_value=mock_file, create=True):
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
