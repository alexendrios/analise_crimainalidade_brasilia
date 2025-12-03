import os
import glob
import pytest
import builtins
from unittest.mock import patch, MagicMock, mock_open
from util.arquivos import (
    limpar_diretorios,
    detectar_extensao,
    download_arquivo
)

# ============================================================
#  TESTES detectar_extensao
# ============================================================

def test_detectar_extensao_csv():
    ext, pasta = detectar_extensao("text/csv")
    assert ext == ".csv"
    assert pasta == "./data/csv"


def test_detectar_extensao_excel():
    ext, pasta = detectar_extensao(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert ext == ".xlsx"
    assert pasta == "./data/planilha"


def test_detectar_extensao_desconhecida():
    ext, pasta = detectar_extensao("application/json")
    assert ext == ".bin"
    assert pasta == "./data/outros"


# ============================================================
#  TESTES limpar_diretorios
# ============================================================

@patch("util.arquivos.fechar_loggers")
@patch("util.arquivos.glob.glob")
@patch("util.arquivos.os.remove")
@patch("util.arquivos.logs")
def test_limpar_diretorios_remove_arquivos(
    mock_logs, mock_remove, mock_glob, mock_fechar
):
    mock_glob.side_effect = [
        ["./data/csv/a.csv"],
        ["./data/planilha/b.xlsx"],
        ["./logs/c.log"],
    ]

    mock_logger = MagicMock()
    mock_logs.return_value = mock_logger

    logger = limpar_diretorios()

    assert mock_remove.call_count == 3
    mock_fechar.assert_called_once()
    mock_logs.assert_called()
    assert logger == mock_logger


@patch("util.arquivos.fechar_loggers")
@patch("util.arquivos.glob.glob", return_value=[])
@patch("util.arquivos.os.remove")
@patch("util.arquivos.logs")
def test_limpar_diretorios_sem_arquivos(
    mock_logs, mock_remove, mock_glob, mock_fechar
):
    logger = limpar_diretorios()

    mock_remove.assert_not_called()
    mock_fechar.assert_called_once()
    mock_logs.assert_called()
    assert logger is not None


@patch("util.arquivos.glob.glob", side_effect=[[], [], []])
@patch("builtins.print")
@patch("util.arquivos.fechar_loggers")
@patch("util.arquivos.logs")
def test_limpar_diretorios_sem_arquivos_print(
    mock_logs, mock_fechar, mock_print, mock_glob
):
    limpar_diretorios()
    assert mock_print.call_count == 4  # 3 mensagens + “Limpando diretórios...”


# ============================================================
#  TESTES download_arquivo — Caminho feliz
# ============================================================

@patch("util.arquivos.requests.get")
@patch("util.arquivos.tqdm")
@patch("util.arquivos.os.makedirs")
def test_download_arquivo_sucesso(mock_makedirs, mock_tqdm, mock_get):
    response = MagicMock()
    response.status_code = 200
    response.headers = {
        "Content-Type": "text/csv",
        "content-length": "10"
    }
    response.iter_content.return_value = [b"abc", b"def"]
    response.raise_for_status = lambda: None

    mock_get.return_value = response
    mock_tqdm.return_value.__enter__.return_value = MagicMock()

    with patch("util.arquivos.open", mock_open(), create=True):
        caminho = download_arquivo("http://teste.com", "arquivo_teste")

    assert caminho.endswith(".csv")


# ============================================================
#  TESTES download_arquivo — Tipo inválido
# ============================================================

@patch("util.arquivos.requests.get")
def test_download_arquivo_tipo_invalido(mock_get):
    response = MagicMock()
    response.status_code = 200
    response.headers = {"Content-Type": "text/html"}
    response.raise_for_status = lambda: None

    mock_get.return_value = response

    resultado = download_arquivo("http://teste.com", "arq_html")
    assert resultado is None


# ============================================================
#  TESTES download_arquivo — Arquivo vazio
# ============================================================

@patch("util.arquivos.requests.get")
@patch("util.arquivos.os.remove")
def test_download_arquivo_vazio(mock_remove, mock_get):
    response = MagicMock()
    response.status_code = 200
    response.headers = {"Content-Type": "text/csv", "content-length": "5"}
    response.iter_content.return_value = []
    response.raise_for_status = lambda: None

    mock_get.return_value = response

    with patch("util.arquivos.open", mock_open(), create=True):
        resultado = download_arquivo("http://teste.com", "arq_vazio")

    assert resultado is None


# ============================================================
#  TESTES download_arquivo — Exceção durante download
# ============================================================

@patch("util.arquivos.requests.get", side_effect=Exception("Erro"))
def test_download_arquivo_exception(mock_get):
    resultado = download_arquivo("http://erro.com", "arq_erro")
    assert resultado is None


# ============================================================
#  TESTES download_arquivo — raise_for_status lança erro
# ============================================================

@patch("util.arquivos.requests.get")
def test_download_arquivo_raise_for_status(mock_get):
    response = MagicMock()
    response.raise_for_status.side_effect = Exception("HTTP Error")
    mock_get.return_value = response

    resultado = download_arquivo("http://teste.com", "arq_fail")
    assert resultado is None


# ============================================================
#  TESTE: finally sempre executa
# ============================================================

@patch("util.arquivos.requests.get", side_effect=Exception("Erro2"))
def test_download_arquivo_finally_executa(mock_get):
    result = download_arquivo("http://teste", "arq_finally")
    assert result is None


# ============================================================
#  TESTE: exceção após arquivo ter sido aberto
# ============================================================

@patch("util.arquivos.requests.get")
@patch("util.arquivos.os.remove")
@patch("util.arquivos.os.path.exists", return_value=True)
def test_download_arquivo_excecao_apos_abrir(
    mock_exists, mock_remove, mock_get
):
    response = MagicMock()
    response.status_code = 200
    response.headers = {"Content-Type": "text/csv", "content-length": "10"}

    # Primeiro chunk OK abre o arquivo
    # Segundo chunk gera erro → ativa finally com file_obj != None
    def iter_chunks(chunk_size=1024):
        yield b"abc"
        raise Exception("falha durante stream")

    response.iter_content.side_effect = iter_chunks
    response.raise_for_status = lambda: None

    mock_get.return_value = response
    m_open = mock_open()

    with patch("util.arquivos.open", m_open, create=True):
        result = download_arquivo("http://teste.com", "arq_quebra")

    assert result is None
    mock_remove.assert_called_once()
