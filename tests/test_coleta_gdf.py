import os
import pytest
from unittest.mock import MagicMock, patch, mock_open
import builtins
import src.coleta_gdf as cg

# -----------------------------
# TESTES download_arquivo
# -----------------------------
@patch("src.coleta_gdf.requests.get")
@patch("builtins.open", new_callable=mock_open)
@patch("src.coleta_gdf.detectar_extensao")
def test_download_sucesso(mock_detectar, mock_open_file, mock_get):
    mock_detectar.return_value = (".csv", "./data/csv")
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "text/csv", "content-length": "3"}
    mock_resp.iter_content.return_value = [b"abc"]
    mock_get.return_value = mock_resp

    caminho = cg.download_arquivo("http://teste", "saida/arq")
    assert os.path.normpath(caminho) == os.path.normpath("./data/csv/saida/arq.csv")
    mock_open_file.assert_called_once()

@patch("src.coleta_gdf.requests.get")
@patch("builtins.open", new_callable=mock_open)
def test_download_sem_content_length(mock_open_file, mock_get):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "text/csv"}
    mock_resp.iter_content.return_value = [b"linha1", b"linha2"]
    mock_get.return_value = mock_resp

    cg.download_arquivo("http://teste", "arquivo")
    mock_open_file.assert_called_once()

@patch("src.coleta_gdf.requests.get")
@patch("builtins.open")
def test_download_iter_content_sem_chunk(mock_open_file, mock_get):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "text/csv", "content-length": "100"}
    mock_resp.iter_content.return_value = [b"", b""]
    mock_get.return_value = mock_resp

    resultado = cg.download_arquivo("http://teste", "arquivo_vazio")
    mock_open_file.assert_not_called()
    assert resultado is None

@patch("src.coleta_gdf.requests.get")
@patch("os.remove")
@patch("builtins.open", new_callable=mock_open)
@patch("src.coleta_gdf.detectar_extensao")
def test_download_erro_apos_escrever(mock_detectar, mock_open_file, mock_rm, mock_get):
    mock_detectar.return_value = (".csv", "./data/csv")
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "text/csv", "content-length": "100"}

    def iter_side_effect(chunk_size=1024):
        yield b"abc"
        raise Exception("Erro depois do chunk")
    mock_resp.iter_content = iter_side_effect
    mock_get.return_value = mock_resp

    resultado = cg.download_arquivo("http://teste", "arquivo")
    mock_open_file.assert_called_once()
    mock_rm.assert_called_once()
    assert resultado is None

@patch("src.coleta_gdf.requests.get")
@patch("os.makedirs", side_effect=OSError("erro mkdir"))
def test_download_erro_makedirs(mock_mkdir, mock_get):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "text/csv", "content-length": "10"}
    mock_resp.iter_content.return_value = [b"abc"]
    mock_get.return_value = mock_resp

    resultado = cg.download_arquivo("http://x", "arquivo")
    assert resultado is None

@patch("src.coleta_gdf.requests.get", side_effect=Exception("ERRO geral"))
def test_download_erro_antes(mock_get):
    resultado = cg.download_arquivo("http://teste", "abc")
    assert resultado is None

@patch("src.coleta_gdf.requests.get")
def test_total_size_invalido(mock_get):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "text/csv", "content-length": "abc"}  # inválido
    mock_resp.iter_content.return_value = [b"123"]
    mock_get.return_value = mock_resp

    resultado = cg.download_arquivo("http://x", "arq.csv")
    assert resultado is None

@patch("src.coleta_gdf.requests.get")
def test_download_tipo_invalido(mock_get):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "application/json"}  # rejeitar
    mock_get.return_value = mock_resp

    resultado = cg.download_arquivo("http://t", "arq.csv")
    assert resultado is None

# Teste exceção close() ou remove()
@patch("src.coleta_gdf.requests.get")
@patch("builtins.open")
@patch("os.remove", side_effect=Exception("Erro remove"))
def test_close_remove_exception(mock_rm, mock_open_file, mock_get):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "text/csv", "content-length": "10"}

    def iter_side_effect(chunk_size=1024):
        yield b"abc"
        raise Exception("Erro durante write")

    mock_resp.iter_content = iter_side_effect
    mock_get.return_value = mock_resp

    result = cg.download_arquivo("http://teste", "arquivo_erro")
    assert result is None

# Teste arquivo vazio
@patch("src.coleta_gdf.requests.get")
@patch("builtins.open", new_callable=mock_open)
@patch("os.path.exists", return_value=True)
@patch("os.remove")
def test_download_arquivo_vazio(mock_rm, mock_exists, mock_open_file, mock_get):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "text/csv", "content-length": "0"}
    mock_resp.iter_content.return_value = [b""]  # chunks vazios
    mock_get.return_value = mock_resp

    result = cg.download_arquivo("http://teste", "arquivo_vazio")
    mock_rm.assert_called_once()
    assert result is None

# Teste bloco finally
@patch("src.coleta_gdf.requests.get")
@patch("builtins.open", new_callable=mock_open)
def test_download_finally_executado(mock_open_file, mock_get):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "text/csv", "content-length": "10"}

    def iter_side_effect(chunk_size=1024):
        raise Exception("erro proposital")
        yield b"abc"

    mock_resp.iter_content = iter_side_effect
    mock_get.return_value = mock_resp

    result = cg.download_arquivo("http://teste", "arquivo_finally")
    assert result is None

# -----------------------------
# TESTES detectar_extensao
# -----------------------------
@pytest.mark.parametrize("content_type, expected", [
    ("text/csv", (".csv", "./data/csv")),
    ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", (".xlsx", "./data/planilha")),
    ("application/octet-stream", (".bin", "./data/outros")),
])
def test_detectar_extensao(content_type, expected):
    assert cg.detectar_extensao(content_type) == expected

# -----------------------------
# TESTES gerar_urls_rotas
# -----------------------------
def test_gerar_urls_rotas():
    cg.rotas = {
        "r1": {"url": "http://teste1", "arquivo": "arq1"},
        "r2": {"dataset": "ds", "resource": "res", "arquivo": "arq2"}
    }
    with patch("src.coleta_gdf.montar_url") as mock_montar:
        mock_montar.return_value = "http://montado"
        urls = cg.gerar_urls_rotas()
        assert urls[0] == ("http://teste1", "arq1")
        assert urls[1] == ("http://montado", "arq2")

# -----------------------------
# TESTES limpar_diretorios
# -----------------------------
@patch("glob.glob")
@patch("os.remove")
@patch("src.coleta_gdf.fechar_loggers")
@patch("src.coleta_gdf.logs")
def test_limpar_diretorios(mock_logs, mock_fechar, mock_rm, mock_glob):
    mock_glob.side_effect = [["arquivo.csv"], [], ["log.log"]]
    mock_logger = MagicMock()
    mock_logs.return_value = mock_logger
    cg.limpar_diretorios()
    mock_fechar.assert_called_once()
    mock_rm.assert_any_call("arquivo.csv")
    mock_rm.assert_any_call("log.log")
    mock_logger.info.assert_called()

# -----------------------------
# TESTES coleta_dados
# -----------------------------
@patch("src.coleta_gdf.download_arquivo")
@patch("src.coleta_gdf.limpar_diretorios")
@patch("src.coleta_gdf.gerar_urls_rotas")
def test_coleta_dados(mock_rotas, mock_limpar, mock_download):
    mock_rotas.return_value = [("http://t1", "a1"), ("http://t2", "a2")]
    mock_download.return_value = "./data/csv/a1.csv"
    cg.coleta_dados()
    mock_limpar.assert_called_once()
    mock_rotas.assert_called_once()
    assert mock_download.call_count == 2

@patch("src.coleta_gdf.download_arquivo")
@patch("src.coleta_gdf.limpar_diretorios")
@patch("src.coleta_gdf.gerar_urls_rotas")
def test_coleta_dados_urls_extras(mock_rotas, mock_limpar, mock_download):
    urls_extras = [("http://t1", "a1")] * 25  # mais URLs que nomes de planilhas
    mock_rotas.return_value = urls_extras
    mock_download.return_value = "./data/csv/arquivo_1.csv"

    cg.coleta_dados()
    mock_limpar.assert_called_once()
    mock_rotas.assert_called_once()
    assert mock_download.call_count == 25
    mock_download.assert_any_call("http://t1", nome_arquivo="arquivo_25")
    
@patch("src.coleta_gdf.requests.get", side_effect=Exception("Erro requests"))
def test_download_exception_antes(mock_get):
    resultado = cg.download_arquivo("http://teste", "arquivo_excecao_antes")
    assert resultado is None
    
@patch("src.coleta_gdf.requests.get")
@patch("builtins.open", new_callable=mock_open)
@patch("os.remove")
def test_download_exception_durante_iter(mock_rm, mock_open_file, mock_get):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "text/csv", "content-length": "10"}
    
    def iter_side_effect(chunk_size=1024):
        yield b"abc"
        raise Exception("Erro iter_content")
    
    mock_resp.iter_content = iter_side_effect
    mock_get.return_value = mock_resp

    resultado = cg.download_arquivo("http://teste", "arquivo_iter")
    
    # file_obj foi aberto, logo o close e remove devem ser chamados dentro do except
    mock_open_file.assert_called_once()
    mock_rm.assert_called_once()
    assert resultado is None
    
@patch("src.coleta_gdf.requests.get")
@patch("builtins.open")
@patch("os.remove", side_effect=Exception("Erro remove"))
def test_download_exception_close_remove(mock_rm, mock_open_file, mock_get):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.headers = {"Content-Type": "text/csv", "content-length": "10"}

    def iter_side_effect(chunk_size=1024):
        yield b"abc"
        raise Exception("Erro durante write")

    mock_resp.iter_content = iter_side_effect
    mock_get.return_value = mock_resp

    resultado = cg.download_arquivo("http://teste", "arquivo_erro_close")
    mock_open_file.assert_called_once()
    mock_rm.assert_called_once()
    assert resultado is None



