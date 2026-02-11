import os
import builtins
import pytest
from unittest.mock import MagicMock, patch

from util.arquivos import limpar_diretorios, download_arquivo


# =========================================================
# 🧹 limpar_diretorios — ramos defensivos
# =========================================================


def test_limpar_diretorios_erro_ao_remover_arquivo(monkeypatch, tmp_path):
    """
    Cobre exceção silenciosa no os.remove (branch defensivo).
    """

    arquivo = tmp_path / "teste.csv"
    arquivo.write_text("conteudo")

    # glob retorna arquivo
    monkeypatch.setattr("glob.glob", lambda _: [str(arquivo)])

    # força erro no remove
    def erro_remove(_):
        raise OSError("erro ao remover")

    monkeypatch.setattr(os, "remove", erro_remove)

    # não deve quebrar
    logger = limpar_diretorios()
    assert logger is not None


# =========================================================
# ⬇️ download_arquivo — ramos defensivos
# =========================================================


def mock_response(content_type="text/csv", chunks=None):
    response = MagicMock()
    response.headers = {
        "Content-Type": content_type,
        "content-length": "3",
    }
    response.raise_for_status.return_value = None
    response.iter_content.return_value = chunks or [b"abc"]
    return response


@patch("util.arquivos.requests.get")
def test_download_arquivo_erro_ao_abrir_arquivo(mock_get, monkeypatch, tmp_path):
    """
    Erro ao abrir arquivo para escrita.
    """
    mock_get.return_value = mock_response()

    def erro_open(*args, **kwargs):
        raise OSError("erro ao abrir")

    monkeypatch.setattr(builtins, "open", erro_open)

    resultado = download_arquivo("http://teste", "arquivo")
    assert resultado is None


@patch("util.arquivos.requests.get")
def test_download_arquivo_erro_durante_iteracao(mock_get):
    """
    Erro levantado dentro do iter_content.
    """
    response = mock_response()
    response.iter_content.side_effect = RuntimeError("erro durante stream")
    mock_get.return_value = response

    resultado = download_arquivo("http://teste", "arquivo")
    assert resultado is None


@patch("util.arquivos.requests.get")
def test_download_arquivo_erro_remover_arquivo_incompleto(
    mock_get, monkeypatch, tmp_path
):
    """
    Falha ao remover arquivo após erro (branch defensivo duplo).
    """
    mock_get.return_value = mock_response()

    # open funciona, mas write falha
    mock_file = MagicMock()
    mock_file.write.side_effect = IOError("erro escrita")

    monkeypatch.setattr(builtins, "open", lambda *a, **k: mock_file)
    monkeypatch.setattr(
        os, "remove", lambda _: (_ for _ in ()).throw(PermissionError())
    )

    resultado = download_arquivo("http://teste", "arquivo")
    assert resultado is None
