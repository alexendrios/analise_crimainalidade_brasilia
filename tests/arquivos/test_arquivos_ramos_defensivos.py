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
    response.url = "http://teste/download"
    response.headers = {
        "Content-Type": content_type,
        "Content-Length": "3",
        "Content-Disposition": "",
    }
    response.raise_for_status.return_value = None
    response.iter_content.return_value = chunks or [b"abc"]
    return response


# NOTA: `download_arquivo` usa `requests.Session()` por tentativa, não
# `requests.get` diretamente — ver nota detalhada em test_arquivos.py. Os
# testes abaixo usam o fixture `mock_session_factory` (tests/conftest.py) e
# mockam `util.arquivos.sleep`, evitando bater na rede de verdade e pagar
# os ~5x sleep/backoff reais entre tentativas (antes: ~165s só neste
# arquivo; agora: frações de segundo).


def test_download_arquivo_erro_ao_abrir_arquivo(monkeypatch, mock_session_factory):
    """
    Erro ao abrir arquivo para escrita.
    """
    mock_session_cls = mock_session_factory(response=mock_response())

    def erro_open(*args, **kwargs):
        raise OSError("erro ao abrir")

    monkeypatch.setattr(builtins, "open", erro_open)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
    ):
        resultado = download_arquivo("http://teste", "arquivo")

    assert resultado is None


def test_download_arquivo_erro_durante_iteracao(mock_session_factory):
    """
    Erro levantado dentro do iter_content.
    """
    response = mock_response()
    response.iter_content.side_effect = RuntimeError("erro durante stream")

    mock_session_cls = mock_session_factory(response=response)

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
        patch("util.arquivos.os.makedirs"),
    ):
        resultado = download_arquivo("http://teste", "arquivo")

    assert resultado is None


def test_download_arquivo_erro_remover_arquivo_incompleto(
    monkeypatch, mock_session_factory
):
    """
    Falha ao remover arquivo após erro (branch defensivo duplo).
    """
    mock_session_cls = mock_session_factory(response=mock_response())

    # open funciona, mas write falha
    mock_file = MagicMock()
    mock_file.write.side_effect = IOError("erro escrita")

    monkeypatch.setattr(builtins, "open", lambda *a, **k: mock_file)
    monkeypatch.setattr(
        os, "remove", lambda _: (_ for _ in ()).throw(PermissionError())
    )

    with (
        patch("util.arquivos.requests.Session", mock_session_cls),
        patch("util.arquivos.sleep"),
        patch("util.arquivos.os.makedirs"),
    ):
        resultado = download_arquivo("http://teste", "arquivo")

    assert resultado is None
