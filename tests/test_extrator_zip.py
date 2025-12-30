import os
import zipfile
import pytest
from unittest.mock import patch, MagicMock

from util.extrator_zip import extrair_zip_seguro


# ============================================================
# FIXTURES
# ============================================================
@pytest.fixture
def tmp_zip_dir(tmp_path):
    zip_dir = tmp_path / "zip"
    zip_dir.mkdir()
    return zip_dir


@pytest.fixture
def tmp_destino_dir(tmp_path):
    destino = tmp_path / "destino"
    destino.mkdir()
    return destino


def criar_zip(caminho_zip, arquivos):
    """
    Cria um ZIP com os arquivos informados.
    arquivos = { "nome.txt": "conteudo" }
    """
    with zipfile.ZipFile(caminho_zip, "w") as z:
        for nome, conteudo in arquivos.items():
            z.writestr(nome, conteudo)


# ============================================================
# TESTE — extração bem-sucedida
# ============================================================
@patch("util.extrator_zip.logger")
def test_extrair_zip_sucesso(mock_logger, tmp_zip_dir, tmp_destino_dir):
    zip_path = tmp_zip_dir / "teste.zip"

    criar_zip(zip_path, {"arquivo1.txt": "dados", "arquivo2.csv": "1;2;3"})

    extrair_zip_seguro(str(zip_path), str(tmp_destino_dir))

    assert (tmp_destino_dir / "arquivo1.txt").exists()
    assert (tmp_destino_dir / "arquivo2.csv").exists()

    assert mock_logger.info.call_count > 0


# ============================================================
# TESTE — Zip Slip detectado
# ============================================================
@patch("util.extrator_zip.logger")
def test_extrair_zip_zip_slip_detectado(mock_logger, tmp_zip_dir, tmp_destino_dir):
    zip_path = tmp_zip_dir / "malicioso.zip"

    criar_zip(zip_path, {"../hack.txt": "ataque"})

    with pytest.raises(Exception, match="Zip Slip"):
        extrair_zip_seguro(str(zip_path), str(tmp_destino_dir))

    mock_logger.error.assert_called_once()


# ============================================================
# TESTE — ZIP vazio
# ============================================================
@patch("util.extrator_zip.logger")
def test_extrair_zip_vazio(mock_logger, tmp_zip_dir, tmp_destino_dir):
    zip_path = tmp_zip_dir / "vazio.zip"

    criar_zip(zip_path, {})

    extrair_zip_seguro(str(zip_path), str(tmp_destino_dir))

    # Nenhum arquivo criado
    assert list(tmp_destino_dir.iterdir()) == []

    assert mock_logger.info.call_count > 0


# ============================================================
# TESTE — múltiplos arquivos no ZIP
# ============================================================
@patch("util.extrator_zip.logger")
def test_extrair_zip_multiplos_arquivos(mock_logger, tmp_zip_dir, tmp_destino_dir):
    zip_path = tmp_zip_dir / "multi.zip"

    criar_zip(zip_path, {"a.txt": "a", "b/b.txt": "b", "c/c/c.txt": "c"})

    extrair_zip_seguro(str(zip_path), str(tmp_destino_dir))

    assert (tmp_destino_dir / "a.txt").exists()
    assert (tmp_destino_dir / "b/b.txt").exists()
    assert (tmp_destino_dir / "c/c/c.txt").exists()

    assert mock_logger.info.call_count > 0
