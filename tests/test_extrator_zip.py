import os
import zipfile
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from util.extrator_zip import extrair_zip_seguro, arquivos_zip_execucao


# =====================================================
# 🧰 Fixtures
# =====================================================
@pytest.fixture
def temp_dirs(tmp_path, monkeypatch):
    """
    Cria diretórios temporários para ZIP e destino
    e injeta no módulo testado.
    """
    zip_dir = tmp_path / "zip"
    planilha_dir = tmp_path / "planilha"

    zip_dir.mkdir()
    planilha_dir.mkdir()

    monkeypatch.setattr("util.extrator_zip.diretorio_zip", str(zip_dir))
    monkeypatch.setattr("util.extrator_zip.diretorio_destino", str(planilha_dir))

    return zip_dir, planilha_dir


def criar_zip(caminho_zip: Path, arquivos: dict[str, str]):
    """
    Cria um ZIP simples com conteúdo textual.
    """
    with zipfile.ZipFile(caminho_zip, "w") as z:
        for nome, conteudo in arquivos.items():
            z.writestr(nome, conteudo)


# =====================================================
# ✅ Testes extrair_zip_seguro
# =====================================================
def test_extrair_zip_seguro_extrai_arquivo(temp_dirs):
    zip_dir, planilha_dir = temp_dirs

    zip_path = zip_dir / "teste.zip"
    criar_zip(zip_path, {"arquivo.txt": "conteudo"})

    extrair_zip_seguro(str(zip_path), str(planilha_dir))

    arquivo_extraido = planilha_dir / "arquivo.txt"
    assert arquivo_extraido.exists()
    assert arquivo_extraido.read_text() == "conteudo"


def test_extrair_zip_seguro_remove_arquivo_existente(temp_dirs):
    zip_dir, planilha_dir = temp_dirs

    arquivo_existente = planilha_dir / "arquivo.txt"
    arquivo_existente.write_text("antigo")

    zip_path = zip_dir / "teste.zip"
    criar_zip(zip_path, {"arquivo.txt": "novo"})

    extrair_zip_seguro(str(zip_path), str(planilha_dir))

    assert arquivo_existente.read_text() == "novo"


def test_extrair_zip_seguro_detecta_zip_slip(temp_dirs):
    zip_dir, planilha_dir = temp_dirs

    zip_path = zip_dir / "malicioso.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("../evil.txt", "ataque")

    with pytest.raises(Exception, match="Zip Slip"):
        extrair_zip_seguro(str(zip_path), str(planilha_dir))


def test_extrair_zip_seguro_arquivo_nao_zip(tmp_path):
    fake_zip = tmp_path / "fake.zip"
    fake_zip.write_text("nao sou zip")

    with pytest.raises(zipfile.BadZipFile):
        extrair_zip_seguro(fake_zip, tmp_path)


def test_extrair_zip_seguro_exception_generica(tmp_path):
    fake_zip = tmp_path / "arquivo.zip"
    fake_zip.touch()

    with patch("util.extrator_zip.zipfile.ZipFile", side_effect=Exception("erro")):
        with pytest.raises(Exception):
            extrair_zip_seguro(fake_zip, tmp_path)


def test_extrair_zip_seguro_zip_vazio(tmp_path):
    zip_path = tmp_path / "vazio.zip"

    with zipfile.ZipFile(zip_path, "w"):
        pass

    extrair_zip_seguro(zip_path, tmp_path)

    # Apenas o ZIP deve existir
    assert list(tmp_path.iterdir()) == [zip_path]


def test_extrair_zip_seguro_erro_durante_extracao(tmp_path):
    fake_zip = tmp_path / "arquivo.zip"
    fake_zip.touch()

    mock_zip = MagicMock()
    mock_zip.__enter__.return_value = mock_zip
    mock_zip.namelist.return_value = ["arquivo.txt"]
    mock_zip.extractall.side_effect = Exception("erro ao extrair")

    with patch("util.extrator_zip.zipfile.ZipFile", return_value=mock_zip):
        with pytest.raises(Exception):
            extrair_zip_seguro(fake_zip, tmp_path)


# =====================================================
# ▶️ Testes arquivos_zip_execucao
# =====================================================
def test_arquivos_zip_execucao_extrai_todos(temp_dirs):
    zip_dir, planilha_dir = temp_dirs

    criar_zip(zip_dir / "a.zip", {"a.txt": "1"})
    criar_zip(zip_dir / "b.zip", {"b.txt": "2"})

    arquivos_zip_execucao()

    assert (planilha_dir / "a.txt").exists()
    assert (planilha_dir / "b.txt").exists()


def test_arquivos_zip_execucao_sem_zips(temp_dirs):
    arquivos_zip_execucao()  # não deve lançar exceção


def test_arquivos_zip_execucao_diretorio_inexistente(monkeypatch, tmp_path):
    diretorio_inexistente = tmp_path / "nao_existe"

    monkeypatch.setattr(
        "util.extrator_zip.diretorio_zip",
        str(diretorio_inexistente),
    )

    arquivos_zip_execucao()  # cobre return antecipado

def test_extrair_zip_seguro_permission_error_ao_remover(tmp_path):
    """
    Garante cobertura do bloco except PermissionError
    """

    zip_path = tmp_path / "teste.zip"
    destino = tmp_path / "destino"
    destino.mkdir()

    # cria zip com um arquivo
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("arquivo.txt", "novo")

    # cria arquivo existente no destino
    arquivo_existente = destino / "arquivo.txt"
    arquivo_existente.write_text("antigo")

    # força PermissionError no os.remove
    with patch("util.extrator_zip.os.remove", side_effect=PermissionError):
        with pytest.raises(PermissionError):
            extrair_zip_seguro(str(zip_path), str(destino))
