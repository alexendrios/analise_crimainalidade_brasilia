import pytest
from unittest.mock import patch
from util.rotas import montar_url, gerar_urls_rotas

# -----------------------------
# FIXTURE DE CONFIGURAÇÃO MOCK
# -----------------------------
@pytest.fixture
def mock_config():
    return {
        "coleta": {
            "fontes": {
                "dados_abertos": {
                    "url": "http://dados.df.gov.br"
                }
            }
        },
        "rotas": {
            "r1": {"url": "http://teste1", "arquivo": "arq1"},
            "r2": {"dataset": "ds", "resource": "res", "arquivo": "arq2"}
        }
    }

# -----------------------------
# TESTES montar_url
# -----------------------------
@patch("util.rotas.config")
def test_montar_url_basico(mocked_config, mock_config):
    mocked_config.__getitem__.side_effect = mock_config.__getitem__

    url = montar_url(
        dataset="meu_dataset",
        resource="12345-abc",
        arquivo="meu_arquivo.csv"
    )

    assert url == "http://dados.df.gov.br/dataset/meu_dataset/resource/12345-abc/download/meu_arquivo.csv"


@patch("util.rotas.config")
def test_montar_url_com_arquivo_xlsx(mocked_config, mock_config):
    mocked_config.__getitem__.side_effect = mock_config.__getitem__

    url = montar_url(
        dataset="dados_transito",
        resource="res987",
        arquivo="fluxo.xlsx"
    )

    assert url.endswith("/dataset/dados_transito/resource/res987/download/fluxo.xlsx")


@patch("util.rotas.config")
def test_montar_url_ignora_espacos(mocked_config, mock_config):
    mocked_config.__getitem__.side_effect = mock_config.__getitem__

    url = montar_url(
        dataset=" dataset_com_espaco ",
        resource=" recurso ",
        arquivo=" arquivo.csv "
    )

    # Mantendo os espaços, pois a função atual não faz strip
    assert "/dataset/ dataset_com_espaco /resource/ recurso /download/ arquivo.csv " in url

# -----------------------------
# TESTES gerar_urls_rotas
# -----------------------------
def test_gerar_urls():
    rotas_mock = {
        "r1": {"url": "http://teste1", "arquivo": "arq1"},
        "r2": {"dataset": "ds", "resource": "res", "arquivo": "arq2"}
    }

    with patch("util.rotas.montar_url", return_value="http://montado"):
        urls = gerar_urls_rotas(rotas_mock)

    assert urls[0] == ("http://teste1", "arq1")
    assert urls[1] == ("http://montado", "arq2")

