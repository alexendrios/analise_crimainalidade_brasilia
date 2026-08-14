from unittest.mock import Mock, patch

import pytest
import requests

from dashboard.api_client import (
    ApiError,
    _montar_url,
    health,
    listar_modelos,
    listar_tabelas,
    obter_dados,
    obter_previsao,
    obter_resumo,
)


def _resposta_ok(payload):
    mock = Mock()
    mock.status_code = 200
    mock.json.return_value = payload
    return mock


def test_montar_url_ignora_barras_redundantes():
    assert _montar_url("http://localhost:8000/", "/health") == "http://localhost:8000/health"
    assert _montar_url("http://localhost:8000", "health") == "http://localhost:8000/health"


def test_health_retorna_payload():
    with patch("dashboard.api_client.requests.get", return_value=_resposta_ok({"status": "ok"})) as mock_get:
        resultado = health("http://localhost:8000")

    assert resultado["status"] == "ok"
    mock_get.assert_called_once()


def test_listar_tabelas_extrai_lista():
    payload = {"total": 1, "tabelas": [{"nome": "crimes_letais_gold", "disponivel_no_banco": True}]}
    with patch("dashboard.api_client.requests.get", return_value=_resposta_ok(payload)):
        tabelas = listar_tabelas()

    assert tabelas[0]["nome"] == "crimes_letais_gold"


def test_listar_tabelas_vazio_retorna_lista_vazia():
    with patch("dashboard.api_client.requests.get", return_value=_resposta_ok({"tabelas": None})):
        assert listar_tabelas() == []


def test_obter_resumo_retorna_payload():
    payload = {"tabela": "crimes_letais_gold", "linhas": 10}
    with patch("dashboard.api_client.requests.get", return_value=_resposta_ok(payload)) as mock_get:
        resumo = obter_resumo("crimes_letais_gold")

    assert resumo["linhas"] == 10
    assert "/gold/crimes_letais_gold/resumo" in mock_get.call_args[0][0]


def test_obter_dados_monta_params_completos():
    with patch("dashboard.api_client.requests.get", return_value=_resposta_ok({"registros": []})) as mock_get:
        obter_dados(
            "crimes_letais_gold",
            pagina=2,
            tamanho_pagina=500,
            ano_min=2020,
            ano_max=2023,
            regiao_administrativa="Taguatinga",
        )

    chamada = mock_get.call_args
    assert chamada[1]["params"] == {
        "pagina": 2,
        "tamanho_pagina": 500,
        "ano_min": 2020,
        "ano_max": 2023,
        "regiao_administrativa": "Taguatinga",
    }


def test_obter_dados_omite_filtros_ausentes():
    with patch("dashboard.api_client.requests.get", return_value=_resposta_ok({"registros": []})) as mock_get:
        obter_dados("crimes_letais_gold", base_url="http://api:9999")

    chamada = mock_get.call_args
    assert chamada[1]["params"] == {"pagina": 1, "tamanho_pagina": 1000}
    assert chamada[0][0].startswith("http://api:9999")


def test_obter_previsao_envia_params():
    with patch("dashboard.api_client.requests.get", return_value=_resposta_ok({"previsao": []})) as mock_get:
        obter_previsao(horizonte_anos=7, usar_cache=False)

    assert mock_get.call_args[1]["params"] == {
        "horizonte_anos": 7,
        "usar_cache": "false",
    }


def test_listar_modelos_retorna_lista():
    payload = {"total": 1, "modelos": [{"arquivo": "x.pkl"}]}
    with patch("dashboard.api_client.requests.get", return_value=_resposta_ok(payload)):
        assert listar_modelos()[0]["arquivo"] == "x.pkl"


def test_erro_de_rede_levanta_api_error():
    with patch("dashboard.api_client.requests.get", side_effect=requests.RequestException("sem rede")):
        with pytest.raises(ApiError, match="Falha de conexão"):
            health()


def test_status_diferente_de_200_levanta_api_error_com_detail():
    mock = Mock()
    mock.status_code = 404
    mock.json.return_value = {"detail": "não existe"}
    with patch("dashboard.api_client.requests.get", return_value=mock):
        with pytest.raises(ApiError, match="HTTP 404.*não existe"):
            health()


def test_resposta_nao_json_levanta_api_error():
    mock = Mock()
    mock.status_code = 200
    mock.json.side_effect = ValueError("invalid json")
    with patch("dashboard.api_client.requests.get", return_value=mock):
        with pytest.raises(ApiError, match="não é JSON"):
            health()
