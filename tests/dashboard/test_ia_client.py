from unittest.mock import Mock, patch

import pytest
import requests

from dashboard.ia_client import (
    OllamaError,
    gerar_resumo_ia,
    listar_modelos_ollama,
)


def _resposta(status_code=200, corpo=None, texto=""):
    mock = Mock()
    mock.status_code = status_code
    mock.json.return_value = corpo if corpo is not None else {}
    mock.text = texto
    return mock


def test_gerar_resumo_ia_envia_payload_e_extrai_resposta():
    with patch("dashboard.ia_client.requests.post") as post:
        post.return_value = _resposta(corpo={"response": "  Resumo executivo.  "})
        resultado = gerar_resumo_ia("DADOS DE TESTE", modelo="m-teste")

    assert resultado == "Resumo executivo."
    payload = post.call_args.kwargs["json"]
    assert payload["model"] == "m-teste"
    assert payload["stream"] is False
    assert payload["options"]["temperature"] < 1
    assert "DADOS DE TESTE" in payload["prompt"]
    assert post.call_args.kwargs["timeout"] > 0


def test_gerar_resumo_ia_erro_de_conexao_levanta_ollama_error():
    with patch(
        "dashboard.ia_client.requests.post",
        side_effect=requests.ConnectionError("boom"),
    ):
        with pytest.raises(OllamaError, match="Falha de conexão"):
            gerar_resumo_ia("ctx")


def test_gerar_resumo_ia_status_diferente_de_200_levanta_ollama_error():
    with patch("dashboard.ia_client.requests.post") as post:
        post.return_value = _resposta(status_code=404, texto="not found")
        with pytest.raises(OllamaError, match="HTTP 404"):
            gerar_resumo_ia("ctx")


def test_gerar_resumo_ia_corpo_nao_json_levanta_ollama_error():
    with patch("dashboard.ia_client.requests.post") as post:
        resposta = _resposta()
        resposta.json.side_effect = ValueError("inválido")
        post.return_value = resposta
        with pytest.raises(OllamaError, match="não é JSON válido"):
            gerar_resumo_ia("ctx")


def test_gerar_resumo_ia_resposta_vazia_levanta_ollama_error():
    with patch("dashboard.ia_client.requests.post") as post:
        post.return_value = _resposta(corpo={"response": "   "})
        with pytest.raises(OllamaError, match="vazia"):
            gerar_resumo_ia("ctx")


def test_listar_modelos_ollama_extrai_nomes_validos():
    corpo = {
        "models": [
            {"name": "llama3.2:latest"},
            {"name": "qwen2.5"},
            {"sem": "nome"},
        ]
    }
    with patch("dashboard.ia_client.requests.get") as get:
        get.return_value = _resposta(corpo=corpo)
        modelos = listar_modelos_ollama()

    assert modelos == ["llama3.2:latest", "qwen2.5"]


def test_listar_modelos_ollama_fora_do_ar_retorna_lista_vazia():
    with patch(
        "dashboard.ia_client.requests.get",
        side_effect=requests.ConnectionError("boom"),
    ):
        assert listar_modelos_ollama() == []


def test_listar_modelos_ollama_status_de_erro_retorna_lista_vazia():
    with patch("dashboard.ia_client.requests.get") as get:
        get.return_value = _resposta(status_code=500, texto="erro")
        assert listar_modelos_ollama() == []


def test_listar_modelos_ollama_corpo_invalido_retorna_lista_vazia():
    with patch("dashboard.ia_client.requests.get") as get:
        resposta = _resposta()
        resposta.json.side_effect = ValueError("inválido")
        get.return_value = resposta
        assert listar_modelos_ollama() == []
