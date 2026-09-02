import pandas as pd
import pytest
from contextlib import ExitStack
from fastapi.testclient import TestClient
from unittest.mock import patch

from api.main import app
from api.services import qualidade_service
from api.services.analise_service import DadosIndisponiveisError
from validation.esquemas import GOLD as ESQUEMAS_GOLD_REAL

client = TestClient(app)


@pytest.fixture(autouse=True)
def limpar_cache():
    qualidade_service.limpar_cache()
    yield
    qualidade_service.limpar_cache()


def _tabela_perfeita():
    anos = list(range(2015, 2025))
    agora = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)
    return pd.DataFrame(
        {
            "ano": anos,
            "regiao_administrativa": ["PLANO PILOTO"] * len(anos),
            "ocorrencia_homicidio": [20 + i for i in range(len(anos))],
            "ocorrencia_latrocinio": [5] * len(anos),
            "ocorrencia_lesao_morte": [5] * len(anos),
            "inserido_em": [agora] * len(anos),
        }
    )


CATALOGO = ["crimes_letais_gold", "desaparecidos_regiao_gold"]
ESQUEMAS = {"crimes_letais_gold": ESQUEMAS_GOLD_REAL["crimes_letais_gold"]}


def _pads(carregadas=None):
    carregadas = carregadas or {"crimes_letais_gold": _tabela_perfeita()}
    return [
        patch.object(qualidade_service, "TABELAS_GOLD", CATALOGO),
        patch.object(qualidade_service, "ESQUEMAS_GOLD", ESQUEMAS),
        patch.object(qualidade_service, "_carregar_tabelas", return_value=carregadas),
    ]


def _entrar_pads(carregadas=None):
    stack = ExitStack()
    for pad in _pads(carregadas):
        stack.enter_context(pad)
    return stack


# ============================================================
# Serviço
# ============================================================
def test_obter_qualidade_dados_sucesso():
    with _entrar_pads():
        resultado = qualidade_service.obter_qualidade_dados()

    assert resultado["total_tabelas"] == 2
    assert resultado["materializadas"] == 1
    assert resultado["escore_geral"] == 50.0
    assert len(resultado["dimensoes"]) == 6

    letais = next(t for t in resultado["tabelas"] if t["tabela"] == "crimes_letais_gold")
    assert letais["materializada"] is True
    assert letais["escore"] == 100.0
    assert letais["problemas"] == []

    desaparecidos = next(
        t for t in resultado["tabelas"] if t["tabela"] == "desaparecidos_regiao_gold"
    )
    assert desaparecidos["materializada"] is False
    assert desaparecidos["escore"] == 0.0


def test_obter_qualidade_dados_todas_ausentes_escore_zero():
    ausentes = {"crimes_letais_gold": None, "desaparecidos_regiao_gold": None}
    with _entrar_pads(carregadas=ausentes):
        resultado = qualidade_service.obter_qualidade_dados()

    assert resultado["materializadas"] == 0
    assert resultado["escore_geral"] == 0.0


def test_obter_qualidade_dados_cache_evita_recarregamento():
    stack = _entrar_pads()
    mock_carregar = stack.enter_context(
        patch.object(qualidade_service, "_carregar_tabelas")
    )
    with stack:
        primeiro = qualidade_service.obter_qualidade_dados()
        segundo = qualidade_service.obter_qualidade_dados()

    assert primeiro == segundo
    assert mock_carregar.call_count == 1


def test_obter_qualidade_dados_falha_ao_carregar_levanta_erro():
    stack = _entrar_pads()
    mock_carregar = stack.enter_context(
        patch.object(qualidade_service, "_carregar_tabelas")
    )
    mock_carregar.side_effect = RuntimeError("banco fora do ar")
    with stack:
        with pytest.raises(DadosIndisponiveisError):
            qualidade_service.obter_qualidade_dados()


# ============================================================
# Endpoints
# ============================================================
def test_endpoint_qualidade_dados_sucesso():
    with _entrar_pads():
        resp = client.get("/qualidade/dados")

    assert resp.status_code == 200
    body = resp.json()
    assert body["escore_geral"] == 50.0
    assert body["total_tabelas"] == 2
    assert body["materializadas"] == 1
    assert len(body["dimensoes"]) == 6
    assert len(body["tabelas"]) == 2
    assert body["tabelas"][1]["materializada"] is False
    assert "gerado_em" in body
    assert body["tabelas"][0]["dimensoes"][0]["chave"] == "completude"


def test_endpoint_qualidade_dados_indisponivel_retorna_503():
    stack = _entrar_pads()
    mock_carregar = stack.enter_context(
        patch.object(qualidade_service, "_carregar_tabelas")
    )
    mock_carregar.side_effect = DadosIndisponiveisError("sem banco")
    with stack:
        resp = client.get("/qualidade/dados")

    assert resp.status_code == 503
    assert "sem banco" in resp.json()["detail"]