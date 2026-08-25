import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

from api.main import app
from api.services import analise_service

client = TestClient(app)


@pytest.fixture(autouse=True)
def limpar_cache():
    analise_service.limpar_cache()
    yield
    analise_service.limpar_cache()


@pytest.fixture
def dados_gold():
    rng = np.random.default_rng(42)
    anos = np.arange(2015, 2023)
    ras = ["CEILANDIA", "TAGUATINGA", "AS SUL"]
    roubo = pd.DataFrame(
        [
            {
                "ano": ano,
                "regiao_administrativa": ra,
                "ocorrencia_roubo_pedestre": int(rng.integers(50, 200)),
                "ocorrencia_roubo_comercio": int(rng.integers(20, 90)),
                "ocorrencia_roubo_transporte_coletivo": int(rng.integers(0, 25)),
                "ocorrencia_roubo_veiculo": int(rng.integers(30, 120)),
                "ocorrencia_furto_em_veiculo": int(rng.integers(40, 150)),
            }
            for ano in anos
            for ra in ras
        ]
    )
    mulher = pd.DataFrame(
        {
            "ano": anos,
            "crimes_contra_mulher": rng.integers(300, 500, len(anos)),
            "casos_feminicidios": rng.integers(10, 40, len(anos)),
        }
    )
    mensais = pd.DataFrame(
        {
            "fato": rng.integers(5, 60, 36).astype(float),
            "registro": rng.integers(1, 10, 36).astype(float),
            "mes_num": np.tile(np.arange(1, 13), 3),
            "ano": np.repeat([2020, 2021, 2022], 12),
        }
    )
    return {
        "violencia_contra_mulher_gold": mulher,
        "crimes_roubo_furto_gold": roubo,
        "crimes_letais_gold": None,
        "crimes_discriminatorios_gold": None,
        "violencia_idosos_gold": None,
        "violencia_idosos_mensais_gold": mensais,
    }


def _patch_dados(dados_gold):
    return patch(
        "api.services.analise_service.carregar_tabelas_gold", return_value=dados_gold
    )


# ============================================================
# Serviço
# ============================================================
def test_obter_correlacoes_sucesso(dados_gold):
    with _patch_dados(dados_gold):
        resultado = analise_service.obter_correlacoes()

    assert resultado["metodo"] == "pearson"
    assert resultado["periodo"] == [2015, 2022]
    assert set(resultado["matriz_correlacao"]) == set(resultado["indicadores"])
    diagonal = {
        resultado["matriz_correlacao"][ind][ind] for ind in resultado["indicadores"]
    }
    assert all(abs(valor - 1.0) < 1e-6 for valor in diagonal)
    absolutos = [abs(par["correlacao"]) for par in resultado["pares_destaque"]]
    assert absolutos == sorted(absolutos, reverse=True)
    assert isinstance(resultado["insights"], list) and resultado["insights"]


def test_obter_correlacoes_serie_historica_json_safe(dados_gold):
    with _patch_dados(dados_gold):
        resultado = analise_service.obter_correlacoes()

    linha = resultado["serie_historica"][0]
    assert isinstance(linha["ano"], int)
    assert all(isinstance(valor, (int, float)) for chave, valor in linha.items() if chave != "ano")


def test_obter_correlacoes_metodo_spearman(dados_gold):
    with _patch_dados(dados_gold):
        resultado = analise_service.obter_correlacoes(metodo="spearman")

    assert resultado["metodo"] == "spearman"


def test_obter_correlacoes_sem_tabelas_levantam_erro():
    with patch(
        "api.services.analise_service.carregar_tabelas_gold",
        return_value={"a_gold": None, "b_gold": None},
    ):
        with pytest.raises(analise_service.DadosIndisponiveisError):
            analise_service.obter_correlacoes()


def test_obter_granger_filtra_significantes(dados_gold):
    with _patch_dados(dados_gold):
        filtrado = analise_service.obter_granger(apenas_significantes=True)
        completo = analise_service.obter_granger(apenas_significantes=False)

    assert completo["total_pares"] > filtrado["total_pares"]
    assert all(par["significante"] for par in filtrado["pares"])
    assert filtrado["total_significantes"] == filtrado["total_pares"]


def test_obter_anomalias_sucesso(dados_gold):
    with _patch_dados(dados_gold):
        resultado = analise_service.obter_anomalias()

    assert resultado["total_painel"] == len(resultado["painel"])
    assert resultado["total_mensal"] == len(resultado["mensal"])
    assert resultado["total_mensal"] > 0
    assert all("regiao_administrativa" in linha for linha in resultado["painel"])
    assert all("anomalia" not in linha and "score" not in linha for linha in resultado["painel"])


def test_obter_anomalias_sem_tabela_painel_levantam_erro(dados_gold):
    dados_gold["crimes_roubo_furto_gold"] = None
    with _patch_dados(dados_gold):
        with pytest.raises(analise_service.DadosIndisponiveisError):
            analise_service.obter_anomalias()


def test_obter_anomalias_sem_mensais_retorna_vazio(dados_gold):
    dados_gold["violencia_idosos_mensais_gold"] = None
    with _patch_dados(dados_gold):
        resultado = analise_service.obter_anomalias()

    assert resultado["total_mensal"] == 0
    assert resultado["mensal"] == []
    assert resultado["total_painel"] > 0


def test_obter_zonas_quentes_sucesso(dados_gold):
    with _patch_dados(dados_gold):
        resultado = analise_service.obter_zonas_quentes(tamanho_celula_km=2.0, top_n=5)

    assert resultado["ano_referencia"] == 2022
    assert resultado["tamanho_celula_km"] == 2.0
    assert len(resultado["zonas"]) <= 5
    valores = [zona["ocorrencia_roubo_pedestre"] for zona in resultado["zonas"]]
    assert valores == sorted(valores, reverse=True)


def test_cache_evita_recalculo_entre_chamadas_identicas(dados_gold):
    with _patch_dados(dados_gold) as mock_carregar:
        primeiro = analise_service.obter_anomalias(limite=50)
        segundo = analise_service.obter_anomalias(limite=50)

    assert primeiro == segundo
    assert mock_carregar.call_count == 1


def test_chaves_diferentes_nao_colidem_no_cache(dados_gold):
    with _patch_dados(dados_gold) as mock_carregar:
        analise_service.obter_anomalias(limite=10)
        analise_service._cache_dados = None
        analise_service.obter_anomalias(limite=20)

    assert mock_carregar.call_count == 2


# ============================================================
# Endpoints
# ============================================================
def test_endpoint_correlacoes_sucesso(dados_gold):
    with _patch_dados(dados_gold), patch(
        "api.services.analise_service.insights_correlacao", return_value=["insight fake"]
    ):
        resp = client.get("/analise/correlacoes", params={"top_n": 3})

    assert resp.status_code == 200
    body = resp.json()
    assert len(body["pares_destaque"]) <= 3
    assert body["insights"] == ["insight fake"]


def test_endpoint_correlacoes_metodo_invalido_retorna_422():
    resp = client.get("/analise/correlacoes", params={"metodo": "kendall"})

    assert resp.status_code == 422


def test_endpoint_correlacoes_dados_indisponiveis_retorna_503():
    with patch(
        "api.services.analise_service.carregar_tabelas_gold",
        side_effect=analise_service.DadosIndisponiveisError("sem banco"),
    ):
        resp = client.get("/analise/correlacoes")

    assert resp.status_code == 503
    assert "sem banco" in resp.json()["detail"]


def test_endpoint_granger_sucesso(dados_gold):
    with _patch_dados(dados_gold):
        resp = client.get("/analise/granger", params={"apenas_significantes": False})

    assert resp.status_code == 200
    body = resp.json()
    assert body["total_pares"] == len(body["pares"])
    assert body["alpha"] == 0.05


def test_endpoint_anomalias_sucesso(dados_gold):
    with _patch_dados(dados_gold):
        resp = client.get("/analise/anomalias", params={"limite": 5})

    assert resp.status_code == 200
    assert resp.json()["total_painel"] > 0


def test_endpoint_zonas_quentes_parametro_invalido_retorna_422():
    resp = client.get("/analise/zonas-quentes", params={"tamanho_celula_km": 0})

    assert resp.status_code == 422


def test_endpoint_zonas_quentes_sucesso(dados_gold):
    with _patch_dados(dados_gold):
        resp = client.get("/analise/zonas-quentes", params={"top_n": 2})

    assert resp.status_code == 200
    body = resp.json()
    assert len(body["zonas"]) <= 2
    assert body["celulas_com_ocorrencias"] >= len(body["zonas"])
