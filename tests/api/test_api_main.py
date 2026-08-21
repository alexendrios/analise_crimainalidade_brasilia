from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.services import classificacao_service, forecast_service

client = TestClient(app)


@pytest.fixture(autouse=True)
def limpar_cache():
    forecast_service.limpar_cache()
    classificacao_service.limpar_cache()
    yield
    forecast_service.limpar_cache()
    classificacao_service.limpar_cache()


def test_raiz_endpoint():
    resp = client.get("/")

    assert resp.status_code == 200
    assert "mensagem" in resp.json()


def test_health_banco_ok():
    with patch("api.main.listar_tabelas", return_value=["violencia_contra_mulher_gold"]):
        resp = client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["database"] == "ok"


def test_health_banco_indisponivel_nao_derruba_api():
    with patch("api.main.listar_tabelas", side_effect=Exception("conexão recusada")):
        resp = client.get("/health")

    assert resp.status_code == 200
    assert "erro" in resp.json()["database"]


def test_listar_tabelas_gold():
    with patch(
        "api.services.gold_service.listar_tabelas",
        return_value=["violencia_contra_mulher_gold"],
    ):
        resp = client.get("/gold/tabelas")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] > 0
    nomes = {t["nome"] for t in body["tabelas"]}
    assert "violencia_contra_mulher_gold" in nomes


def test_resumo_tabela_invalida_retorna_404():
    resp = client.get("/gold/tabela_que_nao_existe/resumo")

    assert resp.status_code == 404


def test_resumo_tabela_sucesso():
    resumo_fake = {
        "tabela": "violencia_contra_mulher_gold",
        "linhas": 5,
        "colunas": 3,
        "nulos_total": 0,
        "colunas_com_nulos": 0,
        "tempo_execucao_s": 0.02,
    }
    with patch("api.services.gold_service.analisar_tabela", return_value=resumo_fake):
        resp = client.get("/gold/violencia_contra_mulher_gold/resumo")

    assert resp.status_code == 200
    assert resp.json() == resumo_fake


def test_resumo_tabela_banco_indisponivel_retorna_503():
    with patch(
        "api.services.gold_service.analisar_tabela", side_effect=Exception("timeout")
    ):
        resp = client.get("/gold/violencia_contra_mulher_gold/resumo")

    assert resp.status_code == 503


def test_dados_tabela_nao_materializada_retorna_503():
    with patch("api.services.gold_service.Repository.load", return_value=None):
        resp = client.get("/gold/violencia_contra_mulher_gold/dados")

    assert resp.status_code == 503


def test_dados_tabela_invalida_retorna_404():
    resp = client.get("/gold/tabela_que_nao_existe/dados")

    assert resp.status_code == 404


def test_dados_tabela_sucesso_com_paginacao():
    df = pd.DataFrame(
        {
            "ano": [2020, 2021, 2022],
            "regiao_administrativa": ["CEILANDIA", "TAGUATINGA", "CEILANDIA"],
            "crimes_contra_mulher": [10, 20, 15],
        }
    )
    with patch("api.services.gold_service.Repository.load", return_value=df):
        resp = client.get(
            "/gold/violencia_contra_mulher_gold/dados",
            params={"pagina": 1, "tamanho_pagina": 2},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total_linhas"] == 3
    assert body["total_paginas"] == 2
    assert len(body["registros"]) == 2


def test_dados_tabela_tamanho_pagina_invalido_retorna_422():
    resp = client.get(
        "/gold/violencia_contra_mulher_gold/dados", params={"tamanho_pagina": 0}
    )

    assert resp.status_code == 422


def test_previsao_dados_insuficientes_retorna_503():
    with patch(
        "api.services.forecast_service.Repository.load", return_value=None
    ):
        resp = client.get("/previsao/crimes-contra-mulher")

    assert resp.status_code == 503


def test_previsao_sucesso():
    forecast_payload = {
        "tabela_origem": "violencia_contra_mulher_gold",
        "coluna_alvo": "crimes_contra_mulher",
        "horizonte_anos": 3,
        "gerado_em": "2026-08-12T10:00:00",
        "cache_ate": "2026-08-12T10:30:00",
        "metricas_residual": {"mae": 0.1, "rmse": 0.2},
        "previsao": [
            {
                "ano": 2027,
                "valor_previsto": 100.0,
                "componente_prophet": 95.0,
                "residual_log_aplicado": 0.05,
            }
        ],
    }
    with patch(
        "api.services.forecast_service.gerar_previsao", return_value=forecast_payload
    ):
        resp = client.get("/previsao/crimes-contra-mulher", params={"horizonte_anos": 3})

    assert resp.status_code == 200
    assert resp.json()["horizonte_anos"] == 3
    assert len(resp.json()["previsao"]) == 1


def test_previsao_horizonte_fora_do_limite_retorna_422():
    resp = client.get("/previsao/crimes-contra-mulher", params={"horizonte_anos": 99})

    assert resp.status_code == 422


def test_previsao_erro_inesperado_retorna_500():
    with patch(
        "api.services.forecast_service.gerar_previsao",
        side_effect=RuntimeError("falha ao treinar"),
    ):
        resp = client.get("/previsao/crimes-contra-mulher")

    assert resp.status_code == 500


def test_retreinar_previsao_sucesso_chama_gerar_previsao_com_flags_corretas():
    forecast_payload = {
        "tabela_origem": "violencia_contra_mulher_gold",
        "coluna_alvo": "crimes_contra_mulher",
        "horizonte_anos": 5,
        "gerado_em": "2026-08-14T10:00:00",
        "cache_ate": "2026-08-14T10:30:00",
        "metricas_residual": {"mae": 0.1, "rmse": 0.2},
        "previsao": [
            {
                "ano": 2027,
                "valor_previsto": 100.0,
                "componente_prophet": 95.0,
                "residual_log_aplicado": 0.05,
            }
        ],
        "fonte_modelo": "retreino",
        "modelo_arquivo": "xgb_residual_log_20260814_100000.pkl",
    }
    with patch(
        "api.services.forecast_service.gerar_previsao", return_value=forecast_payload
    ) as mock_gerar:
        resp = client.post("/previsao/retrain", params={"horizonte_anos": 4})

    assert resp.status_code == 200
    assert resp.json()["fonte_modelo"] == "retreino"
    assert resp.json()["modelo_arquivo"] == "xgb_residual_log_20260814_100000.pkl"

    mock_gerar.assert_called_once_with(
        horizonte_anos=4,
        usar_cache=False,
        forcar_retreino=True,
        persistir_modelo=True,
    )


def test_retreinar_previsao_dados_insuficientes_retorna_503():
    with patch(
        "api.services.forecast_service.Repository.load", return_value=None
    ):
        resp = client.post("/previsao/retrain")

    assert resp.status_code == 503


def test_retreinar_previsao_erro_inesperado_retorna_500():
    with patch(
        "api.services.forecast_service.gerar_previsao",
        side_effect=RuntimeError("falha ao treinar"),
    ):
        resp = client.post("/previsao/retrain")

    assert resp.status_code == 500


def test_retreinar_previsao_horizonte_fora_do_limite_retorna_422():
    resp = client.post("/previsao/retrain", params={"horizonte_anos": 99})

    assert resp.status_code == 422


def test_modelos_treinados():
    payload = {
        "total": 1,
        "modelos": [
            {
                "arquivo": "xgb_residual_log_teste.pkl",
                "criado_em": "2026-01-01T00:00:00",
                "tipo_modelo": "XGBRegressor",
                "formato_artefato": "bundle",
                "metricas": {"mae": 0.1, "rmse": 0.2},
                "dataset_info": {"source_table": "violencia_contra_mulher_gold"},
            }
        ],
    }
    with patch(
        "api.services.forecast_service.listar_modelos_treinados", return_value=payload
    ):
        resp = client.get("/previsao/modelos")

    assert resp.status_code == 200
    assert resp.json() == payload


# ============================================================
# /classificacao — Regressão Logística
# ============================================================
def _payload_classificacao_fake():
    return {
        "tabelas_origem": ["crimes_letais_gold", "populacao_regiao_administrativa"],
        "total_registros": 310,
        "total_ras": 31,
        "periodo": [2015, 2024],
        "limiar_taxa_mediana": 10.66,
        "distribuicao_real": {"alta": 158, "baixa": 152},
        "metricas": {
            "cv_roc_auc_media": 0.994,
            "cv_roc_auc_std": 0.007,
            "holdout_accuracy": 0.962,
            "holdout_precision": 0.974,
            "holdout_recall": 0.95,
            "holdout_f1": 0.962,
            "holdout_roc_auc": 0.997,
        },
        "odds_ratios": {"taxa_homicidio": 198.7, "taxa_latrocinio": 2.86},
        "matriz_confusao": [[73, 3], [5, 95]],
        "classificacoes": [
            {
                "regiao_administrativa": "CEILANDIA",
                "ano": 2016,
                "classe_prevista": 1,
                "rotulo_previsto": "alta",
                "probabilidade_alta": 0.9987,
            },
            {
                "regiao_administrativa": "JARDIM BOTANICO",
                "ano": 2020,
                "classe_prevista": 0,
                "rotulo_previsto": "baixa",
                "probabilidade_alta": 0.0013,
            },
        ],
        "gerado_em": "2026-08-21T12:12:42",
        "cache_ate": "2026-08-21T12:42:42",
        "fonte_modelo": "artefato",
        "modelo_arquivo": "logreg_criminalidade_letal_20260821_1212.pkl",
    }


def test_classificacao_sucesso():
    payload = _payload_classificacao_fake()
    with patch(
        "api.services.classificacao_service.classificar_criminalidade",
        return_value=payload,
    ) as mock_classificar:
        resp = client.get("/classificacao/criminalidade-letal")

    assert resp.status_code == 200
    body = resp.json()
    assert body["fonte_modelo"] == "artefato"
    assert body["total_registros"] == 310
    assert len(body["classificacoes"]) == 2
    mock_classificar.assert_called_once_with(usar_cache=True)


def test_classificacao_sem_cache_passa_flag_para_o_servico():
    with patch(
        "api.services.classificacao_service.classificar_criminalidade",
        return_value=_payload_classificacao_fake(),
    ) as mock_classificar:
        client.get("/classificacao/criminalidade-letal", params={"usar_cache": False})

    mock_classificar.assert_called_once_with(usar_cache=False)


def test_classificacao_dados_insuficientes_retorna_503():
    with patch(
        "api.services.classificacao_service.classificar_criminalidade",
        side_effect=classificacao_service.DadosInsuficientesError("tabela vazia"),
    ):
        resp = client.get("/classificacao/criminalidade-letal")

    assert resp.status_code == 503
    assert "tabela vazia" in resp.json()["detail"]


def test_classificacao_erro_inesperado_retorna_500():
    with patch(
        "api.services.classificacao_service.classificar_criminalidade",
        side_effect=RuntimeError("falha ao treinar"),
    ):
        resp = client.get("/classificacao/criminalidade-letal")

    assert resp.status_code == 500


def test_retreinar_classificacao_sucesso_chama_servico_com_flags_corretas():
    payload = _payload_classificacao_fake()
    payload["fonte_modelo"] = "retreino"
    payload["modelo_arquivo"] = "logreg_criminalidade_letal_novo.pkl"

    with patch(
        "api.services.classificacao_service.classificar_criminalidade",
        return_value=payload,
    ) as mock_classificar:
        resp = client.post("/classificacao/retrain")

    assert resp.status_code == 200
    body = resp.json()
    assert body["fonte_modelo"] == "retreino"
    assert body["modelo_arquivo"] == "logreg_criminalidade_letal_novo.pkl"

    mock_classificar.assert_called_once_with(
        usar_cache=False,
        forcar_retreino=True,
        persistir_modelo=True,
    )


def test_retreinar_classificacao_dados_insuficientes_retorna_503():
    with patch(
        "api.services.classificacao_service.classificar_criminalidade",
        side_effect=classificacao_service.DadosInsuficientesError("tabela vazia"),
    ):
        resp = client.post("/classificacao/retrain")

    assert resp.status_code == 503


def test_retreinar_classificacao_erro_inesperado_retorna_500():
    with patch(
        "api.services.classificacao_service.classificar_criminalidade",
        side_effect=RuntimeError("falha ao treinar"),
    ):
        resp = client.post("/classificacao/retrain")

    assert resp.status_code == 500


def test_politica_event_loop_aplicada_apenas_no_windows(monkeypatch):
    """Cobre o ramo em que sys.platform != 'win32' (a politica do Windows
    nao e aplicada). Recarrega o modulo e restaura o estado original."""
    import importlib
    import sys

    import api.main as modulo_api

    monkeypatch.setattr(sys, "platform", "linux")
    try:
        recarregado = importlib.reload(modulo_api)
        assert recarregado.app is not None
    finally:
        monkeypatch.undo()
        importlib.reload(modulo_api)
