import json
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, mock_open, patch

from api.services import forecast_service


@pytest.fixture(autouse=True)
def limpar_cache():
    forecast_service.limpar_cache()
    yield
    forecast_service.limpar_cache()


def _df_base():
    return pd.DataFrame(
        {
            "ano": [2015, 2016, 2017, 2018, 2019, 2020],
            "crimes_contra_mulher": [100, 110, 105, 120, 130, 125],
            "casos_feminicidios": [2, 3, 1, 4, 2, 3],
        }
    )


def test_gerar_previsao_dados_ausentes():
    with patch("api.services.forecast_service.Repository.load", return_value=None):
        with pytest.raises(forecast_service.DadosInsuficientesError):
            forecast_service.gerar_previsao()


def test_gerar_previsao_dados_insuficientes_apos_preparo():
    df = pd.DataFrame(
        {"ano": [2020], "crimes_contra_mulher": [10], "casos_feminicidios": [1]}
    )

    with (
        patch("api.services.forecast_service.Repository.load", return_value=df),
        patch(
            "api.services.forecast_service.preparar_dados",
            return_value=df.iloc[0:0],
        ),
    ):
        with pytest.raises(forecast_service.DadosInsuficientesError):
            forecast_service.gerar_previsao()


def test_gerar_previsao_sucesso_e_cache():
    df = _df_base()
    df_preparado = df.copy()
    df_preparado["ano"] = pd.to_datetime(df_preparado["ano"], format="%Y")

    modelo_fake = MagicMock()
    prophet_fake = MagicMock()
    metrics_fake = {"mae": 0.1, "rmse": 0.2}
    forecast_fake = pd.DataFrame(
        {
            "ano": [pd.Timestamp("2021-01-01"), pd.Timestamp("2022-01-01")],
            "prophet": [130.0, 135.0],
            "residual_log": [0.01, 0.02],
            "final": [131.0, 136.0],
        }
    )

    with (
        patch("api.services.forecast_service.Repository.load", return_value=df),
        patch(
            "api.services.forecast_service.preparar_dados", return_value=df_preparado
        ),
        patch(
            "api.services.forecast_service.treinar_residual",
            return_value=(modelo_fake, prophet_fake, metrics_fake, -0.5, 0.5, {}),
        ),
        patch(
            "api.services.forecast_service.prever_futuro", return_value=forecast_fake
        ) as mock_prever,
    ):
        resultado = forecast_service.gerar_previsao(horizonte_anos=2)

        assert resultado["horizonte_anos"] == 2
        assert resultado["metricas_residual"] == metrics_fake
        assert len(resultado["previsao"]) == 2
        assert resultado["previsao"][0]["ano"] == 2021
        assert resultado["previsao"][0]["valor_previsto"] == 131.0
        mock_prever.assert_called_once()

        # Segunda chamada deve vir do cache, sem re-treinar
        resultado_cache = forecast_service.gerar_previsao(horizonte_anos=2)
        assert resultado_cache == resultado
        mock_prever.assert_called_once()  # não foi chamado de novo


def test_gerar_previsao_usar_cache_false_forca_retreino():
    df = _df_base()
    df_preparado = df.copy()
    df_preparado["ano"] = pd.to_datetime(df_preparado["ano"], format="%Y")

    forecast_fake = pd.DataFrame(
        {
            "ano": [pd.Timestamp("2021-01-01")],
            "prophet": [130.0],
            "residual_log": [0.01],
            "final": [131.0],
        }
    )

    with (
        patch("api.services.forecast_service.Repository.load", return_value=df),
        patch(
            "api.services.forecast_service.preparar_dados", return_value=df_preparado
        ),
        patch(
            "api.services.forecast_service.treinar_residual",
            return_value=(MagicMock(), MagicMock(), {"mae": 0.1, "rmse": 0.1}, -0.5, 0.5, {}),
        ),
        patch(
            "api.services.forecast_service.prever_futuro", return_value=forecast_fake
        ) as mock_prever,
    ):
        forecast_service.gerar_previsao(horizonte_anos=1)
        forecast_service.gerar_previsao(horizonte_anos=1, usar_cache=False)

    assert mock_prever.call_count == 2


def test_gerar_previsao_persistir_modelo_chama_save():
    df = _df_base()
    df_preparado = df.copy()
    df_preparado["ano"] = pd.to_datetime(df_preparado["ano"], format="%Y")

    forecast_fake = pd.DataFrame(
        {
            "ano": [pd.Timestamp("2021-01-01")],
            "prophet": [130.0],
            "residual_log": [0.01],
            "final": [131.0],
        }
    )

    with (
        patch("api.services.forecast_service.Repository.load", return_value=df),
        patch(
            "api.services.forecast_service.preparar_dados", return_value=df_preparado
        ),
        patch(
            "api.services.forecast_service.treinar_residual",
            return_value=(MagicMock(), MagicMock(), {"mae": 0.1, "rmse": 0.1}, -0.5, 0.5, {}),
        ),
        patch("api.services.forecast_service.prever_futuro", return_value=forecast_fake),
        patch("api.services.forecast_service.save_model_with_metadata") as mock_save,
    ):
        forecast_service.gerar_previsao(horizonte_anos=1, persistir_modelo=True)

    mock_save.assert_called_once()


def test_listar_modelos_treinados(tmp_path):
    meta = {
        "model_file": "xgb_residual_log_teste.pkl",
        "created_at": "2026-01-01T00:00:00",
        "model_type": "XGBRegressor",
        "metrics": {"mae": 0.1, "rmse": 0.2},
        "dataset_info": {"source_table": "violencia_contra_mulher_gold"},
    }
    meta_file = tmp_path / "xgb_residual_log_teste_meta.json"
    meta_file.write_text(json.dumps(meta), encoding="utf-8")

    resultado = forecast_service.listar_modelos_treinados(models_dir=str(tmp_path))

    assert resultado["total"] == 1
    assert resultado["modelos"][0]["arquivo"] == "xgb_residual_log_teste.pkl"
    assert resultado["modelos"][0]["metricas"] == {"mae": 0.1, "rmse": 0.2}


def test_listar_modelos_treinados_ignora_arquivo_corrompido(tmp_path):
    meta_file = tmp_path / "modelo_corrompido_meta.json"
    meta_file.write_text("{ isso nao é json", encoding="utf-8")

    resultado = forecast_service.listar_modelos_treinados(models_dir=str(tmp_path))

    assert resultado["total"] == 0


def test_listar_modelos_treinados_diretorio_vazio(tmp_path):
    resultado = forecast_service.listar_modelos_treinados(models_dir=str(tmp_path))

    assert resultado == {"total": 0, "modelos": []}
