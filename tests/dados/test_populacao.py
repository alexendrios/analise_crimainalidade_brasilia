import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch, MagicMock

import src.tratamento_populacional as mod  # ajuste para o nome real

def mae(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean(np.abs(y_true - y_pred))


def rmse(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.sqrt(np.mean((y_true - y_pred) ** 2))


def test_mape_com_zero():
    assert mod.mape([0, 10], [0, 8]) == 20.0


def test_smape():
    assert round(mod.smape([10], [8]), 2) == 22.22

def test_garantir_coluna_rmse_cria_coluna():
    df = pd.DataFrame({"modelo": ["A"]})
    result = mod.garantir_coluna_rmse(df)
    assert "RMSE" in result.columns


def test_garantir_coluna_rmse_ja_existe():
    df = pd.DataFrame({"RMSE": [1.0]})
    result = mod.garantir_coluna_rmse(df)
    assert result["RMSE"].iloc[0] == 1.0

def test_corrigir_escala_maior_5M():
    assert mod.corrigir_escala(10_000_000) == 1_000_000


def test_corrigir_escala_normal():
    assert mod.corrigir_escala(1000) == 1000


def test_corrigir_escala_nan():
    assert np.isnan(mod.corrigir_escala(np.nan))

@patch("src.tratamento_populacional.adfuller")
def test_definir_d_estacionaria(mock_adf):
    mock_adf.return_value = (None, 0.01)

    serie = pd.Series(range(20))
    d, df = mod.definir_d(serie)

    assert d == 0
    assert not df.empty

@patch("src.tratamento_populacional.SARIMAX")
def test_grid_search(mock_sarimax):
    mock_fit = MagicMock()
    mock_fit.aic = 10
    mock_fit.bic = 20

    mock_model = MagicMock()
    mock_model.fit.return_value = mock_fit
    mock_sarimax.return_value = mock_model

    serie = pd.Series(range(15))
    df = mod.grid_search_sarimax(serie, 1)

    assert not df.empty
    assert "aic" in df.columns

@patch("src.tratamento_populacional.SARIMAX")
def test_walk_forward_sarimax(mock_sarimax):
    mock_fit = MagicMock()
    mock_fit.forecast.return_value = pd.Series([100])

    mock_model = MagicMock()
    mock_model.fit.return_value = mock_fit
    mock_sarimax.return_value = mock_model

    serie = pd.Series(
        range(20),
        index=pd.date_range("2000", periods=20, freq="YS"),
    )

    df = mod.walk_forward_sarimax(serie, (1, 1, 0))
    assert not df.empty

@patch("src.tratamento_populacional.ExponentialSmoothing")
def test_ajustar_ets(mock_ets):
    mock_model = MagicMock()
    mock_model.fit.return_value = "fit"
    mock_ets.return_value = mock_model

    serie = pd.Series(range(10))
    result = mod.ajustar_ets(serie)

    assert result == "fit"

@patch("src.tratamento_populacional.ExponentialSmoothing", side_effect=Exception)
def test_ajustar_ets_erro(mock_ets):
    serie = pd.Series(range(10))
    result = mod.ajustar_ets(serie)
    assert result is None

def test_diagnostico_residuos_curto():
    result = mod.diagnostico_residuos(pd.Series([1, 2]))
    assert result["n"] == 2

@patch("src.tratamento_populacional.acorr_ljungbox")
@patch("src.tratamento_populacional.jarque_bera")
@patch("src.tratamento_populacional.acf")
@patch("src.tratamento_populacional.pacf")
def test_diagnostico_residuos_completo(mock_pacf, mock_acf, mock_jb, mock_lb):
    mock_lb.return_value = pd.DataFrame({"lb_pvalue": [0.5]})
    mock_jb.return_value = (None, 0.6, None, None)
    mock_acf.return_value = np.array([0, 0.1])
    mock_pacf.return_value = np.array([0, 0.2])

    serie = pd.Series(range(20))
    result = mod.diagnostico_residuos(serie)

    assert "acf_max" in result

def test_time_series_cv_modelo_desconhecido():
    serie = pd.Series(range(10))
    metrics, df = mod.time_series_cv("X", serie)

    assert np.isnan(metrics["MAE"])
    assert df.empty


@patch("src.tratamento_populacional.pd.read_csv")
@patch("src.tratamento_populacional.walk_forward_sarimax")
@patch("src.tratamento_populacional.grid_search_sarimax")
@patch("src.tratamento_populacional.definir_d")
@patch("src.tratamento_populacional.walk_forward_ets")
@patch("src.tratamento_populacional.time_series_cv")
def test_analisar_populacao(
    mock_cv,
    mock_ets,
    mock_definir_d,
    mock_grid,
    mock_walk,
    mock_read,
):
    mock_read.return_value = pd.DataFrame(
        {
            "ano": range(2000, 2015),
            "populacao": range(1000, 1015),
        }
    )

    mock_definir_d.return_value = (1, pd.DataFrame())
    mock_grid.return_value = pd.DataFrame(
        {
            "p": [1],
            "d": [1],
            "q": [0],
            "aic": [1],
            "bic": [1],
        }
    )

    mock_walk.return_value = pd.DataFrame(
        {
            "real": [1],
            "previsto": [1],
            "erro": [0],
        }
    )

    mock_ets.return_value = pd.DataFrame(
        {
            "real": [1],
            "previsto": [1],
            "erro": [0],
        }
    )

    mock_cv.return_value = ({"RMSE": 1}, pd.DataFrame())

    df = mod.analisar_populacao()
    assert not df.empty

@patch("src.tratamento_populacional.pd.read_csv")
def test_tratar_populacao(mock_read, tmp_path):
    mock_read.return_value = pd.DataFrame({"a": [1]})

    entrada = tmp_path / "in.csv"
    saida = tmp_path / "out.csv"

    mod.tratar_populacao_regiao_administrativa(
        str(entrada),
        str(saida),
    )

@patch("src.tratamento_populacional.adfuller", side_effect=ValueError)
def test_definir_d_value_error(mock_adf):
    serie = pd.Series(range(10))
    d, df = mod.definir_d(serie)
    assert not df.empty

@patch("src.tratamento_populacional.adfuller")
def test_definir_d_nunca_estacionaria(mock_adf):
    mock_adf.return_value = (None, 0.5)

    serie = pd.Series(range(20))
    d, df = mod.definir_d(serie, max_d=2)

    assert d == 2

@patch("src.tratamento_populacional.SARIMAX", side_effect=Exception)
def test_grid_search_exception(mock_sarimax):
    serie = pd.Series(range(20))
    df = mod.grid_search_sarimax(serie, 1)
    assert df.empty

def test_walk_forward_seasonal_invalido():
    serie = pd.Series(
        range(20),
        index=pd.date_range("2000", periods=20, freq="YS"),
    )

    df = mod.walk_forward_sarimax(
        serie,
        order=(1, 1, 0),
        seasonal_order=(1, 0, 0, 1),  # periodicidade inválida
    )

    assert df.empty

@patch("src.tratamento_populacional.SARIMAX", side_effect=Exception)
def test_walk_forward_exception(mock_sarimax):
    serie = pd.Series(
        range(20),
        index=pd.date_range("2000", periods=20, freq="YS"),
    )

    df = mod.walk_forward_sarimax(serie, (1, 1, 0))
    assert df.empty

@patch("src.tratamento_populacional.ExponentialSmoothing", side_effect=Exception)
def test_ajustar_ets_exception(mock_ets):
    serie = pd.Series(range(10))
    assert mod.ajustar_ets(serie) is None

@patch("src.tratamento_populacional.ajustar_ets", side_effect=Exception)
def test_walk_forward_ets_exception(mock_ets):
    serie = pd.Series(
        range(20),
        index=pd.date_range("2000", periods=20, freq="YS"),
    )

    df = mod.walk_forward_ets(serie)
    assert df.empty

def test_diagnostico_residuos_n_menor_5():
    result = mod.diagnostico_residuos(pd.Series([1, 2, 3]))
    assert result["n"] == 3

@patch("src.tratamento_populacional.time_series_cv")
@patch("src.tratamento_populacional.walk_forward_ets")
@patch("src.tratamento_populacional.walk_forward_sarimax")
@patch("src.tratamento_populacional.grid_search_sarimax")
@patch("src.tratamento_populacional.definir_d")
@patch("src.tratamento_populacional.pd.read_csv")
def test_analisar_populacao_forca_ets(
    mock_read,
    mock_definir,
    mock_grid,
    mock_walk,
    mock_ets,
    mock_cv,
):
    mock_read.return_value = pd.DataFrame(
        {
            "ano": range(2000, 2015),
            "populacao": range(1000, 1015),
        }
    )

    mock_definir.return_value = (1, pd.DataFrame())
    mock_grid.return_value = pd.DataFrame(
        {"p": [1], "d": [1], "q": [0], "aic": [1], "bic": [1]}
    )

    mock_walk.return_value = pd.DataFrame({"real": [1], "previsto": [1], "erro": [0]})
    mock_ets.return_value = pd.DataFrame({"real": [1], "previsto": [1], "erro": [0]})

    mock_cv.return_value = ({"RMSE": 0.1}, pd.DataFrame())

    df = mod.analisar_populacao()
    assert not df.empty

@patch("src.tratamento_populacional.time_series_cv")
def test_cv_sem_rmse(mock_cv):
    mock_cv.return_value = ({}, pd.DataFrame())

@patch("src.tratamento_populacional.pd.read_csv")
def test_tratar_populacao(mock_read, tmp_path):
    mock_read.return_value = pd.DataFrame({"a": [1]})
    entrada = tmp_path / "in.csv"
    saida = tmp_path / "out.csv"

    mod.tratar_populacao_regiao_administrativa(
        str(entrada),
        str(saida),
    )

def test_definir_d_len_menor_que_min_obs():
    # Série pequena (menos que 8)
    serie = pd.Series([1, 2, 3])

    d, df = mod.definir_d(serie, max_d=2, min_obs=8)

    # Deve retornar max_d porque nunca conseguiu testar ADF
    assert d == 2
    assert not df.empty

    # p_value deve ser NaN porque caiu no except
    assert df["p_value"].isna().all()

patch("src.tratamento_populacional.ajustar_ets", return_value=None)


@patch("src.tratamento_populacional.ajustar_ets", return_value=None)
def test_walk_forward_ets_fit_none(mock_ajustar):
    # Série com tamanho suficiente para entrar no loop
    serie = pd.Series(
        range(15),
        index=pd.date_range("2000", periods=15, freq="YS"),
    )

    df = mod.walk_forward_ets(serie, min_train=8)

    # Como fit sempre retorna None, nunca entra no append
    assert df.empty

def test_time_series_cv_seasonal_periodicity_invalid():
    serie = pd.Series(
        range(15),
        index=pd.date_range("2000", periods=15, freq="YS"),
    )

    metrics, df = mod.time_series_cv(
        model_type="SARIMA",
        serie=serie,
        order=(1, 0, 0),
        seasonal_order=(1, 0, 0, 1),  # <- periodicidade inválida (<=1)
        initial_train=8,
    )

    # Como sempre cai no except, nunca adiciona resultados
    assert df.empty

    # Métricas devem ser NaN
    assert np.isnan(metrics["MAE"])
    assert np.isnan(metrics["RMSE"])
    assert np.isnan(metrics["MAPE"])
    assert np.isnan(metrics["sMAPE"])
    assert np.isnan(metrics["std_erro"])
  
@patch("src.tratamento_populacional.time_series_cv", return_value=({}, pd.DataFrame()))
@patch("src.tratamento_populacional.ajustar_ets")
@patch("src.tratamento_populacional.walk_forward_ets")
@patch("src.tratamento_populacional.garantir_coluna_rmse")
@patch("src.tratamento_populacional.rmse", return_value=1)
@patch("src.tratamento_populacional.grid_search_sarimax")
@patch("src.tratamento_populacional.definir_d", return_value=(1, None))
@patch("src.tratamento_populacional.walk_forward_sarimax")
@patch("src.tratamento_populacional.pd.read_csv")
def test_analisar_populacao_preenche_faltantes(
    mock_read_csv,
    mock_walk_forward,
    mock_definir_d,
    mock_grid,
    mock_rmse,
    mock_garantir,
    mock_walk_ets,
    mock_ajustar,
    mock_cv,
):

    # ------------------ Arrange ------------------
    df_fake = pd.DataFrame(
        {
            "ano": [2000, 2001, 2002],
            "populacao": [1000, np.nan, 1200],
        }
    )

    mock_read_csv.return_value = df_fake

    mock_grid.return_value = pd.DataFrame([{"p": 1, "d": 1, "q": 0}])

    df_bt = pd.DataFrame(
        {
            "ano": [2001],
            "real": [1100],
            "previsto": [1100],
            "erro": [0],
        }
    )

    mock_walk_forward.return_value = df_bt

    mock_walk_ets.return_value = pd.DataFrame(
        {
            "ano": [2000, 2001, 2002],
            "real": [1000, 1100, 1200],
            "previsto": [1000, 1100, 1200],
            "erro": [0, 0, 0],
        }
    )

    # 🔥 Forçando ETS como melhor modelo
    mock_garantir.return_value = pd.DataFrame(
        [
            {"modelo": "ETS", "RMSE": 0},
            {"modelo": "ARIMA", "RMSE": 10},
            {"modelo": "SARIMA", "RMSE": 20},
        ]
    )

    mock_fit = MagicMock()
    mock_fit.forecast.return_value = pd.Series(
        [1300] * 10,
        index=pd.date_range("2003", periods=10, freq="YS"),
    )

    mock_ajustar.return_value = mock_fit

    # ------------------ Act ------------------
    df_result = mod.analisar_populacao()

    # ------------------ Assert ------------------
    assert df_result.loc[df_result["ano"] == 2001, "populacao"].iloc[0] == 1100
