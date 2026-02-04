import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

import src.tratamento_populacao_df_csv as populacao
from statsmodels.tools.sm_exceptions import MissingDataError


# ======================================================
# MÉTRICAS
# ======================================================


def test_mae():
    assert populacao.mae(np.array([1, 2]), np.array([2, 4])) == 1.5


def test_rmse():
    assert populacao.rmse(np.array([1, 2]), np.array([1, 4])) == pytest.approx(
        1.4142, 0.001
    )


def test_mape_com_zero():
    y_true = np.array([0, 100])
    y_pred = np.array([10, 110])
    assert populacao.mape(y_true, y_pred) == 10.0


def test_smape():
    y_true = np.array([100, 200])
    y_pred = np.array([110, 190])
    assert populacao.smape(y_true, y_pred) > 0


# ======================================================
# PRÉ-PROCESSAMENTO
# ======================================================


def test_corrigir_escala_nan():
    assert np.isnan(populacao.corrigir_escala(np.nan))


def test_corrigir_escala_grande():
    assert populacao.corrigir_escala(10_000_000) == 1_000_000


def test_corrigir_escala_normal():
    assert populacao.corrigir_escala(1000) == 1000


# ======================================================
# DEFINIR d
# ======================================================


@patch("src.tratamento_populacao_df_csv.adfuller")
def test_definir_d_estacionaria(mock_adf):
    mock_adf.return_value = (None, 0.01)
    serie = pd.Series(range(20))
    d, df = populacao.definir_d(serie)
    assert d == 0
    assert not df.empty


@patch("src.tratamento_populacao_df_csv.adfuller", side_effect=Exception)
def test_definir_d_falha(mock_adf):
    serie = pd.Series(range(5))
    d, df = populacao.definir_d(serie)
    assert d == 2


# ======================================================
# GRID SEARCH SARIMAX
# ======================================================


@patch("src.tratamento_populacao_df_csv.SARIMAX")
def test_grid_search_sarimax(mock_sarimax, tmp_path):
    mock_fit = MagicMock()
    mock_fit.aic = 100
    mock_fit.bic = 110
    mock_sarimax.return_value.fit.return_value = mock_fit

    serie = pd.Series(range(20))
    populacao.OUTPUT = tmp_path

    df = populacao.grid_search_sarimax(serie, d=1)
    assert not df.empty


# ======================================================
# WALK FORWARD SARIMAX
# ======================================================


@patch("src.tratamento_populacao_df_csv.SARIMAX")
def test_walk_forward_sarimax_ok(mock_sarimax):
    mock_fit = MagicMock()
    mock_fit.forecast.return_value = pd.Series([10])
    mock_sarimax.return_value.fit.return_value = mock_fit

    serie = pd.Series(range(20), index=pd.date_range("2000", periods=20, freq="YS"))

    df = populacao.walk_forward_sarimax(serie, order=(1, 1, 0))
    assert not df.empty


@patch("src.tratamento_populacao_df_csv.SARIMAX", side_effect=Exception)
def test_walk_forward_sarimax_exception(mock_sarimax):
    serie = pd.Series(range(20), index=pd.date_range("2000", periods=20, freq="YS"))
    df = populacao.walk_forward_sarimax(serie, order=(1, 1, 0))
    assert df.empty


# ======================================================
# ETS
# ======================================================


@patch("src.tratamento_populacao_df_csv.ExponentialSmoothing")
def test_ajustar_ets_ok(mock_ets):
    mock_ets.return_value.fit.return_value = MagicMock()
    serie = pd.Series(range(20))
    assert populacao.ajustar_ets(serie) is not None


@patch("src.tratamento_populacao_df_csv.ExponentialSmoothing", side_effect=Exception)
def test_ajustar_ets_falha(mock_ets):
    serie = pd.Series(range(20))
    assert populacao.ajustar_ets(serie) is None


# ======================================================
# DIAGNÓSTICO RESÍDUOS
# ======================================================


def test_diagnostico_residuos_curto():
    res = pd.Series([1, 2, 3])
    out = populacao.diagnostico_residuos(res)
    assert out["n"] == 3


@patch("src.tratamento_populacao_df_csv.acorr_ljungbox")
@patch("src.tratamento_populacao_df_csv.jarque_bera")
@patch("src.tratamento_populacao_df_csv.acf")
@patch("src.tratamento_populacao_df_csv.pacf")
def test_diagnostico_residuos_completo(mock_pacf, mock_acf, mock_jb, mock_lb):
    mock_lb.return_value = pd.DataFrame({"lb_pvalue": [0.5]})
    mock_jb.return_value = (None, 0.2, None, None)
    mock_acf.return_value = np.array([0, 0.1])
    mock_pacf.return_value = np.array([0, 0.2])

    res = pd.Series(range(20))
    out = populacao.diagnostico_residuos(res)

    assert "ljung_p" in out
    assert "jb_p" in out


# ======================================================
# TIME SERIES CV
# ======================================================


@patch("src.tratamento_populacao_df_csv.SARIMAX")
def test_time_series_cv_arima(mock_sarimax):
    mock_fit = MagicMock()
    mock_fit.forecast.return_value = pd.Series([10])
    mock_sarimax.return_value.fit.return_value = mock_fit

    serie = pd.Series(range(20), index=pd.date_range("2000", periods=20, freq="YS"))

    metrics, df = populacao.time_series_cv("ARIMA", serie, order=(1, 1, 0))

    assert "RMSE" in metrics


def test_time_series_cv_modelo_desconhecido():
    serie = pd.Series(range(20))
    metrics, df = populacao.time_series_cv("XYZ", serie)
    assert np.isnan(metrics["RMSE"])



@patch("src.tratamento_populacao_df_csv.time_series_cv")
@patch("src.tratamento_populacao_df_csv.ajustar_ets")
@patch("src.tratamento_populacao_df_csv.walk_forward_ets")
@patch("src.tratamento_populacao_df_csv.walk_forward_sarimax")
@patch("src.tratamento_populacao_df_csv.grid_search_sarimax")
@patch("src.tratamento_populacao_df_csv.definir_d")
@patch("src.tratamento_populacao_df_csv.pd.read_csv")
def test_analisar_populacao_pipeline_completo(
    mock_read_csv,
    mock_definir_d,
    mock_grid,
    mock_wf_sarimax,
    mock_wf_ets,
    mock_ajustar_ets,
    mock_cv,
    tmp_path,
):
    # ------------------------
    # Dados fake de entrada
    # ------------------------
    df_fake = pd.DataFrame(
        {
            "ano": [2000, 2001, 2002, 2003],
            "populacao": [1000, np.nan, 1100, 1200],
            "arquivo": ["a", "b", "c", "d"],  # cobre drop da coluna
        }
    )
    mock_read_csv.return_value = df_fake

    # ------------------------
    # definir_d
    # ------------------------
    mock_definir_d.return_value = (1, pd.DataFrame())

    # ------------------------
    # grid search
    # ------------------------
    mock_grid.return_value = pd.DataFrame(
        [{"p": 1, "d": 1, "q": 0, "aic": 10, "bic": 12}]
    )

    # ------------------------
    # walk forward ARIMA / SARIMA
    # ------------------------
    mock_wf_sarimax.return_value = pd.DataFrame(
        {
            "ano": [2001, 2002, 2003],
            "real": [1000, 1100, 1200],
            "previsto": [990, 1080, 1190],
        }
    )

    # ------------------------
    # ETS
    # ------------------------
    mock_wf_ets.return_value = pd.DataFrame(
        {
            "real": [1000, 1100],
            "previsto": [995, 1090],
        }
    )

    fake_fit = MagicMock()
    fake_fit.forecast.return_value = pd.Series(
        [1300, 1350],
        index=pd.date_range("2004", periods=2, freq="YS"),
    )
    mock_ajustar_ets.return_value = fake_fit

    # ------------------------
    # Validação cruzada
    # ------------------------
    mock_cv.return_value = (
        {
            "MAE": 1,
            "RMSE": 1,
            "MAPE": 1,
            "sMAPE": 1,
            "std_erro": 1,
        },
        pd.DataFrame({"real": [1], "previsto": [1]}),
    )

    # ------------------------
    # OUTPUT isolado
    # ------------------------
    populacao.OUTPUT = tmp_path

    # ------------------------
    # Execução
    # ------------------------
    populacao.analisar_populacao()

# ======================================================
# corrigir_escala – cobre NaN explícito
# ======================================================

def test_corrigir_escala_nan_numpy():
    result = populacao.corrigir_escala(np.nan)
    assert np.isnan(result)


# ======================================================
# definir_d – MissingDataError no adfuller
# ======================================================

def test_definir_d_missing_data_error(monkeypatch):
    def fake_adfuller(*args, **kwargs):
        raise MissingDataError("erro simulado")

    monkeypatch.setattr(populacao, "adfuller", fake_adfuller)

    serie = pd.Series(range(20))
    d, df = populacao.definir_d(serie)

    assert d == 2
    assert df["p_value"].isna().any()


# ======================================================
# time_series_cv – SARIMA com periodicidade inválida
# ======================================================

def test_time_series_cv_sarima_periodicidade_invalida():
    serie = pd.Series(
        np.random.rand(20),
        index=pd.date_range("2000", periods=20, freq="YS")
    )

    metrics, df = populacao.time_series_cv(
        model_type="SARIMA",
        serie=serie,
        order=(1, 1, 1),
        seasonal_order=(1, 0, 1, 1),  # inválido
        initial_train=8,
    )

    assert df.empty
    assert np.isnan(metrics["RMSE"])

# ======================================================
# analisar_populacao – ETS como melhor modelo
# ======================================================

def test_analisar_populacao_melhor_modelo_ets(monkeypatch, tmp_path):
    df = pd.DataFrame(
        {
            "ano": list(range(2000, 2015)),
            "populacao": list(range(1000, 1015)),
        }
    )

    csv_path = tmp_path / "pop.csv"
    df.to_csv(csv_path, sep=";", index=False)

    monkeypatch.setattr(populacao, "CAMINHO_CSV", csv_path)

    # Força ETS a vencer
    monkeypatch.setattr(populacao, "rmse", lambda *args, **kwargs: 100)

    monkeypatch.setattr(
        populacao,
        "walk_forward_ets",
        lambda *args, **kwargs: pd.DataFrame(
            {"real": [1, 2], "previsto": [1, 2]}
        ),
    )

    populacao.analisar_populacao()

def test_grid_search_sarimax_excecao_em_fit(monkeypatch):
    class FakeFit:
        aic = 100
        bic = 200

    class FakeModel:
        def __init__(self, order):
            self.order = order

        def fit(self, *args, **kwargs):
            # Falha só para uma combinação específica
            if self.order == (0, 1, 0):
                raise ValueError("erro forçado")
            return FakeFit()

    def fake_sarimax(serie, order, **kwargs):
        return FakeModel(order)

    monkeypatch.setattr(populacao, "SARIMAX", fake_sarimax)

    serie = pd.Series(
        np.random.rand(20),
        index=pd.date_range("2000", periods=20, freq="YS"),
    )

    df = populacao.grid_search_sarimax(serie, d=1)

    # Continua funcionando
    assert not df.empty
    assert "aic" in df.columns

def test_walk_forward_ets_fit_none(monkeypatch):
    serie = pd.Series(
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
        index=pd.date_range("2000", periods=9, freq="YS"),
    )

    monkeypatch.setattr(
        populacao,
        "ajustar_ets",
        lambda *a, **k: None,
    )

    df = populacao.walk_forward_ets(serie)

    assert df.empty


def test_walk_forward_sarimax_seasonal_periodicity_invalida(monkeypatch):
    # Série mínima válida
    serie = pd.Series(
        np.random.rand(10),
        index=pd.date_range("2000", periods=10, freq="YS"),
    )

    # seasonal_order inválido: s <= 1 e diferente de (0,0,0,0)
    seasonal_order = (1, 0, 1, 1)

    df = populacao.walk_forward_sarimax(
        serie=serie,
        order=(1, 1, 1),
        seasonal_order=seasonal_order,
        min_train=8,
    )

    # Como todas as iterações caem no except, não há registros
    assert isinstance(df, pd.DataFrame)
    assert df.empty

def test_walk_forward_ets_excecao_em_ajustar_ets(monkeypatch):
    # Força exceção no ajustar_ets
    def fake_ajustar_ets(*args, **kwargs):
        raise RuntimeError("erro forçado")

    monkeypatch.setattr(populacao, "ajustar_ets", fake_ajustar_ets)

    serie = pd.Series(
        np.random.rand(10),
        index=pd.date_range("2000", periods=10, freq="YS"),
    )

    df = populacao.walk_forward_ets(
        serie=serie,
        trend="add",
        seasonal=None,
        min_train=8,
    )

    # Todas as iterações caem no except → DataFrame vazio
    assert isinstance(df, pd.DataFrame)
    assert df.empty

def test_analisar_populacao_preenche_faltante_com_arima(monkeypatch):
    # --- CSV fake com população faltante ---
    df_csv = pd.DataFrame(
        {
            "ano": ["2005"],
            "populacao": [np.nan],
        }
    )

    # --- Retorno 1: preenchimento de faltantes ---
    df_bt_faltante = pd.DataFrame(
        {
            "ano": [2005],
            "previsto": [123456],
        }
    )

    # --- Retorno 2: backtest ARIMA normal ---
    df_bt_arima = pd.DataFrame(
        {
            "ano": [2006],
            "real": [120000],
            "previsto": [121000],
            "erro": [-1000],
        }
    )

    calls = {"n": 0}

    def fake_walk_forward(*args, **kwargs):
        calls["n"] += 1
        return df_bt_faltante if calls["n"] == 1 else df_bt_arima

    monkeypatch.setattr(
        populacao.pd,
        "read_csv",
        lambda *a, **k: df_csv.copy(),
    )
    monkeypatch.setattr(populacao, "walk_forward_sarimax", fake_walk_forward)

    # --- Mocks do restante do pipeline ---
    monkeypatch.setattr(
        populacao,
        "grid_search_sarimax",
        lambda *a, **k: pd.DataFrame([{"p": 1, "d": 1, "q": 0, "aic": 1, "bic": 1}]),
    )
    monkeypatch.setattr(populacao, "definir_d", lambda *a, **k: (1, None))
    monkeypatch.setattr(populacao, "walk_forward_ets", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(populacao, "ajustar_ets", lambda *a, **k: None)
    monkeypatch.setattr(
        populacao, "time_series_cv", lambda *a, **k: ({}, pd.DataFrame())
    )

    class FakeFit:
        resid = pd.Series([0])

        def forecast(self, steps):
            idx = pd.date_range("2007", periods=steps, freq="YS")
            return pd.Series([100] * steps, index=idx)

    class FakeModel:
        def fit(self, *a, **k):
            return FakeFit()

    monkeypatch.setattr(populacao, "SARIMAX", lambda *a, **k: FakeModel())
    monkeypatch.setattr(pd.DataFrame, "to_csv", lambda *a, **k: None)

    # --- EXECUÇÃO ---
    df_result = populacao.analisar_populacao()


    assert df_result.loc[0, "populacao"] == 123456

def test_adiciona_coluna_rmse_quando_ausente():
    import pandas as pd
    import numpy as np
    from src.tratamento_populacao_df_csv import garantir_coluna_rmse

    df = pd.DataFrame(
        {
            "MAE": [1.0],
            "MAPE": [2.0],
            "SMAPE": [3.0],
        }
    )

    resultado = garantir_coluna_rmse(df)

    assert "RMSE" in resultado.columns
    assert np.isnan(resultado["RMSE"].iloc[0])
