import pandas as pd
import numpy as np
import itertools
import warnings
from pathlib import Path

from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.stattools import adfuller, acf, pacf
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.stats.stattools import jarque_bera
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tools.sm_exceptions import MissingDataError

from util.log import logs

# ======================================================
# CONFIGURAÇÃO GLOBAL
# ======================================================
warnings.filterwarnings("ignore")

CAMINHO_CSV = "./data/csv/populacao_df_historico.csv"
OUTPUT = Path("./data/output")
OUTPUT.mkdir(parents=True, exist_ok=True)

logger = logs()


# ======================================================
# MÉTRICAS
# ======================================================
def mae(y_true, y_pred):
    return np.mean(np.abs(y_true - y_pred))


def rmse(y_true, y_pred):
    return np.sqrt(np.mean((y_true - y_pred) ** 2))


def mape(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    mask = y_true != 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100


def smape(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    denom = (np.abs(y_true) + np.abs(y_pred)) / 2
    mask = denom != 0
    return np.mean(np.abs(y_true[mask] - y_pred[mask]) / denom[mask]) * 100

def garantir_coluna_rmse(metricas: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que o DataFrame de métricas contenha a coluna RMSE.
    """
    if "RMSE" not in metricas.columns:
        metricas["RMSE"] = np.nan
    return metricas


# ======================================================
# PRÉ-PROCESSAMENTO
# ======================================================
def corrigir_escala(v):
    if pd.isna(v):
        return np.nan  # mantém NaN, para preenchimento posterior
    return int(v / 10) if v > 5_000_000 else int(v)


# ======================================================
# DEFINIÇÃO DE d (ADF)
# ======================================================
def definir_d(serie, max_d=2, min_obs=8):
    resultados = []
    serie = serie.replace([np.inf, -np.inf], np.nan).dropna()
    for d in range(max_d + 1):
        try:
            s = serie.diff(d).dropna() if d > 0 else serie.copy()
            if len(s) < min_obs:
                raise ValueError
            p = adfuller(s, autolag=None)[1]
            resultados.append({"d": d, "p_value": p})
            if p < 0.05:
                return d, pd.DataFrame(resultados)
        except (ValueError, MissingDataError):
            resultados.append({"d": d, "p_value": np.nan})
    return max_d, pd.DataFrame(resultados)


# ======================================================
# GRID SEARCH SARIMAX
# ======================================================
def grid_search_sarimax(serie, d):
    rows = []
    for p, q in itertools.product(range(4), range(4)):
        try:
            model = SARIMAX(
                serie,
                order=(p, d, q),
                seasonal_order=(0, 0, 0, 0),
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            fit = model.fit()
            rows.append({"p": p, "d": d, "q": q, "aic": fit.aic, "bic": fit.bic})
        except Exception:
            continue
    df = pd.DataFrame(rows).sort_values("aic")
    df.to_csv(OUTPUT / "grid_sarimax.csv", sep=";", index=False)
    return df


# ======================================================
# WALK-FORWARD SARIMAX
# ======================================================
def walk_forward_sarimax(serie, order, seasonal_order=(0, 0, 0, 0), min_train=8):
    registros = []
    for i in range(min_train, len(serie)):
        treino = serie.iloc[:i]
        teste = serie.iloc[i]
        try:
            if seasonal_order[3] <= 1 and seasonal_order != (0, 0, 0, 0):
                # Evita erro de periodicidade
                raise ValueError("Seasonal periodicity must be greater than 1.")
            model = SARIMAX(
                treino,
                order=order,
                seasonal_order=seasonal_order,
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            fit = model.fit(disp=False)
            prev = fit.forecast(1).iloc[0]
            registros.append(
                {
                    "ano": serie.index[i].year,
                    "real": teste,
                    "previsto": prev,
                    "erro": teste - prev,
                }
            )
        except Exception as e:
            logger.warning(f"Walk-forward SARIMAX falhou na iteração {i}: {e}")
            continue
    return pd.DataFrame(registros)


# ======================================================
# ETS / HOLT-WINTERS
# ======================================================
def ajustar_ets(serie, trend="add", seasonal=None, seasonal_periods=None):
    try:
        model = ExponentialSmoothing(
            serie,
            trend=trend,
            seasonal=seasonal,
            seasonal_periods=seasonal_periods,
            initialization_method="estimated",
        )
        return model.fit(optimized=True)
    except Exception:
        return None


def walk_forward_ets(serie, trend="add", seasonal=None, min_train=8):
    registros = []
    for i in range(min_train, len(serie)):
        treino = serie.iloc[:i]
        teste = serie.iloc[i]
        try:
            fit = ajustar_ets(treino, trend=trend, seasonal=seasonal)
            if fit is None:
                continue
            prev = fit.forecast(1).iloc[0]
            registros.append(
                {
                    "ano": serie.index[i].year,
                    "real": teste,
                    "previsto": prev,
                    "erro": teste - prev,
                }
            )
        except Exception:
            continue
    return pd.DataFrame(registros)


# ======================================================
# DIAGNÓSTICO DE RESÍDUOS
# ======================================================
def diagnostico_residuos(res):
    res = res.replace([np.inf, -np.inf], np.nan).dropna()
    n = len(res)
    if n < 5:
        return {"n": n}
    nlags = max(1, min(10, (n // 2) - 1))
    lb = acorr_ljungbox(res, lags=[nlags], return_df=True)
    _, jb_p, _, _ = jarque_bera(res)
    return {
        "n": n,
        "nlags": nlags,
        "ljung_p": lb["lb_pvalue"].iloc[0],
        "jb_p": jb_p,
        "acf_max": float(np.max(np.abs(acf(res, nlags=nlags)[1:]))),
        "pacf_max": float(np.max(np.abs(pacf(res, nlags=nlags)[1:]))),
    }


# ======================================================
# VALIDAÇÃO CRUZADA TEMPORAL
# ======================================================
def time_series_cv(
    model_type,
    serie,
    order=None,
    seasonal_order=(0, 0, 0, 0),
    trend="add",
    seasonal=None,
    seasonal_periods=None,
    initial_train=8,
    window_type="expanding",
):
    n = len(serie)
    results = []
    for i in range(initial_train, n):
        treino = (
            serie.iloc[:i]
            if window_type == "expanding"
            else serie.iloc[i - initial_train : i]
        )
        teste = serie.iloc[i]
        try:
            if model_type in ["ARIMA", "SARIMA"]:
                if seasonal_order[3] <= 1 and seasonal_order != (0, 0, 0, 0):
                    raise ValueError("Seasonal periodicity must be greater than 1.")
                fit = SARIMAX(
                    treino,
                    order=order,
                    seasonal_order=seasonal_order,
                    enforce_stationarity=False,
                    enforce_invertibility=False,
                ).fit(disp=False)
                prev = fit.forecast(1).iloc[0]
            elif model_type == "ETS":
                fit = ExponentialSmoothing(
                    treino,
                    trend=trend,
                    seasonal=seasonal,
                    seasonal_periods=seasonal_periods,
                    initialization_method="estimated",
                ).fit(optimized=True)
                prev = fit.forecast(1).iloc[0]
            else:
                raise ValueError("Modelo desconhecido")
            results.append({"real": teste, "previsto": prev, "erro": teste - prev})
        except Exception as e:
            logger.warning(f"CV {model_type} falhou na iteração {i}: {e}")
            continue
    df = pd.DataFrame(results)
    if df.empty:
        metrics = {
            "MAE": np.nan,
            "RMSE": np.nan,
            "MAPE": np.nan,
            "sMAPE": np.nan,
            "std_erro": np.nan,
        } 
    else:
        metrics = {
            "MAE": mae(df["real"], df["previsto"]),
            "RMSE": rmse(df["real"], df["previsto"]),
            "MAPE": mape(df["real"], df["previsto"]),
            "sMAPE": smape(df["real"], df["previsto"]),
            "std_erro": np.std(df["erro"]),
        }
    return metrics, df


# ======================================================
# PIPELINE PRINCIPAL
# ======================================================

def analisar_populacao():
    
    logger.info("Lendo dados...")
    df = pd.read_csv(CAMINHO_CSV, sep=";")
    df["populacao"] = df["populacao"].apply(corrigir_escala)

    # ----------- ANÁLISE E COMPLETAÇÃO DF ORIGINAL -----------
    logger.info("Analisando DataFrame original...")

    # Remove coluna 'arquivo' se existir
    if "arquivo" in df.columns:
        df = df.drop(columns=["arquivo"])

    # Ordena e calcula variação percentual
    df = df.sort_values("ano").reset_index(drop=True)
    df["populacao_anterior"] = df["populacao"].shift(1)
    df["var_perc_populacao"] = (
        (df["populacao"] - df["populacao_anterior"]) / df["populacao_anterior"] * 100
    ).round(2)
    df = df.drop(columns=["populacao_anterior"])

    # Identifica dados faltantes e preenche com backtest ARIMA
    serie_temp = (
        df[["ano", "populacao"]]
        .assign(ano=lambda x: pd.to_datetime(x["ano"], format="%Y"))
        .set_index("ano")
        .asfreq("YS")
    )
    bt_arima_temp = walk_forward_sarimax(
        serie_temp, order=(1, 1, 0)
    )  # fallback para preencher faltantes se necessário

    faltantes = df[df["populacao"].isna()]
    if not faltantes.empty:
        logger.info(f"Preenchendo {len(faltantes)} linhas faltantes com previsões ARIMA...")
        df_bt_arima = bt_arima_temp  # já contém colunas 'ano' e 'previsto'
        for idx, row in faltantes.iterrows():
            ano_faltante = pd.to_datetime(row["ano"]).year
            match = df_bt_arima[df_bt_arima["ano"] == ano_faltante]
            if not match.empty:
                df.at[idx, "populacao"] = match["previsto"].iloc[0]

    df.to_csv(OUTPUT / "df_analise_populacao.csv", sep=";", index=False)
    logger.info(
        "Análise e preenchimento de dados concluído. CSV salvo em df_analise_populacao.csv"
    )

    # ----------- PREPARAÇÃO DA SÉRIE TEMPORAL -----------
    serie = (
        df[["ano", "populacao"]]
        .assign(ano=lambda x: pd.to_datetime(x["ano"], format="%Y"))
        .set_index("ano")
        .asfreq("YS")
    )

    # ----------- SARIMA / ARIMA -----------
    d, _ = definir_d(serie["populacao"])
    d = max(d, 1)
    grid = grid_search_sarimax(serie["populacao"], d)
    order = tuple(grid.iloc[0][["p", "d", "q"]].astype(int))

    bt_arima = walk_forward_sarimax(serie["populacao"], order)
    bt_sarima = walk_forward_sarimax(serie["populacao"], order, seasonal_order=(0, 0, 0, 0))

    bt_arima.to_csv(OUTPUT / "backtest_arima.csv", sep=";", index=False)
    bt_sarima.to_csv(OUTPUT / "backtest_sarima.csv", sep=";", index=False)

    rmse_arima = (
        rmse(bt_arima["real"], bt_arima["previsto"]) if not bt_arima.empty else np.inf
    )
    rmse_sarima = (
        rmse(bt_sarima["real"], bt_sarima["previsto"]) if not bt_sarima.empty else np.inf
    )

    # ----------- ETS -----------
    logger.info("Executando ETS...")
    bt_ets = walk_forward_ets(serie["populacao"])
    bt_ets.to_csv(OUTPUT / "backtest_ets.csv", sep=";", index=False)
    rmse_ets = rmse(bt_ets["real"], bt_ets["previsto"]) if not bt_ets.empty else np.inf

    # ----------- COMPARAÇÃO FINAL -----------
    metricas = pd.DataFrame(
        [
            {"modelo": "ARIMA", "RMSE": rmse_arima},
            {"modelo": "SARIMA", "RMSE": rmse_sarima},
            {"modelo": "ETS", "RMSE": rmse_ets},
        ]
    )
    
    metricas = garantir_coluna_rmse(metricas)

        
    metricas = metricas.sort_values("RMSE")   
    
    metricas.to_csv(
        OUTPUT / "comparacao_modelos_series_temporais.csv", sep=";", index=False
    )
    melhor_modelo = metricas.iloc[0]["modelo"]
    logger.info(f"Melhor modelo: {melhor_modelo}")

    # ----------- MODELO FINAL -----------
    if melhor_modelo in ["ARIMA", "SARIMA"]:
        seasonal = (0, 0, 0, 0)
        final_model = SARIMAX(
            serie["populacao"],
            order=order,
            seasonal_order=seasonal,
            enforce_stationarity=False,
            enforce_invertibility=False, 
        )
        fit_final = final_model.fit()
        pd.DataFrame([diagnostico_residuos(fit_final.resid)]).to_csv(
            OUTPUT / "diagnostico_modelo_final.csv", sep=";", index=False
        )
    else:
        fit_final = ajustar_ets(serie["populacao"])

    # ----------- PREVISÃO FUTURA -----------
    horizonte = max(10, len(serie) // 2)
    prev = fit_final.forecast(horizonte)
    pd.DataFrame(
        {
            "ano": prev.index.year,
            "previsao_populacao": prev.astype(int),
            "modelo": melhor_modelo,
        }
    ).to_csv(OUTPUT / "previsao_populacional_modelo_final.csv", sep=";", index=False)

   # ----------- VALIDAÇÃO CRUZADA TEMPORAL -----------
    metrics_sarima, df_sarima_cv = time_series_cv(
        "SARIMA",
        serie["populacao"],
        order=order,
        seasonal_order=(0, 0, 0, 0),
        initial_train=8,
    )
    metrics_arima, df_arima_cv = time_series_cv(
        "ARIMA",
        serie["populacao"],
        order=order,
        seasonal_order=(0, 0, 0, 0),
        initial_train=8,
    )
    metrics_ets, df_ets_cv = time_series_cv(
        "ETS", serie["populacao"], trend="add", seasonal=None, initial_train=8
    )

    cv_results = pd.DataFrame(
        [
            {"modelo": "ARIMA", **metrics_arima},
            {"modelo": "SARIMA", **metrics_sarima},
            {"modelo": "ETS", **metrics_ets},
        ]
    )

    if "RMSE" not in cv_results.columns:
        cv_results["RMSE"] = np.nan

    cv_results = cv_results.sort_values("RMSE")

    cv_results.to_csv(
        OUTPUT / "cv_comparacao_modelos.csv", sep=";", index=False
    )

    melhor_modelo = cv_results.iloc[0]["modelo"]    
    logger.info(f"Melhor modelo segundo CV: {melhor_modelo}")
    return df
