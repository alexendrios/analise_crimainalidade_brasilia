# =========================================================
# IMPORTS
# =========================================================
import os
import pandas as pd
import numpy as np
import joblib
from datetime import datetime

from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor
from prophet import Prophet

from ingestion.repository_adapter import Repository
from util.log import logs

logger = logs()

# =========================================================
# CONFIG
# =========================================================
FEATURES = [
    "lag_1",
    "lag_2",
    "rolling_mean_2",
    "rolling_mean_3",
    "taxa_feminicidio",
    "feminicidio_lag_1",
    "trend",
    "ano_num",
    "diff_1",
]

MODEL_NAME = f"xgb_residual_log_{datetime.now().strftime('%Y%m%d_%H%M')}"
MODEL_PATH = f"models/{MODEL_NAME}.pkl"


# =========================================================
# MÉTRICAS
# =========================================================
def calcular_metricas(y_true, y_pred):
    return {
        "mae": mean_absolute_error(y_true, y_pred),
        "rmse": np.sqrt(mean_squared_error(y_true, y_pred)),
    }


# =========================================================
# FEATURE ENGINEERING
# =========================================================
def preparar_dados(df, valor_col):
    df = df.copy()

    df["ano"] = pd.to_datetime(df["ano"].astype(int), format="%Y")
    df = df.sort_values("ano")

    df["lag_1"] = df[valor_col].shift(1)
    df["lag_2"] = df[valor_col].shift(2)

    df["rolling_mean_2"] = df["lag_1"].rolling(2).mean()
    df["rolling_mean_3"] = df["lag_1"].rolling(3).mean()

    df["feminicidio_lag_1"] = df["casos_feminicidios"].shift(1)

    df["taxa_feminicidio"] = (
        (df["casos_feminicidios"].shift(1) / df["lag_1"])
        .replace([np.inf, -np.inf], 0)
        .fillna(0)
    )

    df["trend"] = np.arange(len(df))
    df["ano_num"] = df["ano"].dt.year
    df["diff_1"] = df[valor_col].diff(1)

    df = df.dropna()

    return df


# =========================================================
# PROPHET
# =========================================================
def treinar_prophet(df, valor_col):
    df_p = df[["ano", valor_col]].rename(columns={"ano": "ds", valor_col: "y"})

    model = Prophet(yearly_seasonality=True)
    model.fit(df_p)

    forecast = model.predict(df_p)

    return model, forecast


# =========================================================
# TREINAMENTO RESIDUAL (ROBUSTO)
# =========================================================
def treinar_residual(df, valor_col):
    logger.info("🤖 Treinando modelo residual LOG (robusto)")

    prophet_model, prophet_fit = treinar_prophet(df, valor_col)

    df["prophet_fit"] = prophet_fit["yhat"].values

    # 🔥 Residual log
    df["residual"] = np.log1p(df[valor_col]) - np.log1p(df["prophet_fit"])

    split = int(len(df) * 0.8)

    train = df.iloc[:split]
    test = df.iloc[split:]

    X_train = train[FEATURES]
    y_train = train["residual"]

    X_test = test[FEATURES]
    y_test = test["residual"]

    model = XGBRegressor(
        n_estimators=600,
        learning_rate=0.015,
        max_depth=3,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.5,
        reg_lambda=2,
        random_state=42,
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    metrics = calcular_metricas(y_test, preds)

    # 🔥 bounds dinâmico
    residual_min = df["residual"].quantile(0.05)
    residual_max = df["residual"].quantile(0.95)

    logger.info(f"📊 Residual bounds: [{residual_min:.4f}, {residual_max:.4f}]")
    logger.info(f"📊 Residual MAE: {metrics['mae']:.4f}")

    return model, prophet_model, metrics, residual_min, residual_max


# =========================================================
# PREVISÃO FUTURA (🔥 NÍVEL PRODUÇÃO)
# =========================================================
def prever_futuro(
    model, prophet_model, df, valor_col, residual_min, residual_max, anos=5
):

    logger.info("🔮 Previsão híbrida LOG + XGB (produção)")

    future = prophet_model.make_future_dataframe(periods=anos, freq="YS")
    forecast = prophet_model.predict(future)

    futuro_prophet = forecast.tail(anos).reset_index(drop=True)

    ultimo = df.iloc[-1:].copy()
    resultados = []

    for i in range(anos):
        base = float(futuro_prophet.loc[i, "yhat"])

        residual_log = float(model.predict(ultimo[FEATURES])[0])

        # 🔥 CLIP DINÂMICO
        residual_log = np.clip(residual_log, residual_min, residual_max)

        # 🔥 DECAY TEMPORAL (cada ano perde impacto)
        decay = 1 - (i * 0.15)
        decay = max(0.4, decay)

        residual_log *= decay

        # 🔥 RECONSTRUÇÃO LOG
        final = np.expm1(np.log1p(base) + residual_log)

        # 🔥 SUAVIZAÇÃO
        if len(resultados) > 0:
            prev = resultados[-1]["final"]
            final = 0.7 * final + 0.3 * prev

        final = max(0.1, final)

        # =========================
        # UPDATE FEATURES
        # =========================
        novo = ultimo.copy()
        novo["ano"] += pd.DateOffset(years=1)
        novo[valor_col] = final

        lag1 = float(final)
        lag2 = float(ultimo["lag_1"].values[0])
        lag3 = float(ultimo["lag_2"].values[0])

        novo["lag_1"] = lag1
        novo["lag_2"] = lag2

        novo["rolling_mean_2"] = np.mean([lag1, lag2])
        novo["rolling_mean_3"] = np.mean([lag1, lag2, lag3])

        novo["diff_1"] = float(final - ultimo[valor_col].values[0])
        novo["trend"] = ultimo["trend"].values[0] + 1
        novo["ano_num"] = novo["ano"].dt.year
        novo["taxa_feminicidio"] = ultimo["taxa_feminicidio"]

        resultados.append(
            {
                "ano": novo["ano"].values[0],
                "prophet": base,
                "residual_log": residual_log,
                "final": final,
            }
        )

        ultimo = novo

    return pd.DataFrame(resultados)


# =========================================================
# PIPELINE
# =========================================================
def executar_pipeline():
    logger.info("🚀 Pipeline iniciado")

    df = Repository.load("violencia_contra_mulher_gold")
    valor_col = "crimes_contra_mulher"

    df = preparar_dados(df, valor_col)

    model, prophet_model, metrics, rmin, rmax = treinar_residual(df, valor_col)

    forecast = prever_futuro(model, prophet_model, df, valor_col, rmin, rmax)

    os.makedirs("models", exist_ok=True)
    joblib.dump(model, MODEL_PATH)

    logger.info("🏁 Pipeline finalizado")
    logger.info({"metrics_residual": metrics, "forecast": forecast})

    return forecast


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    executar_pipeline()
