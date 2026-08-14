# =========================================================
# IMPORTS
# =========================================================
import glob
import json
import os
from datetime import datetime
import joblib
import numpy as np
import pandas as pd
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

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

MODELS_DIR = "models"
MODEL_NAME = f"xgb_residual_log_{datetime.now().strftime('%Y%m%d_%H%M')}"
MODEL_PATH = f"{MODELS_DIR}/{MODEL_NAME}.pkl"

# Chaves usadas dentro do artefato .pkl quando ele é um "bundle"
# (Prophet + XGBoost) salvo em conjunto, em vez do formato legado que
# guardava apenas o regressor XGBoost.
_CHAVE_XGB = "xgb_model"
_CHAVE_PROPHET = "prophet_model"


# =========================================================
# GERENCIAMENTO DE ARTEFATOS & METADADOS
# =========================================================
def save_model_with_metadata(
    model, model_path: str, metadata: dict, prophet_model=None
) -> None:
    """
    Salva o modelo serializado (.pkl) e gera um arquivo de metadados padronizado (*_meta.json).

    :param model: Objeto do modelo treinado (tipicamente o XGBRegressor do resíduo).
    :param model_path: Caminho completo de saída para o arquivo .pkl.
    :param metadata: Dicionário contendo informações específicas do modelo/treinamento.
    :param prophet_model: Opcional. Quando informado, o modelo Prophet correspondente é
        salvo *junto* com `model` em um único artefato "bundle" (dict serializado via
        joblib), permitindo que a previsão híbrida seja servida a partir do artefato
        salvo em disco, sem re-treinar. Quando omitido (padrão), o comportamento é o
        legado: apenas `model` é serializado diretamente (compatível com artefatos
        antigos gerados antes desta funcionalidade).
    """
    os.makedirs(os.path.dirname(model_path), exist_ok=True)

    if prophet_model is not None:
        # Formato "bundle": Prophet + XGBoost persistidos juntos no mesmo .pkl,
        # para que a previsão híbrida completa possa ser reconstruída sem re-treino.
        joblib.dump({_CHAVE_XGB: model, _CHAVE_PROPHET: prophet_model}, model_path)
        model_type = f"bundle({type(model).__name__}+{type(prophet_model).__name__})"
        artifact_format = "bundle"
    else:
        # Formato legado: apenas o objeto `model` (tipicamente o XGBoost do resíduo).
        joblib.dump(model, model_path)
        model_type = type(model).__name__
        artifact_format = "legacy"

    # 2. Estrutura padronizada do arquivo de metadados
    base_path, _ = os.path.splitext(model_path)
    meta_path = f"{base_path}_meta.json"

    full_metadata = {
        "created_at": datetime.now().isoformat(),
        "model_file": os.path.basename(model_path),
        "model_type": model_type,
        "artifact_format": artifact_format,
        "metrics": metadata.get("metrics", {}),
        "hyperparameters": metadata.get("hyperparameters", {}),
        "features": metadata.get("features", []),
        "target": metadata.get("target", ""),
        "dataset_info": metadata.get("dataset_info", {}),
        "extra": metadata.get("extra", {}),
    }

    # 3. Salvar o arquivo JSON
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(full_metadata, f, indent=4, ensure_ascii=False)

    logger.info(f"✅ Modelo salvo em: {model_path} (formato: {artifact_format})")
    logger.info(f"📄 Metadados salvos em: {meta_path}")


def carregar_modelo(model_path: str):
    """
    Carrega um artefato .pkl salvo por `save_model_with_metadata`.

    Reconhece automaticamente os dois formatos possíveis:
    - "bundle": dict com `xgb_model` e `prophet_model` -> retorna ambos.
    - "legacy": apenas o objeto do modelo (XGBoost) -> retorna `(model, None)`.

    :return: tupla `(xgb_model, prophet_model)`. `prophet_model` é `None`
        quando o artefato é do formato legado (sem Prophet persistido).
    """
    artefato = joblib.load(model_path)

    if isinstance(artefato, dict) and _CHAVE_XGB in artefato:
        return artefato[_CHAVE_XGB], artefato.get(_CHAVE_PROPHET)

    return artefato, None


def localizar_ultimo_modelo_bundle(models_dir: str = MODELS_DIR):
    """
    Procura, em `models_dir`, o artefato mais recente salvo no formato "bundle"
    (Prophet + XGBoost persistidos juntos), com base no campo `created_at` do
    respectivo `*_meta.json`.

    Artefatos "legacy" (apenas XGBoost, sem Prophet) são ignorados aqui, pois
    não permitem reconstruir a previsão híbrida sem re-treinar o Prophet.

    :return: tupla `(model_path, meta_dict)` do bundle mais recente, ou
        `(None, None)` se nenhum artefato nesse formato existir (ex.: apenas
        modelos legados, ou diretório vazio/inexistente).
    """
    padrao = os.path.join(models_dir, "*_meta.json")
    candidatos = []

    for meta_path in glob.glob(padrao):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except (OSError, json.JSONDecodeError):
            logger.warning(f"⚠️ Não foi possível ler metadados em: {meta_path}")
            continue

        if meta.get("artifact_format") != "bundle":
            continue

        model_file = meta.get("model_file")
        if not model_file:
            continue

        model_path = os.path.join(models_dir, model_file)
        if not os.path.exists(model_path):
            continue

        candidatos.append((meta.get("created_at", ""), model_path, meta))

    if not candidatos:
        return None, None

    candidatos.sort(key=lambda item: item[0], reverse=True)
    _, model_path, meta = candidatos[0]

    return model_path, meta


# =========================================================
# MÉTRICAS
# =========================================================
def calcular_metricas(y_true, y_pred):
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
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

    # Residual log
    df["residual"] = np.log1p(df[valor_col]) - np.log1p(df["prophet_fit"])

    split = int(len(df) * 0.8)

    train = df.iloc[:split]
    test = df.iloc[split:]

    X_train = train[FEATURES]
    y_train = train["residual"]

    X_test = test[FEATURES]
    y_test = test["residual"]

    params = {
        "n_estimators": 600,
        "learning_rate": 0.015,
        "max_depth": 3,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.5,
        "reg_lambda": 2,
        "random_state": 42,
    }

    model = XGBRegressor(**params)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    metrics = calcular_metricas(y_test, preds)

    # bounds dinâmicos
    residual_min = float(df["residual"].quantile(0.05))
    residual_max = float(df["residual"].quantile(0.95))

    logger.info(f"📊 Residual bounds: [{residual_min:.4f}, {residual_max:.4f}]")
    logger.info(f"📊 Residual MAE: {metrics['mae']:.4f}")

    return model, prophet_model, metrics, residual_min, residual_max, params


# =========================================================
# PREVISÃO FUTURA
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

        # CLIP DINÂMICO
        residual_log = np.clip(residual_log, residual_min, residual_max)

        # DECAY TEMPORAL
        decay = 1 - (i * 0.15)
        decay = max(0.4, decay)

        residual_log *= decay

        # RECONSTRUÇÃO LOG
        final = np.expm1(np.log1p(base) + residual_log)

        # SUAVIZAÇÃO
        if len(resultados) > 0:
            prev = resultados[-1]["final"]
            final = 0.7 * final + 0.3 * prev

        final = max(0.1, final)

        # UPDATE FEATURES
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

    table_name = "violencia_contra_mulher_gold"
    valor_col = "crimes_contra_mulher"

    df = Repository.load(table_name)
    df_preparado = preparar_dados(df, valor_col)

    (
        model,
        prophet_model,
        metrics,
        rmin,
        rmax,
        hyperparams,
    ) = treinar_residual(df_preparado, valor_col)

    forecast = prever_futuro(model, prophet_model, df_preparado, valor_col, rmin, rmax)

    # Prepara o payload de metadados
    metadata = {
        "metrics": metrics,
        "hyperparameters": hyperparams,
        "features": FEATURES,
        "target": "residual_log",
        "dataset_info": {
            "source_table": table_name,
            "target_column": valor_col,
            "total_records": len(df_preparado),
            "period_min": str(df_preparado["ano"].min().year),
            "period_max": str(df_preparado["ano"].max().year),
        },
        "extra": {
            "residual_bounds": {"min": rmin, "max": rmax},
            "forecast_horizon_years": 5,
        },
    }

    # Salva o bundle Prophet+XGBoost (.pkl) junto do arquivo _meta.json,
    # permitindo que a API sirva previsões a partir do artefato sem re-treinar.
    save_model_with_metadata(model, MODEL_PATH, metadata, prophet_model=prophet_model)

    logger.info("🏁 Pipeline finalizado")
    logger.info({"metrics_residual": metrics, "forecast": forecast})

    return forecast


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    executar_pipeline()
