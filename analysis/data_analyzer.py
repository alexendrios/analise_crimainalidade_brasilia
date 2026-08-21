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
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

from ingestion.repository_adapter import Repository
from util.config_loader import get_config
from util.log import logs

logger = logs()

# =========================================================
# CONFIG
# =========================================================
_config_modelagem = get_config().get("modelagem", {})

TABELA_MODELO = _config_modelagem.get("tabela_gold", "violencia_contra_mulher_gold")
COLUNA_ALVO = _config_modelagem.get("coluna_alvo", "crimes_contra_mulher")
HORIZONTE_ANOS_PADRAO = int(_config_modelagem.get("horizonte_anos", 5))

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

# Versão da definição de features. A semântica de diff_1 mudou para ser
# estritamente causal (usa apenas valores defasados), então artefatos
# treinados com a definição antiga não devem servir previsão.
VERSAO_FEATURES = 2

PARAMS_XGB = {
    "n_estimators": 600,
    "learning_rate": 0.015,
    "max_depth": 3,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.5,
    "reg_lambda": 2,
    "random_state": 42,
}

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
        "r2": float(r2_score(y_true, y_pred)),
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
    df["diff_1"] = df[valor_col].shift(1).diff(1)

    df = df.dropna()

    return df


# =========================================================
# PROPHET
# =========================================================
def treinar_prophet(df, valor_col):
    """
    Ajusta o Prophet na série informada e retorna (modelo, forecast in-sample).

    Dados anuais (1 observação/ano) não têm ciclos anuais repetidos, então
    a sazonalidade yearly fica indefinida — apenas a tendência é modelada.
    """
    df_p = df[["ano", valor_col]].rename(columns={"ano": "ds", valor_col: "y"})

    model = Prophet(yearly_seasonality=False)
    model.fit(df_p)

    forecast = model.predict(df_p)

    return model, forecast


# =========================================================
# VALIDAÇÃO SEM VAZAMENTO (BACKTESTING ROLLING-ORIGIN)
# =========================================================
def _projetar_prophet(prophet_model, df):
    """Gera yhat do Prophet já ajustado para as datas de df (fora da amostra
    quando df contém períodos não vistos no ajuste)."""
    futuro = prophet_model.predict(df[["ano"]].rename(columns={"ano": "ds"}))
    return futuro["yhat"].to_numpy(dtype=float)


def _residuo_log(y_real, yhat_prophet):
    return np.log1p(np.asarray(y_real, dtype=float)) - np.log1p(yhat_prophet)


def _treinar_xgb(X, y):
    modelo = XGBRegressor(**PARAMS_XGB)
    modelo.fit(X, y)
    return modelo


def _avaliar_fold(df_treino, df_teste, valor_col):
    """Avalia um fold sem vazamento: Prophet ajustado só com o treino do fold
    e projetado fora da amostra sobre o teste; XGBoost aprende o resíduo do
    treino e é medido no teste. Retorna métricas no resíduo (log) e na
    escala original (contagem de casos)."""
    prophet_fold, _ = treinar_prophet(df_treino, valor_col)

    resid_treino = _residuo_log(
        df_treino[valor_col].to_numpy(),
        _projetar_prophet(prophet_fold, df_treino),
    )

    yhat_teste = _projetar_prophet(prophet_fold, df_teste)
    resid_teste = _residuo_log(df_teste[valor_col].to_numpy(), yhat_teste)

    xgb_fold = _treinar_xgb(df_treino[FEATURES], resid_treino)
    preds = xgb_fold.predict(df_teste[FEATURES])

    metricas_residuo = calcular_metricas(resid_teste, preds)
    metricas_original = calcular_metricas(
        df_teste[valor_col].to_numpy(),
        np.expm1(np.log1p(yhat_teste) + preds),
    )
    return metricas_residuo, metricas_original


def _media_metricas(folds):
    chaves = folds[0].keys()
    return {k: float(np.mean([m[k] for m in folds])) for k in chaves}


def _numero_folds(n_amostras):
    if n_amostras >= 12:
        return 3
    if n_amostras >= 6:
        return 2
    return 0


def avaliar_generalizacao(df, valor_col):
    """
    Backtesting rolling-origin via TimeSeriesSplit: em cada fold o Prophet é
    ajustado apenas com o passado daquele fold, eliminando o vazamento de
    avaliar com informação do período de teste. Com série escassa demais para
    múltiplos folds, cai para um único holdout 80/20 (também sem vazamento).

    :return: tupla `(metricas_residuo_log, metricas_escala_original)`, cada
        uma com mae/rmse/r2 agregados pela média dos folds.
    """
    n_folds = _numero_folds(len(df))

    if n_folds == 0:
        logger.warning("⚠️ Série curta demais para backtesting; usando holdout único (80/20)")
        split = int(len(df) * 0.8)
        return _avaliar_fold(df.iloc[:split], df.iloc[split:], valor_col)

    metricas_residuo_folds = []
    metricas_original_folds = []

    for idx_treino, idx_teste in TimeSeriesSplit(n_splits=n_folds).split(df):
        m_res, m_orig = _avaliar_fold(df.iloc[idx_treino], df.iloc[idx_teste], valor_col)
        metricas_residuo_folds.append(m_res)
        metricas_original_folds.append(m_orig)
        logger.info(f"📊 Fold ({len(idx_treino)} treino / {len(idx_teste)} teste): {m_orig}")

    return _media_metricas(metricas_residuo_folds), _media_metricas(metricas_original_folds)


# =========================================================
# TREINAMENTO RESIDUAL (ROBUSTO)
# =========================================================
def treinar_residual(df, valor_col):
    logger.info("🤖 Treinando modelo residual LOG (validação por backtesting)")

    # Métricas honestas: nenhum componente vê o período de teste durante a validação
    metricas_residuo, metricas_original = avaliar_generalizacao(df, valor_col)
    metrics = {**metricas_residuo, "escala_original": metricas_original}

    # Modelo final: Prophet + XGBoost ajustados na série completa
    prophet_model, prophet_fit = treinar_prophet(df, valor_col)
    df["residual"] = _residuo_log(
        df[valor_col].to_numpy(), prophet_fit["yhat"].to_numpy(dtype=float)
    )

    model = _treinar_xgb(df[FEATURES], df["residual"])

    # bounds dinâmicos
    residual_min = float(df["residual"].quantile(0.05))
    residual_max = float(df["residual"].quantile(0.95))

    logger.info(f"📊 Residual bounds: [{residual_min:.4f}, {residual_max:.4f}]")
    logger.info(f"📊 Validação (backtesting): residuo={metricas_residuo} | escala_original={metricas_original}")

    return model, prophet_model, metrics, residual_min, residual_max, dict(PARAMS_XGB)


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

        # UPDATE FEATURES — mesma semântica causal do treino:
        # para a linha r, lag_1=y[r-1], lag_2=y[r-2], diff_1=y[r-1]-y[r-2].
        # Quando o ano anterior foi previsto, seu valor previsto é usado.
        novo = ultimo.copy()
        novo["ano"] += pd.DateOffset(years=1)
        novo[valor_col] = final

        y_anterior = float(ultimo[valor_col].values[0])
        lag2_novo = float(ultimo["lag_1"].values[0])
        lag3_novo = float(ultimo["lag_2"].values[0])

        novo["lag_1"] = y_anterior
        novo["lag_2"] = lag2_novo

        novo["rolling_mean_2"] = np.mean([y_anterior, lag2_novo])
        novo["rolling_mean_3"] = np.mean([y_anterior, lag2_novo, lag3_novo])

        novo["diff_1"] = float(y_anterior - lag2_novo)
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
def executar_pipeline(horizonte_anos: int | None = None):
    logger.info("🚀 Pipeline iniciado")

    horizonte = int(horizonte_anos or HORIZONTE_ANOS_PADRAO)

    df = Repository.load(TABELA_MODELO)
    df_preparado = preparar_dados(df, COLUNA_ALVO)

    (
        model,
        prophet_model,
        metrics,
        rmin,
        rmax,
        hyperparams,
    ) = treinar_residual(df_preparado, COLUNA_ALVO)

    forecast = prever_futuro(
        model, prophet_model, df_preparado, COLUNA_ALVO, rmin, rmax, anos=horizonte
    )

    # Prepara o payload de metadados
    metadata = {
        "metrics": metrics,
        "hyperparameters": hyperparams,
        "features": FEATURES,
        "target": "residual_log",
        "dataset_info": {
            "source_table": TABELA_MODELO,
            "target_column": COLUNA_ALVO,
            "total_records": len(df_preparado),
            "period_min": str(df_preparado["ano"].min().year),
            "period_max": str(df_preparado["ano"].max().year),
        },
        "extra": {
            "residual_bounds": {"min": rmin, "max": rmax},
            "forecast_horizon_years": horizonte,
            "versao_features": VERSAO_FEATURES,
            "estrategia_validacao": "backtesting rolling-origin (TimeSeriesSplit)",
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
