# =========================================================
# IMPORTS
# =========================================================
import json
import os
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from ingestion.repository_adapter import Repository
from util.log import logs
from util.padronizacao import normalizar_colunas, padronizar_regiao

logger = logs()

# =========================================================
# CONFIG
# =========================================================
TABELA_CRIMES_LETAIS = "crimes_letais_gold"
TABELA_POPULACAO = "populacao_regiao_administrativa"

FEATURES = [
    "taxa_homicidio",
    "taxa_latrocinio",
    "taxa_lesao_morte",
    "log_populacao",
    "ano_num",
]
ALVO = "alta_criminalidade"

RANDOM_STATE = 42
TEST_SIZE = 0.25
N_FOLDS_CV = 5
FATOR_TAXA = 100_000

MODELS_DIR = "models"
OUTPUTS_DIR = os.path.join("analysis", "outputs")


# =========================================================
# CARGA E PREPARAÇÃO DOS DADOS
# =========================================================
def carregar_dados():
    """
    Carrega crimes letais (RA/ano) e população por RA do banco, padroniza os
    nomes das regiões e retorna o DataFrame já agregado pela chave (RA, ano).
    """
    df_crimes = Repository.load(TABELA_CRIMES_LETAIS)
    df_pop = Repository.load(TABELA_POPULACAO)

    if df_crimes.empty:
        raise ValueError(f"Tabela {TABELA_CRIMES_LETAIS} está vazia")
    if df_pop.empty:
        raise ValueError(f"Tabela {TABELA_POPULACAO} está vazia")

    df_pop = normalizar_colunas(df_pop)

    df_crimes = padronizar_regiao(df_crimes, "regiao_administrativa")
    df_pop = padronizar_regiao(df_pop, "regiao_administrativa")

    duplicatas_pop = df_pop["regiao_administrativa"].duplicated().sum()
    if duplicatas_pop > 0:
        logger.warning(
            f"⚠️ {duplicatas_pop} RAs duplicadas na população; mantendo a última ocorrência"
        )
        df_pop = df_pop.drop_duplicates(subset="regiao_administrativa", keep="last")

    df = df_crimes.merge(
        df_pop[["regiao_administrativa", "populacao"]],
        on="regiao_administrativa",
        how="inner",
        validate="many_to_one",
    )

    sem_populacao = df["populacao"].isna().sum()
    if sem_populacao > 0:
        logger.warning(f"⚠️ {sem_populacao} linhas sem população correspondente")

    n_sem_match = len(df_crimes) - len(df)
    if n_sem_match > 0:
        logger.warning(
            f"⚠️ {n_sem_match} linhas de crimes descartadas por não terem população correspondente"
        )

    logger.info(f"📊 Base conjunta: {len(df)} linhas | {df['regiao_administrativa'].nunique()} RAs")
    return df


def preparar_features(df):
    """
    Cria as features de taxa por 100 mil habitantes e o alvo binário
    `alta_criminalidade` (taxa total de crimes letais >= mediana da base).
    """
    df = df.copy()

    populacao_invalida = (df["populacao"] <= 0).sum()
    if populacao_invalida > 0:
        raise ValueError(f"{populacao_invalida} linhas com população <= 0")

    for coluna, feature in [
        ("ocorrencia_homicidio", "taxa_homicidio"),
        ("ocorrencia_latrocinio", "taxa_latrocinio"),
        ("ocorrencia_lesao_morte", "taxa_lesao_morte"),
    ]:
        if coluna not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {coluna}")
        df[feature] = df[coluna] / df["populacao"] * FATOR_TAXA

    df["log_populacao"] = np.log(df["populacao"])
    df["ano_num"] = df["ano"].astype(int)

    taxa_total = (
        df["taxa_homicidio"]
        + df["taxa_latrocinio"]
        + df["taxa_lesao_morte"]
    )
    limiar = float(taxa_total.median())
    df[ALVO] = (taxa_total >= limiar).astype(int)

    n_altas = int(df[ALVO].sum())
    logger.info(
        f"🎯 Alvo '{ALVO}': limiar={limiar:.2f}/100k | "
        f"alta={n_altas} ({n_altas / len(df):.1%}) | baixa={len(df) - n_altas}"
    )

    return df, limiar


# =========================================================
# TREINAMENTO E AVALIAÇÃO
# =========================================================
def criar_modelo():
    """
    Pipeline com padronização das features e Regressão Logística.
    O scaler é essencial: os coeficientes ficam em escala comparável e a
    regularização L2 atua de forma uniforme.
    """
    return Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "logreg",
                LogisticRegression(max_iter=1000, random_state=RANDOM_STATE),
            ),
        ]
    )


def treinar_regressao_logistica(df):
    """
    Treina a Regressão Logística com validação estratificada.

    :return: dicionário com modelo, métricas de CV e holdout, matriz de
        confusão e odds ratios interpretáveis dos coeficientes.
    """
    X = df[FEATURES]
    y = df[ALVO]

    if y.nunique() < 2:
        raise ValueError("Alvo possui apenas uma classe; não é possível treinar")

    X_treino, X_teste, y_treino, y_teste = train_test_split(
        X, y, test_size=TEST_SIZE, stratify=y, random_state=RANDOM_STATE
    )

    modelo = criar_modelo()

    cv = StratifiedKFold(n_splits=N_FOLDS_CV, shuffle=True, random_state=RANDOM_STATE)
    scores_cv = cross_val_score(modelo, X_treino, y_treino, cv=cv, scoring="roc_auc")

    modelo.fit(X_treino, y_treino)

    y_pred = modelo.predict(X_teste)
    y_proba = modelo.predict_proba(X_teste)[:, 1]

    logreg = modelo.named_steps["logreg"]
    odds_ratios = {
        feat: float(np.exp(coef))
        for feat, coef in zip(FEATURES, logreg.coef_[0])
    }

    metricas = {
        "cv_roc_auc_media": float(scores_cv.mean()),
        "cv_roc_auc_std": float(scores_cv.std()),
        "holdout_accuracy": float(accuracy_score(y_teste, y_pred)),
        "holdout_precision": float(precision_score(y_teste, y_pred, zero_division=0)),
        "holdout_recall": float(recall_score(y_teste, y_pred, zero_division=0)),
        "holdout_f1": float(f1_score(y_teste, y_pred, zero_division=0)),
        "holdout_roc_auc": float(roc_auc_score(y_teste, y_proba)),
    }
    matriz = confusion_matrix(y_teste, y_pred).tolist()

    logger.info(f"📈 CV ROC-AUC: {metricas['cv_roc_auc_media']:.3f} ± {metricas['cv_roc_auc_std']:.3f}")
    logger.info(f"📈 Holdout ROC-AUC: {metricas['holdout_roc_auc']:.3f} | F1: {metricas['holdout_f1']:.3f}")

    return {
        "modelo": modelo,
        "metricas": metricas,
        "matriz_confusao": matriz,
        "odds_ratios": odds_ratios,
        "coeficientes": dict(zip(FEATURES, logreg.coef_[0].tolist())),
        "intercepto": float(logreg.intercept_[0]),
        "tamanhos": {"treino": len(X_treino), "teste": len(X_teste)},
    }


# =========================================================
# PERSISTÊNCIA
# =========================================================
def salvar_modelo(resultado, model_path=None):
    """
    Salva o pipeline treinado (.pkl) e metadados padronizados (_meta.json),
    seguindo o mesmo formato dos demais artefatos em models/.
    """
    if model_path is None:
        nome = f"logreg_criminalidade_letal_{datetime.now().strftime('%Y%m%d_%H%M')}"
        model_path = os.path.join(MODELS_DIR, f"{nome}.pkl")

    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    joblib.dump(resultado["modelo"], model_path)

    base_path, _ = os.path.splitext(model_path)
    meta_path = f"{base_path}_meta.json"

    metadata = {
        "created_at": datetime.now().isoformat(),
        "model_file": os.path.basename(model_path),
        "model_type": type(resultado["modelo"]).__name__,
        "artifact_format": "pipeline",
        "metrics": resultado["metricas"],
        "hyperparameters": {
            "max_iter": 1000,
            "random_state": RANDOM_STATE,
            "test_size": TEST_SIZE,
            "cv_folds": N_FOLDS_CV,
        },
        "features": FEATURES,
        "target": ALVO,
        "dataset_info": {},
        "extra": {
            "odds_ratios": resultado["odds_ratios"],
            "coeficientes": resultado["coeficientes"],
            "intercepto": resultado["intercepto"],
            "matriz_confusao": resultado["matriz_confusao"],
            "tamanhos_treino_teste": resultado["tamanhos"],
        },
    }

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)

    logger.info(f"✅ Modelo salvo em: {model_path}")
    logger.info(f"📄 Metadados salvos em: {meta_path}")

    return model_path, meta_path


# =========================================================
# VISUALIZAÇÃO (PLOTLY)
# =========================================================
def graficar_odds_ratios(odds_ratios):
    """
    Odds ratios por feature em barra horizontal; 1.0 significa efeito neutro.
    """
    itens = sorted(odds_ratios.items(), key=lambda kv: abs(np.log(kv[1])))
    nomes = [nome for nome, _ in itens]
    valores = [valor for _, valor in itens]

    fig = go.Figure(
        go.Bar(
            x=valores,
            y=nomes,
            orientation="h",
            marker_color=[
                "#d62728" if v >= 1 else "#1f77b4" for v in valores
            ],
            text=[f"{v:.3f}" for v in valores],
            textposition="outside",
        )
    )
    fig.add_vline(x=1.0, line_dash="dash", line_color="gray")
    fig.update_layout(
        title="Regressão Logística — Odds Ratios por Feature",
        xaxis_title="Odds Ratio",
        yaxis_title="Feature",
        template="plotly_white",
    )
    return fig


def graficar_matriz_confusao(matriz):
    """
    Heatmap da matriz de confusão no holdout.
    """
    z = np.array(matriz)
    rotulos = [["VN", "FP"], ["FN", "VP"]]
    textos = [
        [f"{rotulos[i][j]}<br>{z[i][j]}" for j in range(z.shape[1])]
        for i in range(z.shape[0])
    ]

    fig = go.Figure(
        go.Heatmap(
            z=z,
            x=["Previsto: baixa", "Previsto: alta"],
            y=["Real: baixa", "Real: alta"],
            text=textos,
            texttemplate="%{text}",
            colorscale="Blues",
            showscale=False,
        )
    )
    fig.update_layout(
        title="Matriz de Confusão (Holdout)",
        xaxis_title="Predição",
        yaxis_title="Valor Real",
        template="plotly_white",
    )
    return fig


def salvar_graficos(fig_odds, fig_matriz):
    """
    Persiste os gráficos como HTML interativo em analysis/outputs/.
    """
    os.makedirs(OUTPUTS_DIR, exist_ok=True)

    caminho_odds = os.path.join(OUTPUTS_DIR, "logreg_odds_ratios.html")
    caminho_matriz = os.path.join(OUTPUTS_DIR, "logreg_matriz_confusao.html")

    fig_odds.write_html(caminho_odds)
    fig_matriz.write_html(caminho_matriz)

    logger.info(f"🖼️ Gráficos salvos em: {OUTPUTS_DIR}")
    return caminho_odds, caminho_matriz


# =========================================================
# PIPELINE
# =========================================================
def executar_pipeline(salvar_arquivos=True):
    """
    Pipeline completo: carga -> features -> treino/validação -> artefatos.
    """
    logger.info("🚀 Pipeline Regressão Logística iniciado")

    df_bruto = carregar_dados()
    df, limiar = preparar_features(df_bruto)

    resultado = treinar_regressao_logistica(df)

    info_dataset = {
        "source_tables": [TABELA_CRIMES_LETAIS, TABELA_POPULACAO],
        "total_records": len(df),
        "ras": int(df["regiao_administrativa"].nunique()),
        "periodo": [int(df["ano_num"].min()), int(df["ano_num"].max())],
        "limiar_taxa_mediana": limiar,
        "distribuicao_alvo": df[ALVO].value_counts().to_dict(),
    }
    resultado["dataset_info"] = info_dataset

    if salvar_arquivos:
        model_path, meta_path = salvar_modelo(resultado)

        with open(meta_path, encoding="utf-8") as f:
            meta = json.load(f)
        meta["dataset_info"] = info_dataset
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=4, ensure_ascii=False)

        fig_odds = graficar_odds_ratios(resultado["odds_ratios"])
        fig_matriz = graficar_matriz_confusao(resultado["matriz_confusao"])
        caminhos = salvar_graficos(fig_odds, fig_matriz)
        resultado["artefatos"] = {"modelo": model_path, "meta": meta_path, **dict(zip(["grafico_odds", "grafico_matriz"], caminhos))}

    logger.info("🏁 Pipeline Regressão Logística finalizado")
    logger.info({"metricas": resultado["metricas"], "odds_ratios": resultado["odds_ratios"]})

    return resultado


if __name__ == "__main__":
    executar_pipeline()
