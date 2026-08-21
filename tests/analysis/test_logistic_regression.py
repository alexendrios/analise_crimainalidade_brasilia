import json
import re
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from analysis.logistic_regression import (
    ALVO,
    FEATURES,
    carregar_dados,
    criar_modelo,
    executar_pipeline,
    graficar_matriz_confusao,
    graficar_odds_ratios,
    preparar_features,
    salvar_graficos,
    salvar_modelo,
    treinar_regressao_logistica,
)

MODULO = "analysis.logistic_regression"


# ============================================================
# FÁBRICAS DE DADOS DE TESTE
# ============================================================
def _gerar_base_crimes(n_ras=6, n_anos=6, seed=42):
    rng = np.random.default_rng(seed)
    linhas = []
    for i in range(n_ras):
        ra = f"RA_{i}"
        for ano in range(2015, 2015 + n_anos):
            linhas.append(
                {
                    "regiao_administrativa": ra,
                    "ano": ano,
                    "ocorrencia_homicidio": int(rng.integers(0, 40)),
                    "ocorrencia_latrocinio": int(rng.integers(0, 5)),
                    "ocorrencia_lesao_morte": int(rng.integers(0, 3)),
                }
            )
    return pd.DataFrame(linhas)


@pytest.fixture
def df_base():
    df = _gerar_base_crimes()
    rng = np.random.default_rng(42)
    populacoes = {
        f"RA_{i}": int(rng.integers(50_000, 300_000)) for i in range(6)
    }
    df["populacao"] = df["regiao_administrativa"].map(populacoes)
    return df


@pytest.fixture
def df_features(df_base):
    df, limiar = preparar_features(df_base)
    return df, limiar


# ============================================================
# carregar_dados
# ============================================================
def test_carregar_dados_faz_merge_e_padroniza_regioes():
    df_crimes = _gerar_base_crimes()
    df_pop = pd.DataFrame(
        {
            "região administrativa": ["Ra_0", "Ra_1", "Ra_2", "Ra_3", "Ra_4", "Ra_5"],
            "população": [100_000, 200_000, 300_000, 50_000, 150_000, 250_000],
        }
    )

    with patch(f"{MODULO}.Repository") as mock_repo:
        mock_repo.load.side_effect = [df_crimes, df_pop]
        resultado = carregar_dados()

    assert len(resultado) == len(df_crimes)
    assert "populacao" in resultado.columns
    assert set(resultado["regiao_administrativa"]) == {f"RA_{i}" for i in range(6)}
    assert mock_repo.load.call_count == 2


def test_carregar_dados_descarta_crimes_sem_populacao_correspondente():
    df_crimes = _gerar_base_crimes()
    df_pop = pd.DataFrame(
        {
            "região administrativa": [f"RA_{i}" for i in range(5)],
            "população": [100_000] * 5,
        }
    )

    with patch(f"{MODULO}.Repository") as mock_repo:
        mock_repo.load.side_effect = [df_crimes, df_pop]
        resultado = carregar_dados()

    assert len(resultado) < len(df_crimes)
    assert not (resultado["regiao_administrativa"] == "RA_5").any()


def test_carregar_dados_remove_duplicatas_de_populacao():
    df_crimes = _gerar_base_crimes()
    df_pop = pd.DataFrame(
        {
            "região administrativa": ["RA_0", "RA_0", "RA_1"],
            "população": [111_111, 222_222, 333_333],
        }
    )

    with patch(f"{MODULO}.Repository") as mock_repo:
        mock_repo.load.side_effect = [df_crimes, df_pop]
        resultado = carregar_dados()

    pop_ra1 = resultado.loc[
        resultado["regiao_administrativa"] == "RA_1", "populacao"
    ].unique()
    assert len(pop_ra1) == 1 and pop_ra1[0] == 333_333


@pytest.mark.parametrize("tabela_vazia", ["crimes", "populacao"])
def test_carregar_dados_tabela_vazia_levanta_value_error(tabela_vazia):
    df_crimes = _gerar_base_crimes() if tabela_vazia != "crimes" else pd.DataFrame()
    df_pop = (
        pd.DataFrame({"região administrativa": ["RA_0"], "população": [100_000]})
        if tabela_vazia != "populacao"
        else pd.DataFrame()
    )

    with patch(f"{MODULO}.Repository") as mock_repo:
        mock_repo.load.side_effect = [df_crimes, df_pop]
        with pytest.raises(ValueError, match="vazia"):
            carregar_dados()


# ============================================================
# preparar_features
# ============================================================
def test_preparar_features_cria_taxas_e_alvo_por_mediana(df_base):
    df, limiar = preparar_features(df_base)

    assert FEATURES + [ALVO] == [
        c for c in df.columns if c in FEATURES + [ALVO]
    ]
    esperado = (
        df_base["ocorrencia_homicidio"] / df_base["populacao"] * 100_000
    )
    pd.testing.assert_series_equal(
        df["taxa_homicidio"], esperado, check_names=False
    )
    assert df[ALVO].nunique() == 2
    assert df[ALVO].sum() >= len(df) // 2 - 1
    taxa_total = df["taxa_homicidio"] + df["taxa_latrocinio"] + df["taxa_lesao_morte"]
    assert limiar == pytest.approx(float(taxa_total.median()))


def test_preparar_features_populacao_invalida_levanta_erro(df_base):
    df_base["populacao"] = 0
    with pytest.raises(ValueError, match="população <= 0"):
        preparar_features(df_base)


def test_preparar_features_coluna_obrigatoria_ausente_levanta_erro(df_base):
    df_base = df_base.drop(columns=["ocorrencia_homicidio"])
    with pytest.raises(ValueError, match="ocorrencia_homicidio"):
        preparar_features(df_base)


# ============================================================
# criar_modelo
# ============================================================
def test_criar_modelo_eh_pipeline_com_scaler_e_logreg():
    modelo = criar_modelo()

    nomes = [nome for nome, _ in modelo.steps]
    assert nomes == ["scaler", "logreg"]
    logreg = modelo.named_steps["logreg"]
    assert logreg.max_iter == 1000
    assert logreg.random_state == 42


# ============================================================
# treinar_regressao_logistica
# ============================================================
def test_treinar_regressao_logistica_retorna_resultado_completo(df_features):
    df, _ = df_features

    resultado = treinar_regressao_logistica(df)

    for chave in [
        "modelo",
        "metricas",
        "matriz_confusao",
        "odds_ratios",
        "coeficientes",
        "intercepto",
        "tamanhos",
    ]:
        assert chave in resultado

    metricas = resultado["metricas"]
    for valor in metricas.values():
        assert 0.0 <= valor <= 1.0
    assert "cv_roc_auc_media" in metricas
    assert "holdout_roc_auc" in metricas

    assert set(resultado["odds_ratios"]) == set(FEATURES)
    assert all(v > 0 for v in resultado["odds_ratios"].values())
    assert set(resultado["coeficientes"]) == set(FEATURES)
    assert isinstance(resultado["intercepto"], float)

    matriz = np.array(resultado["matriz_confusao"])
    assert matriz.sum() == resultado["tamanhos"]["teste"]
    assert resultado["tamanhos"]["treino"] + resultado["tamanhos"]["teste"] == len(df)

    X_fake = df[FEATURES].head(3)
    preds = resultado["modelo"].predict(X_fake)
    assert len(preds) == 3


def test_treinar_regressao_logistica_classe_unica_levanta_erro(df_features):
    df, _ = df_features
    df[ALVO] = 0

    with pytest.raises(ValueError, match="apenas uma classe"):
        treinar_regressao_logistica(df)


# ============================================================
# salvar_modelo
# ============================================================
def test_salvar_modelo_grava_pkl_e_meta_json(tmp_path, df_features):
    df, limiar = df_features
    resultado = treinar_regressao_logistica(df)
    model_path = str(tmp_path / "modelos" / "logreg_teste.pkl")

    with patch(f"{MODULO}.joblib.dump") as mock_dump:
        caminho_pkl, caminho_meta = salvar_modelo(resultado, model_path)

    mock_dump.assert_called_once_with(resultado["modelo"], model_path)
    assert caminho_pkl == model_path

    with open(caminho_meta, encoding="utf-8") as f:
        meta = json.load(f)

    assert meta["model_file"] == "logreg_teste.pkl"
    assert meta["artifact_format"] == "pipeline"
    assert meta["features"] == FEATURES
    assert meta["target"] == ALVO
    assert meta["metrics"] == resultado["metricas"]
    assert meta["extra"]["odds_ratios"] == resultado["odds_ratios"]
    assert meta["extra"]["matriz_confusao"] == resultado["matriz_confusao"]
    assert meta["hyperparameters"]["random_state"] == 42
    assert "created_at" in meta


def test_salvar_modelo_caminho_padrao_usa_models_dir(tmp_path, df_features):
    df, _ = df_features
    resultado = treinar_regressao_logistica(df)

    with patch(f"{MODULO}.MODELS_DIR", str(tmp_path)), patch(f"{MODULO}.joblib.dump"):
        caminho_pkl, caminho_meta = salvar_modelo(resultado)

    assert caminho_pkl.startswith(str(tmp_path))
    assert re.search(r"logreg_criminalidade_letal_\d{8}_\d{4}\.pkl$", caminho_pkl)
    assert caminho_meta.endswith("_meta.json")


# ============================================================
# GRÁFICOS (PLOTLY)
# ============================================================
def test_graficar_odds_ratios_retorna_figura_com_barras():
    odds = {"taxa_homicidio": 2.5, "taxa_latrocinio": 0.8, "ano_num": 1.01}

    fig = graficar_odds_ratios(odds)

    barra = fig.data[0]
    assert barra.type == "bar"
    assert list(barra.y) == ["ano_num", "taxa_latrocinio", "taxa_homicidio"]
    assert any(vline.x0 == 1.0 for vline in fig.layout.shapes)


def test_graficar_matriz_confusao_retorna_heatmap_com_rotulos():
    fig = graficar_matriz_confusao([[8, 2], [1, 9]])

    heatmap = fig.data[0]
    assert heatmap.type == "heatmap"
    texto = "".join(cell for linha in heatmap.text for cell in linha)
    for rotulo in ["VN", "FP", "FN", "VP"]:
        assert rotulo in texto


def test_salvar_graficos_escreve_html_em_outputs(tmp_path):
    fig_odds = graficar_odds_ratios({"taxa_homicidio": 2.5})
    fig_matriz = graficar_matriz_confusao([[8, 2], [1, 9]])

    outputs_dir = tmp_path / "outputs"
    with patch(f"{MODULO}.OUTPUTS_DIR", str(outputs_dir)):
        caminho_odds, caminho_matriz = salvar_graficos(fig_odds, fig_matriz)

    assert (outputs_dir / "logreg_odds_ratios.html").exists()
    assert (outputs_dir / "logreg_matriz_confusao.html").exists()
    assert caminho_odds.endswith("logreg_odds_ratios.html")
    assert caminho_matriz.endswith("logreg_matriz_confusao.html")


# ============================================================
# executar_pipeline
# ============================================================
def test_executar_pipeline_fluxo_completo_salva_artefatos(tmp_path):
    df_crimes = _gerar_base_crimes()
    df_pop = pd.DataFrame(
        {
            "região administrativa": [f"RA_{i}" for i in range(6)],
            "população": [100_000, 200_000, 300_000, 50_000, 150_000, 250_000],
        }
    )

    models_dir = tmp_path / "models"
    outputs_dir = tmp_path / "outputs"

    with (
        patch(f"{MODULO}.Repository") as mock_repo,
        patch(f"{MODULO}.MODELS_DIR", str(models_dir)),
        patch(f"{MODULO}.OUTPUTS_DIR", str(outputs_dir)),
    ):
        mock_repo.load.side_effect = [df_crimes, df_pop]
        resultado = executar_pipeline(salvar_arquivos=True)

    assert "metricas" in resultado
    assert "dataset_info" in resultado
    info = resultado["dataset_info"]
    assert info["total_records"] == len(df_crimes)
    assert info["ras"] == 6
    assert info["periodo"] == [2015, 2020]

    artefatos = resultado["artefatos"]
    assert artefatos["modelo"].startswith(str(models_dir))
    assert (tmp_path / artefatos["meta"].replace(str(tmp_path) + "\\", "")).exists()
    assert (outputs_dir / "logreg_odds_ratios.html").exists()
    assert (outputs_dir / "logreg_matriz_confusao.html").exists()


def test_executar_pipeline_sem_salvar_nao_escreve_arquivos(tmp_path):
    df_crimes = _gerar_base_crimes()
    df_pop = pd.DataFrame(
        {
            "região administrativa": [f"RA_{i}" for i in range(6)],
            "população": [100_000] * 6,
        }
    )

    with patch(f"{MODULO}.Repository") as mock_repo:
        mock_repo.load.side_effect = [df_crimes, df_pop]
        resultado = executar_pipeline(salvar_arquivos=False)

    assert "artefatos" not in resultado
    assert "metricas" in resultado
