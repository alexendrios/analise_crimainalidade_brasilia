import json
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from api.services import classificacao_service
from analysis.logistic_regression import (
    ALVO,
    FEATURES,
    criar_modelo,
    treinar_regressao_logistica,
)

MODULO = "api.services.classificacao_service"


@pytest.fixture(autouse=True)
def limpar_cache():
    classificacao_service.limpar_cache()
    yield
    classificacao_service.limpar_cache()


def _gerar_base(n_ras=6, n_anos=6, seed=42):
    rng = np.random.default_rng(seed)
    linhas = []
    for i in range(n_ras):
        ra = f"RA_{i}"
        populacao = int(rng.integers(50_000, 300_000))
        for ano in range(2015, 2015 + n_anos):
            linhas.append(
                {
                    "regiao_administrativa": ra,
                    "ano": ano,
                    "ocorrencia_homicidio": int(rng.integers(0, 40)),
                    "ocorrencia_latrocinio": int(rng.integers(0, 5)),
                    "ocorrencia_lesao_morte": int(rng.integers(0, 3)),
                    "populacao": populacao,
                }
            )
    return pd.DataFrame(linhas)


@pytest.fixture
def df_base():
    return _gerar_base()


@pytest.fixture
def df_features(df_base):
    df, limiar = classificacao_service.preparar_features(df_base)
    return df, limiar


# ============================================================
# _localizar_ultimo_artefato
# ============================================================
def test_localizar_ultimo_artefato_diretorio_vazio(tmp_path):
    modelo, meta = classificacao_service._localizar_ultimo_artefato(str(tmp_path))

    assert modelo is None
    assert meta is None


def test_localizar_ultimo_artefato_retorna_o_mais_recente(tmp_path):
    for nome, criado_em in [
        ("logreg_criminalidade_letal_20260101_000000", "2026-01-01T00:00:00"),
        ("logreg_criminalidade_letal_20260201_000000", "2026-02-01T00:00:00"),
    ]:
        (tmp_path / f"{nome}.pkl").write_bytes(b"fake")
        meta = {
            "model_file": f"{nome}.pkl",
            "created_at": criado_em,
            "metrics": {},
            "extra": {},
        }
        (tmp_path / f"{nome}_meta.json").write_text(
            json.dumps(meta), encoding="utf-8"
        )

    model_path, meta = classificacao_service._localizar_ultimo_artefato(str(tmp_path))

    assert model_path.endswith("logreg_criminalidade_letal_20260201_000000.pkl")
    assert meta["created_at"] == "2026-02-01T00:00:00"


def test_localizar_ultimo_artefato_ignora_meta_corrompido_e_pkl_ausente(tmp_path):
    (tmp_path / "logreg_criminalidade_letal_quebrado_meta.json").write_text(
        "{json inválido", encoding="utf-8"
    )
    meta_valido = {
        "model_file": "logreg_criminalidade_letal_sumido.pkl",
        "created_at": "2026-03-01T00:00:00",
    }
    (tmp_path / "logreg_criminalidade_letal_sumido_meta.json").write_text(
        json.dumps(meta_valido), encoding="utf-8"
    )

    model_path, meta = classificacao_service._localizar_ultimo_artefato(str(tmp_path))

    assert model_path is None
    assert meta is None


# ============================================================
# classificar_criminalidade — erros e retreino
# ============================================================
@pytest.mark.parametrize("vazio", [None, pd.DataFrame()])
def test_classificar_dados_ausentes_levanta_erro(vazio):
    with patch(f"{MODULO}.carregar_dados", return_value=vazio):
        with pytest.raises(classificacao_service.DadosInsuficientesError):
            classificacao_service.classificar_criminalidade()


def test_classificar_retreino_sem_persistir(df_base, df_features):
    df, limiar = df_features

    with (
        patch(f"{MODULO}.carregar_dados", return_value=df_base),
        patch(f"{MODULO}._tentar_servir_de_artefato", return_value=None),
    ):
        resultado = classificacao_service.classificar_criminalidade()

    assert resultado["fonte_modelo"] == "retreino"
    assert resultado["modelo_arquivo"] is None
    assert resultado["tabelas_origem"] == [
        "crimes_letais_gold",
        "populacao_regiao_administrativa",
    ]
    assert resultado["total_registros"] == len(df)
    assert resultado["total_ras"] == 6
    assert resultado["periodo"] == [2015, 2020]
    assert resultado["limiar_taxa_mediana"] == pytest.approx(limiar)
    assert set(resultado["distribuicao_real"]) == {"alta", "baixa"}
    assert resultado["distribuicao_real"]["alta"] == int(df[ALVO].sum())
    assert len(resultado["classificacoes"]) == len(df)
    assert set(FEATURES) <= set(resultado["odds_ratios"])
    assert all(
        0.0 <= item["probabilidade_alta"] <= 1.0
        for item in resultado["classificacoes"]
    )
    probs = [item["probabilidade_alta"] for item in resultado["classificacoes"]]
    assert probs == sorted(probs, reverse=True)


def test_classificar_usa_cache_na_segunda_chamada(df_base):
    with (
        patch(f"{MODULO}.carregar_dados", return_value=df_base) as mock_carga,
        patch(f"{MODULO}._tentar_servir_de_artefato", return_value=None),
    ):
        primeiro = classificacao_service.classificar_criminalidade()
        segundo = classificacao_service.classificar_criminalidade()

    assert segundo == primeiro
    assert mock_carga.call_count == 1


def test_classificar_cache_expirado_recarga_dados(df_base):
    classificacao_service._CACHE[classificacao_service._CHAVE_CACHE] = (0.0, {})

    with (
        patch(f"{MODULO}.carregar_dados", return_value=df_base) as mock_carga,
        patch(f"{MODULO}._tentar_servir_de_artefato", return_value=None),
    ):
        classificacao_service.classificar_criminalidade()

    assert mock_carga.call_count == 1


def test_classificar_forcar_retreino_persiste_modelo(df_base, tmp_path):
    caminho_fake = str(tmp_path / "logreg_criminalidade_letal_novo.pkl")

    with (
        patch(f"{MODULO}.carregar_dados", return_value=df_base),
        patch(f"{MODULO}.salvar_modelo", return_value=(caminho_fake, "meta.json")) as mock_salvar,
    ):
        resultado = classificacao_service.classificar_criminalidade(
            usar_cache=False,
            forcar_retreino=True,
            persistir_modelo=True,
        )

    assert resultado["fonte_modelo"] == "retreino"
    assert resultado["modelo_arquivo"] == "logreg_criminalidade_letal_novo.pkl"
    mock_salvar.assert_called_once()


def test_classificar_forcar_retreino_sem_persistir_nao_salva(df_base):
    with (
        patch(f"{MODULO}.carregar_dados", return_value=df_base),
        patch(f"{MODULO}.salvar_modelo") as mock_salvar,
    ):
        resultado = classificacao_service.classificar_criminalidade(
            forcar_retreino=True,
            persistir_modelo=False,
        )

    assert resultado["modelo_arquivo"] is None
    mock_salvar.assert_not_called()


# ============================================================
# classificar_criminalidade — serving de artefato
# ============================================================
def _treinar_e_salvar_artefato(df_features, models_dir):
    df, _ = df_features
    treinado = treinar_regressao_logistica(df)
    with patch("analysis.logistic_regression.MODELS_DIR", str(models_dir)):
        model_path, meta_path = classificacao_service.salvar_modelo(treinado)
    return model_path, meta_path


def test_classificar_serve_de_artefato_existente(df_base, df_features, tmp_path):
    df, limiar = df_features
    model_path, meta_path = _treinar_e_salvar_artefato(df_features, tmp_path)

    with open(meta_path, encoding="utf-8") as f:
        meta_esperado = json.load(f)

    with (
        patch(f"{MODULO}.carregar_dados", return_value=df_base),
        patch(f"{MODULO}.MODELS_DIR", str(tmp_path)),
    ):
        resultado = classificacao_service.classificar_criminalidade()

    assert resultado["fonte_modelo"] == "artefato"
    assert resultado["modelo_arquivo"] == meta_esperado["model_file"]
    assert resultado["metricas"] == meta_esperado["metrics"]
    assert resultado["odds_ratios"] == meta_esperado["extra"]["odds_ratios"]
    assert resultado["matriz_confusao"] == meta_esperado["extra"]["matriz_confusao"]
    assert len(resultado["classificacoes"]) == len(df)


def test_classificar_artefato_corrompido_cai_para_retreino(df_base, df_features, tmp_path):
    _, limiar = df_features
    nome = "logreg_criminalidade_letal_quebrado"
    (tmp_path / f"{nome}.pkl").write_bytes(b"nao-e-um-pickle")
    meta = {
        "model_file": f"{nome}.pkl",
        "created_at": "2026-01-01T00:00:00",
        "metrics": {},
        "extra": {},
    }
    (tmp_path / f"{nome}_meta.json").write_text(json.dumps(meta), encoding="utf-8")

    with (
        patch(f"{MODULO}.carregar_dados", return_value=df_base),
        patch(f"{MODULO}.MODELS_DIR", str(tmp_path)),
    ):
        resultado = classificacao_service.classificar_criminalidade()

    assert resultado["fonte_modelo"] == "retreino"


def test_classificar_com_modelo_real_prediz_classes_e_probabilidades(df_features):
    df, _ = df_features
    modelo = criar_modelo()
    modelo.fit(df[FEATURES], df[ALVO])

    preds, probas = classificacao_service._classificar_com_modelo(modelo, df)

    assert len(preds) == len(probas) == len(df)
    assert set(preds) <= {0, 1}
    assert all(0.0 <= pr <= 1.0 for pr in probas)


def test_montar_classificacoes_ordena_por_probabilidade_desc(df_features):
    df, _ = df_features
    modelo = criar_modelo()
    modelo.fit(df[FEATURES], df[ALVO])
    preds, probas = classificacao_service._classificar_com_modelo(modelo, df)

    itens = classificacao_service._montar_classificacoes(df, preds, probas)

    probs = [item["probabilidade_alta"] for item in itens]
    assert probs == sorted(probs, reverse=True)
    primeiro = itens[0]
    assert set(primeiro) == {
        "regiao_administrativa",
        "ano",
        "classe_prevista",
        "rotulo_previsto",
        "probabilidade_alta",
    }
    assert primeiro["rotulo_previsto"] == ("alta" if primeiro["classe_prevista"] == 1 else "baixa")


def test_limpar_cache_esvazia_cache():
    classificacao_service._CACHE["x"] = (0.0, {})
    classificacao_service.limpar_cache()
    assert classificacao_service._CACHE == {}
