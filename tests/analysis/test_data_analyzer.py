import json
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from analysis.data_analyzer import (
    FEATURES,
    calcular_metricas,
    carregar_modelo,
    executar_pipeline,
    localizar_ultimo_modelo_bundle,
    prever_futuro,
    preparar_dados,
    save_model_with_metadata,
    treinar_prophet,
    treinar_residual,
)

MODULO = "analysis.data_analyzer"


# ============================================================
# save_model_with_metadata
# ============================================================
def test_save_model_with_metadata_salva_pkl_e_json(tmp_path):
    model = MagicMock()
    model_path = str(tmp_path / "modelos" / "xgb_teste.pkl")

    metadata = {
        "metrics": {"mae": 0.1, "rmse": 0.2},
        "hyperparameters": {"n_estimators": 100},
        "features": ["a", "b"],
        "target": "residual_log",
        "dataset_info": {"total_records": 10},
        "extra": {"foo": "bar"},
    }

    with patch(f"{MODULO}.joblib.dump") as mock_dump:
        save_model_with_metadata(model, model_path, metadata)

    mock_dump.assert_called_once_with(model, model_path)

    meta_path = str(tmp_path / "modelos" / "xgb_teste_meta.json")
    with open(meta_path, encoding="utf-8") as f:
        salvo = json.load(f)

    assert salvo["model_file"] == "xgb_teste.pkl"
    assert salvo["metrics"] == {"mae": 0.1, "rmse": 0.2}
    assert salvo["hyperparameters"] == {"n_estimators": 100}
    assert salvo["features"] == ["a", "b"]
    assert salvo["target"] == "residual_log"
    assert salvo["dataset_info"] == {"total_records": 10}
    assert salvo["extra"] == {"foo": "bar"}
    assert "created_at" in salvo


def test_save_model_with_metadata_usa_defaults_quando_metadata_incompleto(tmp_path):
    model = MagicMock()
    model_path = str(tmp_path / "xgb_minimo.pkl")

    with patch(f"{MODULO}.joblib.dump"):
        save_model_with_metadata(model, model_path, {})

    meta_path = str(tmp_path / "xgb_minimo_meta.json")
    with open(meta_path, encoding="utf-8") as f:
        salvo = json.load(f)

    assert salvo["metrics"] == {}
    assert salvo["hyperparameters"] == {}
    assert salvo["features"] == []
    assert salvo["target"] == ""
    assert salvo["dataset_info"] == {}
    assert salvo["extra"] == {}


def test_save_model_with_metadata_sem_prophet_salva_no_formato_legado(tmp_path):
    """Comportamento padrão (sem prophet_model): dump direto do modelo, sem bundle."""
    model = MagicMock()
    model_path = str(tmp_path / "xgb_legado.pkl")

    with patch(f"{MODULO}.joblib.dump") as mock_dump:
        save_model_with_metadata(model, model_path, {})

    mock_dump.assert_called_once_with(model, model_path)

    meta_path = str(tmp_path / "xgb_legado_meta.json")
    with open(meta_path, encoding="utf-8") as f:
        salvo = json.load(f)

    assert salvo["artifact_format"] == "legacy"


def test_save_model_with_metadata_com_prophet_salva_bundle(tmp_path):
    """Quando prophet_model é informado, salva um dict {xgb_model, prophet_model}
    em um único artefato, e marca artifact_format='bundle' nos metadados."""
    xgb_model = MagicMock(name="xgb")
    prophet_model = MagicMock(name="prophet")
    model_path = str(tmp_path / "bundle.pkl")

    with patch(f"{MODULO}.joblib.dump") as mock_dump:
        save_model_with_metadata(xgb_model, model_path, {}, prophet_model=prophet_model)

    mock_dump.assert_called_once_with(
        {"xgb_model": xgb_model, "prophet_model": prophet_model}, model_path
    )

    meta_path = str(tmp_path / "bundle_meta.json")
    with open(meta_path, encoding="utf-8") as f:
        salvo = json.load(f)

    assert salvo["artifact_format"] == "bundle"
    assert "MagicMock" in salvo["model_type"]


# ============================================================
# carregar_modelo
# ============================================================
def test_carregar_modelo_bundle_retorna_xgb_e_prophet(tmp_path):
    import joblib

    xgb_model = {"tipo": "xgb-fake"}
    prophet_model = {"tipo": "prophet-fake"}
    model_path = str(tmp_path / "bundle.pkl")
    joblib.dump({"xgb_model": xgb_model, "prophet_model": prophet_model}, model_path)

    xgb_carregado, prophet_carregado = carregar_modelo(model_path)

    assert xgb_carregado == xgb_model
    assert prophet_carregado == prophet_model


def test_carregar_modelo_legado_retorna_prophet_none(tmp_path):
    import joblib

    model_path = str(tmp_path / "legado.pkl")
    joblib.dump({"apenas": "xgb"}, model_path)

    modelo_carregado, prophet_carregado = carregar_modelo(model_path)

    assert modelo_carregado == {"apenas": "xgb"}
    assert prophet_carregado is None


# ============================================================
# localizar_ultimo_modelo_bundle
# ============================================================
def _escrever_meta(tmp_path, nome_pkl, created_at, artifact_format="bundle", criar_pkl=True):
    if criar_pkl:
        (tmp_path / nome_pkl).write_bytes(b"conteudo-fake")
    meta = {
        "created_at": created_at,
        "model_file": nome_pkl,
        "artifact_format": artifact_format,
        "metrics": {"mae": 0.1, "rmse": 0.2},
        "extra": {"residual_bounds": {"min": -0.5, "max": 0.5}},
    }
    base = nome_pkl.rsplit(".", 1)[0]
    (tmp_path / f"{base}_meta.json").write_text(json.dumps(meta), encoding="utf-8")


def test_localizar_ultimo_modelo_bundle_escolhe_o_mais_recente(tmp_path):
    _escrever_meta(tmp_path, "bundle_antigo.pkl", "2026-01-01T00:00:00")
    _escrever_meta(tmp_path, "bundle_novo.pkl", "2026-06-01T00:00:00")

    model_path, meta = localizar_ultimo_modelo_bundle(str(tmp_path))

    assert model_path == str(tmp_path / "bundle_novo.pkl")
    assert meta["model_file"] == "bundle_novo.pkl"


def test_localizar_ultimo_modelo_bundle_ignora_formato_legado(tmp_path):
    _escrever_meta(tmp_path, "legado.pkl", "2026-06-01T00:00:00", artifact_format="legacy")

    model_path, meta = localizar_ultimo_modelo_bundle(str(tmp_path))

    assert model_path is None
    assert meta is None


def test_localizar_ultimo_modelo_bundle_ignora_meta_sem_pkl_correspondente(tmp_path):
    _escrever_meta(tmp_path, "orfao.pkl", "2026-06-01T00:00:00", criar_pkl=False)

    model_path, meta = localizar_ultimo_modelo_bundle(str(tmp_path))

    assert model_path is None
    assert meta is None


def test_localizar_ultimo_modelo_bundle_ignora_json_corrompido(tmp_path):
    (tmp_path / "corrompido_meta.json").write_text("{ nao é json", encoding="utf-8")

    model_path, meta = localizar_ultimo_modelo_bundle(str(tmp_path))

    assert model_path is None
    assert meta is None


def test_localizar_ultimo_modelo_bundle_diretorio_vazio(tmp_path):
    model_path, meta = localizar_ultimo_modelo_bundle(str(tmp_path))

    assert model_path is None
    assert meta is None


# ============================================================
# calcular_metricas
# ============================================================
def test_calcular_metricas_mae_rmse():
    y_true = [1.0, 2.0, 3.0, 4.0]
    y_pred = [1.5, 2.5, 2.5, 4.5]

    resultado = calcular_metricas(y_true, y_pred)

    assert set(resultado.keys()) == {"mae", "rmse"}
    assert resultado["mae"] == pytest.approx(0.5)
    assert resultado["rmse"] > 0


# ============================================================
# preparar_dados
# ============================================================
def _df_bruto(anos=range(2015, 2023)):
    anos = list(anos)
    return pd.DataFrame(
        {
            "ano": anos,
            "crimes_contra_mulher": [100 + i * 5 for i in range(len(anos))],
            "casos_feminicidios": [2 + (i % 3) for i in range(len(anos))],
        }
    )


def test_preparar_dados_gera_features_esperadas():
    df = _df_bruto()

    resultado = preparar_dados(df, "crimes_contra_mulher")

    for col in FEATURES:
        assert col in resultado.columns

    # dropna() remove as primeiras linhas sem lag/rolling suficiente
    assert len(resultado) < len(df)
    assert resultado["ano"].is_monotonic_increasing
    assert resultado["ano_num"].tolist() == sorted(resultado["ano_num"].tolist())


def test_preparar_dados_trata_divisao_por_zero_na_taxa_feminicidio():
    df = pd.DataFrame(
        {
            "ano": [2018, 2019, 2020, 2021],
            "crimes_contra_mulher": [0, 10, 20, 30],
            "casos_feminicidios": [1, 2, 3, 4],
        }
    )

    resultado = preparar_dados(df, "crimes_contra_mulher")

    # não deve haver inf/nan na coluna taxa_feminicidio após o tratamento
    assert not np.isinf(resultado["taxa_feminicidio"]).any()
    assert not resultado["taxa_feminicidio"].isna().any()


# ============================================================
# treinar_prophet
# ============================================================
def test_treinar_prophet_chama_fit_e_predict_com_colunas_ds_y():
    df = pd.DataFrame(
        {
            "ano": pd.to_datetime(["2020-01-01", "2021-01-01", "2022-01-01"]),
            "crimes_contra_mulher": [10, 20, 30],
        }
    )

    mock_prophet_instance = MagicMock()
    mock_prophet_instance.predict.return_value = pd.DataFrame({"yhat": [11, 21, 31]})

    with patch(f"{MODULO}.Prophet", return_value=mock_prophet_instance) as mock_prophet_cls:
        model, forecast = treinar_prophet(df, "crimes_contra_mulher")

    mock_prophet_cls.assert_called_once_with(yearly_seasonality=True)

    fit_args = mock_prophet_instance.fit.call_args[0][0]
    assert list(fit_args.columns) == ["ds", "y"]
    assert fit_args["y"].tolist() == [10, 20, 30]

    assert model is mock_prophet_instance
    assert forecast["yhat"].tolist() == [11, 21, 31]


# ============================================================
# treinar_residual
# ============================================================
def _df_preparado(n=10):
    """DataFrame já no formato pós preparar_dados, pronto para treinar_residual."""
    df = _df_bruto(range(2010, 2010 + n))
    return preparar_dados(df, "crimes_contra_mulher")


def test_treinar_residual_retorna_modelo_metricas_e_bounds():
    df = _df_preparado()

    prophet_model_fake = MagicMock()
    prophet_forecast_fake = pd.DataFrame({"yhat": np.linspace(100, 200, len(df))})

    with patch(
        f"{MODULO}.treinar_prophet",
        return_value=(prophet_model_fake, prophet_forecast_fake),
    ) as mock_treinar_prophet:
        model, prophet_model, metrics, rmin, rmax, params = treinar_residual(
            df.copy(), "crimes_contra_mulher"
        )

    mock_treinar_prophet.assert_called_once()
    assert prophet_model is prophet_model_fake
    assert set(metrics.keys()) == {"mae", "rmse"}
    assert rmin <= rmax
    assert params["n_estimators"] == 600
    assert params["random_state"] == 42
    # modelo XGBoost real e treinado -> deve conseguir prever
    preds = model.predict(df[FEATURES])
    assert len(preds) == len(df)


# ============================================================
# prever_futuro
# ============================================================
def _df_para_previsao():
    df = _df_preparado(n=8)
    return df.reset_index(drop=True)


def test_prever_futuro_gera_horizonte_solicitado_com_colunas_esperadas():
    df = _df_para_previsao()
    valor_col = "crimes_contra_mulher"

    model_fake = MagicMock()
    model_fake.predict.return_value = np.array([0.05])

    prophet_model_fake = MagicMock()
    prophet_model_fake.make_future_dataframe.return_value = pd.DataFrame(
        {"ds": pd.date_range("2020-01-01", periods=3, freq="YS")}
    )
    prophet_model_fake.predict.return_value = pd.DataFrame(
        {"yhat": [150.0, 155.0, 160.0]}
    )

    resultado = prever_futuro(
        model_fake,
        prophet_model_fake,
        df,
        valor_col,
        residual_min=-0.5,
        residual_max=0.5,
        anos=3,
    )

    assert len(resultado) == 3
    assert list(resultado.columns) == ["ano", "prophet", "residual_log", "final"]
    # todos os valores finais devem ser positivos (piso de 0.1 aplicado no código)
    assert (resultado["final"] > 0).all()
    # anos devem ser crescentes (um ano incrementado por rodada)
    anos = pd.to_datetime(resultado["ano"])
    assert anos.is_monotonic_increasing


def test_prever_futuro_aplica_clip_nos_limites_do_residual():
    df = _df_para_previsao()
    valor_col = "crimes_contra_mulher"

    # resíduo previsto muito acima do máximo permitido -> deve ser "clipado"
    model_fake = MagicMock()
    model_fake.predict.return_value = np.array([999.0])

    prophet_model_fake = MagicMock()
    prophet_model_fake.make_future_dataframe.return_value = pd.DataFrame(
        {"ds": pd.date_range("2020-01-01", periods=1, freq="YS")}
    )
    prophet_model_fake.predict.return_value = pd.DataFrame({"yhat": [100.0]})

    resultado = prever_futuro(
        model_fake,
        prophet_model_fake,
        df,
        valor_col,
        residual_min=-0.2,
        residual_max=0.2,
        anos=1,
    )

    # residual_log já vem multiplicado pelo decay (1.0 na primeira rodada),
    # então deve ficar exatamente no teto informado
    assert resultado.loc[0, "residual_log"] == pytest.approx(0.2)


def test_prever_futuro_suaviza_com_resultado_anterior_a_partir_da_segunda_rodada():
    df = _df_para_previsao()
    valor_col = "crimes_contra_mulher"

    model_fake = MagicMock()
    model_fake.predict.return_value = np.array([0.0])

    prophet_model_fake = MagicMock()
    prophet_model_fake.make_future_dataframe.return_value = pd.DataFrame(
        {"ds": pd.date_range("2020-01-01", periods=2, freq="YS")}
    )
    prophet_model_fake.predict.return_value = pd.DataFrame({"yhat": [100.0, 500.0]})

    resultado = prever_futuro(
        model_fake,
        prophet_model_fake,
        df,
        valor_col,
        residual_min=-1.0,
        residual_max=1.0,
        anos=2,
    )

    # a segunda previsão deve ter sido puxada pela suavização (0.7*novo + 0.3*anterior),
    # portanto menor do que seria sem suavização (salto direto para ~500)
    assert resultado.loc[1, "final"] < 500


# ============================================================
# executar_pipeline
# ============================================================
def test_executar_pipeline_orquestra_todas_as_etapas():
    df_bruto_fake = _df_bruto()
    df_preparado_fake = preparar_dados(df_bruto_fake.copy(), "crimes_contra_mulher")

    model_fake = MagicMock()
    prophet_model_fake = MagicMock()
    metrics_fake = {"mae": 0.1, "rmse": 0.2}
    forecast_fake = pd.DataFrame({"ano": [2023], "final": [123.0]})

    with (
        patch(f"{MODULO}.Repository.load", return_value=df_bruto_fake) as mock_load,
        patch(
            f"{MODULO}.treinar_residual",
            return_value=(model_fake, prophet_model_fake, metrics_fake, -0.5, 0.5, {"n_estimators": 600}),
        ) as mock_treinar,
        patch(f"{MODULO}.prever_futuro", return_value=forecast_fake) as mock_prever,
        patch(f"{MODULO}.save_model_with_metadata") as mock_save,
    ):
        resultado = executar_pipeline()

    mock_load.assert_called_once_with("violencia_contra_mulher_gold")
    mock_treinar.assert_called_once()
    mock_prever.assert_called_once()
    mock_save.assert_called_once()

    # o metadata passado para save_model_with_metadata deve refletir o treino
    args = mock_save.call_args[0]
    kwargs = mock_save.call_args[1]
    saved_model, saved_path, saved_metadata = args
    assert saved_model is model_fake
    assert saved_metadata["metrics"] == metrics_fake
    assert saved_metadata["dataset_info"]["source_table"] == "violencia_contra_mulher_gold"
    assert saved_metadata["dataset_info"]["target_column"] == "crimes_contra_mulher"
    # o Prophet correspondente deve ser passado junto, para persistir o bundle
    assert kwargs["prophet_model"] is prophet_model_fake

    assert resultado is forecast_fake


def test_modulo_executado_como_main_chama_executar_pipeline():
    """
    `runpy.run_module` reexecuta o arquivo inteiro do zero, então um patch em
    `analysis.data_analyzer.executar_pipeline` (função definida no PRÓPRIO
    módulo) não tem efeito sobre essa nova execução. É preciso mirar nas
    dependências externas que o pipeline usa por baixo (Repository, Prophet,
    joblib) — que continuam sendo os mesmos objetos já cacheados em
    `sys.modules`, mesmo com o módulo sendo reexecutado.
    """
    import runpy

    df_bruto_fake = _df_bruto()
    df_preparado_fake = preparar_dados(df_bruto_fake.copy(), "crimes_contra_mulher")
    n = len(df_preparado_fake)

    mock_prophet_instance = MagicMock()
    # 1ª chamada a .predict() acontece dentro de treinar_prophet (via
    # treinar_residual) e precisa casar em tamanho com df_preparado;
    # a 2ª acontece dentro de prever_futuro (horizonte padrão = 5 anos).
    mock_prophet_instance.predict.side_effect = [
        pd.DataFrame({"yhat": np.linspace(100, 200, n)}),
        pd.DataFrame({"yhat": np.linspace(100, 300, 5)}),
    ]
    mock_prophet_instance.make_future_dataframe.return_value = pd.DataFrame(
        {"ds": pd.date_range("2020-01-01", periods=5, freq="YS")}
    )

    with (
        patch(
            "ingestion.repository_adapter.Repository.load",
            return_value=df_bruto_fake,
        ),
        patch("prophet.Prophet", return_value=mock_prophet_instance),
        patch("joblib.dump"),
        patch("os.makedirs"),
        patch("builtins.open", MagicMock()),
        patch("json.dump"),
    ):
        runpy.run_module(MODULO, run_name="__main__")
