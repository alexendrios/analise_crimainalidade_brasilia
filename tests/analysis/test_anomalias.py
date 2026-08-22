import numpy as np
import pandas as pd
import pytest

from analysis.anomalias import (
    detectar_anomalias,
    detectar_anomalias_painel,
    resumo_anomalias,
)


def _serie_mensal_com_pico() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    linhas = []
    for ano in (2016, 2017):
        for mes in range(1, 13):
            base = 12 + int(rng.integers(-2, 3))
            fato = 100 if (ano == 2017 and mes == 6) else base
            linhas.append({"ano": ano, "mes_num": mes, "fato": fato})
    return pd.DataFrame(linhas)


def test_detecta_o_pico_obvio_da_serie_mensal():
    marcado = detectar_anomalias(_serie_mensal_com_pico(), "fato")

    anomalias = marcado.query("anomalia")
    assert {"ano", "mes_num", "anomalia", "score"}.issubset(marcado.columns)
    pico = anomalias.query("ano == 2017 and mes_num == 6")
    assert len(pico) == 1
    assert marcado["score"].notna().sum() > 0


def test_linhas_iniciais_sem_historico_ficam_fora_da_avaliacao():
    serie = pd.DataFrame({"ano": range(2020, 2026), "valor": [5, 5, 5, 5, 50, 5]})

    marcado = detectar_anomalias(serie, "valor")

    primeira = marcado.iloc[0]
    assert primeira["anomalia"] is False or not primeira["anomalia"]
    assert pd.isna(primeira["score"])


def test_serie_curta_nao_treina_modelo_e_alerta():
    from unittest.mock import patch

    serie = pd.DataFrame({"ano": [2020, 2021, 2022], "valor": [3, 9, 30]})

    with patch("analysis.anomalias.logger.warning") as mock_warning:
        marcado = detectar_anomalias(serie, "valor")

    mock_warning.assert_called_once()
    assert not marcado["anomalia"].any()
    assert marcado["score"].isna().all()


def test_colunas_ausentes_levitam_valueerror():
    with pytest.raises(ValueError, match="ausente"):
        detectar_anomalias(pd.DataFrame({"ano": [2020]}), "fato")


def test_painel_por_ra_marca_anomalia_no_grupo_correto(dados_gold):
    painel = dados_gold["crimes_roubo_furto_gold"]

    # injeta um pico isolado em uma RA/ano específico
    alvo = painel.query("regiao_administrativa == 'Gama' and ano == 2021").index[0]
    painel.loc[alvo, "ocorrencia_roubo_pedestre"] += 500

    marcado = detectar_anomalias_painel(painel, "ocorrencia_roubo_pedestre")

    assert "regiao_administrativa" in marcado.columns
    anomalias_gama = marcado.query("anomalia and regiao_administrativa == 'Gama' and ano == 2021")
    assert len(anomalias_gama) == 1


def test_resumo_anomalias_ordena_do_mais_extremo():
    marcado = pd.DataFrame(
        {
            "regiao_administrativa": ["A", "B"],
            "ano": [2020, 2020],
            "valor": [10, 99],
            "anomalia": [True, True],
            "score": [-0.42, -0.61],
        }
    )

    resumo = resumo_anomalias(marcado, ("regiao_administrativa",))

    assert list(resumo["regiao_administrativa"]) == ["B", "A"]
    assert "score" not in resumo.columns


def test_painel_vazio_levanta_valueerror():
    with pytest.raises(ValueError, match="vazio"):
        detectar_anomalias_painel(
            pd.DataFrame(columns=["ano", "regiao_administrativa", "valor"]), "valor"
        )
