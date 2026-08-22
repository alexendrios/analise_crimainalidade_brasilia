import numpy as np
import pandas as pd
import pytest

from geoespacial import BBOX_DF, agregar_por_celula, atribuir_celulas, gerar_malha
from geoespacial.centroides import CENTROIDES_RA
from geoespacial.malha import _delta_graus

# bbox sintético: 0.2 grau de lado (~22 km) com células de ~11 km
BBOX_SIMPLES = (-48.00, -16.00, -47.80, -15.80)
TAMANHO = 11.0


# ============================================================
# gerar_malha
# ============================================================
def test_malha_cobre_o_bbox_inteiro_com_ids_unicos():
    malha = gerar_malha(tamanho_celula_km=TAMANHO, bbox=BBOX_SIMPLES)

    delta_lon, delta_lat = _delta_graus(TAMANHO, (BBOX_SIMPLES[1] + BBOX_SIMPLES[3]) / 2)
    n_linhas = int(np.ceil((BBOX_SIMPLES[3] - BBOX_SIMPLES[1]) / delta_lat - 1e-9))
    n_colunas = int(np.ceil((BBOX_SIMPLES[2] - BBOX_SIMPLES[0]) / delta_lon - 1e-9))

    assert len(malha) == n_linhas * n_colunas
    assert malha["celula_id"].is_unique
    assert malha["lon_min"].min() == pytest.approx(BBOX_SIMPLES[0])
    assert malha["lat_min"].min() == pytest.approx(BBOX_SIMPLES[1])
    assert malha["lon_max"].max() == pytest.approx(BBOX_SIMPLES[2])
    assert malha["lat_max"].max() == pytest.approx(BBOX_SIMPLES[3])


def test_malha_padrao_usa_bbox_do_df_e_ids_unicos():
    malha = gerar_malha(tamanho_celula_km=5.0)

    assert malha["celula_id"].is_unique
    assert len(malha) > 100
    dentro = (
        (malha["centro_lon"] >= BBOX_DF[0]) & (malha["centro_lon"] <= BBOX_DF[2])
        & (malha["centro_lat"] >= BBOX_DF[1]) & (malha["centro_lat"] <= BBOX_DF[3])
    )
    assert dentro.all()


def test_malha_tamanho_invalido_levanta_valueerror():
    with pytest.raises(ValueError, match="positivo"):
        gerar_malha(tamanho_celula_km=0, bbox=BBOX_SIMPLES)


def test_malha_bbox_invertido_levanta_valueerror():
    with pytest.raises(ValueError, match="bbox inválido"):
        gerar_malha(tamanho_celula_km=1.0, bbox=(0, 1, -1, 0))


# ============================================================
# atribuir_celulas
# ============================================================
def test_ponto_central_cai_na_celula_correta():
    malha = gerar_malha(tamanho_celula_km=TAMANHO, bbox=BBOX_SIMPLES)
    alvo = malha.iloc[3]  # célula R001C001

    pontos = pd.DataFrame({"latitude": [alvo["centro_lat"]], "longitude": [alvo["centro_lon"]]})
    atribuido = atribuir_celulas(
        pontos, tamanho_celula_km=TAMANHO, bbox=BBOX_SIMPLES
    )

    assert atribuido["celula_id"].iloc[0] == alvo["celula_id"]


def test_ponto_fora_do_bbox_recebe_id_nulo():
    pontos = pd.DataFrame({"latitude": [-14.0], "longitude": [-46.0]})
    atribuido = atribuir_celulas(
        pontos, tamanho_celula_km=TAMANHO, bbox=BBOX_SIMPLES
    )

    assert atribuido["celula_id"].isna().all()


def test_atribuicao_vetorizada_consistente_com_a_malha():
    rng = np.random.default_rng(42)
    n = 500
    pontos = pd.DataFrame(
        {
            "latitude": rng.uniform(BBOX_SIMPLES[1], BBOX_SIMPLES[3], n),
            "longitude": rng.uniform(BBOX_SIMPLES[0], BBOX_SIMPLES[2], n),
        }
    )
    malha = gerar_malha(tamanho_celula_km=TAMANHO, bbox=BBOX_SIMPLES)
    atribuido = atribuir_celulas(pontos, tamanho_celula_km=TAMANHO, bbox=BBOX_SIMPLES)

    ids_validos = set(malha["celula_id"])
    atribuidos = set(atribuido["celula_id"].dropna())
    assert atribuidos.issubset(ids_validos)
    assert not atribuido["celula_id"].isna().any()


def test_colunas_ausentes_levitam_valueerror():
    with pytest.raises(ValueError, match="latitude"):
        atribuir_celulas(pd.DataFrame({"longitude": [-47.9]}), bbox=BBOX_SIMPLES)


# ============================================================
# agregar_por_celula
# ============================================================
def test_agrega_indicador_na_celula_do_centroide_da_ra():
    df_gold = pd.DataFrame(
        {
            "regiao_administrativa": ["Brasília", "BRASILIA", "Gama"],
            "ocorrencias": [10, 5, 7],
        }
    )
    agregado = agregar_por_celula(
        df_gold, "ocorrencias", tamanho_celula_km=TAMANHO, bbox=BBOX_DF
    )

    # Brasília soma 10+5 na célula do seu centróide; Gama cai em outra célula
    assert agregado["ocorrencias"].sum() == pytest.approx(22.0)
    assert sorted(agregado["ocorrencias"], reverse=True) == [15.0, 7.0]


def test_ra_sem_centroide_e_ignorada_com_aviso():
    from unittest.mock import patch

    df_gold = pd.DataFrame(
        {"regiao_administrativa": ["RA FANTASMA"], "ocorrencias": [99]}
    )
    with patch("geoespacial.malha.logger.warning") as mock_warning:
        agregado = agregar_por_celula(
            df_gold, "ocorrencias", tamanho_celula_km=TAMANHO, bbox=BBOX_DF
        )

    extras = [c.kwargs.get("extra") or {} for c in mock_warning.call_args_list]
    assert {"ra": "RA FANTASMA"} in extras
    assert agregado.empty


def test_agregar_exige_colunas():
    with pytest.raises(ValueError, match="exige as colunas"):
        agregar_por_celula(pd.DataFrame({"x": [1]}), "ocorrencias", bbox=BBOX_SIMPLES)


def test_centroides_ficam_dentro_do_bbox_do_df():
    for ra, (lat, lon) in CENTROIDES_RA.items():
        assert BBOX_DF[1] <= lat <= BBOX_DF[3], ra
        assert BBOX_DF[0] <= lon <= BBOX_DF[2], ra
