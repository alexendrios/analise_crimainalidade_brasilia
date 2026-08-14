from unittest.mock import patch

import pandas as pd

from domain.crimes_letais import CrimesLetaisService


def _df_wide():
    return pd.DataFrame(
        {
            "regiao": ["CEILANDIA", "GAMA"],
            "inserido_em": ["2024-01-01", "2024-01-01"],
            "2020": [1, 2],
            "2021": [3, 4],
        }
    )


def _df_homicidio_long():
    return pd.DataFrame(
        {
            "regiao_administrativa": ["CEILANDIA", "GAMA"],
            "inserido_em": ["2024-01-01", "2024-01-01"],
            "2020": [5, 6],
            "2021": [7, 8],
        }
    )


@patch("domain.crimes_letais.Repository.load")
def test_carregar_homicidio(mock_load):
    mock_load.return_value = _df_homicidio_long()

    resultado = CrimesLetaisService.carregar_homicidio()

    mock_load.assert_called_once_with("homicidio")
    assert "ocorrencia_homicidio" in resultado.columns


@patch("domain.crimes_letais.Repository.load")
def test_carregar_latrocinio_recria_unidades_prisionais(mock_load):
    mock_load.return_value = _df_wide()

    resultado = CrimesLetaisService.carregar_latrocinio()

    mock_load.assert_called_once_with("latrocinio")
    assert "ocorrencia_latrocinio" in resultado.columns
    assert "UNIDADES PRISIONAIS" in set(resultado["regiao_administrativa"])


@patch("domain.crimes_letais.Repository.load")
def test_carregar_lesao_morte(mock_load):
    mock_load.return_value = _df_wide()

    resultado = CrimesLetaisService.carregar_lesao_morte()

    mock_load.assert_called_once_with("lesao_corporal_morte")
    assert "ocorrencia_lesao_morte" in resultado.columns


@patch("domain.crimes_letais.Repository.load")
def test_consolidar_mescla_homicidio_latrocinio_e_lesao(mock_load):
    mock_load.side_effect = [_df_homicidio_long(), _df_wide(), _df_wide()]

    resultado = CrimesLetaisService.consolidar()

    esperado = {
        "ano",
        "regiao_administrativa",
        "ocorrencia_homicidio",
        "ocorrencia_latrocinio",
        "ocorrencia_lesao_morte",
    }
    assert esperado <= set(resultado.columns)
    assert not resultado.isna().any().any()  # fillna(0) aplicado
