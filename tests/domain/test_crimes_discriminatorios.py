from unittest.mock import patch

import pandas as pd

from domain.crimes_discriminatorios import CrimesDiscriminatoriosService


def _df_wide(valor_2020=5, valor_2021=8):
    return pd.DataFrame(
        {
            "regiao": ["CEILANDIA", "GAMA"],
            "inserido_em": ["2024-01-01", "2024-01-01"],
            "2020": [valor_2020, valor_2020 + 1],
            "2021": [valor_2021, valor_2021 + 1],
        }
    )


@patch("domain.crimes_discriminatorios.Repository.load")
def test_carregar_racismo(mock_load):
    mock_load.return_value = _df_wide()

    resultado = CrimesDiscriminatoriosService.carregar_racismo()

    mock_load.assert_called_once_with("racismo")
    assert "ocorrencia_racismo" in resultado.columns
    assert "regiao_administrativa" in resultado.columns
    assert "inserido_em" not in resultado.columns


@patch("domain.crimes_discriminatorios.Repository.load")
def test_carregar_injuria(mock_load):
    mock_load.return_value = _df_wide()

    resultado = CrimesDiscriminatoriosService.carregar_injuria()

    mock_load.assert_called_once_with("injuria_racial")
    assert "ocorrencia_injuria" in resultado.columns


@patch("domain.crimes_discriminatorios.Repository.load")
def test_consolidar_mescla_racismo_e_injuria(mock_load):
    mock_load.side_effect = [_df_wide(), _df_wide()]

    resultado = CrimesDiscriminatoriosService.consolidar()

    assert {"ano", "regiao_administrativa", "ocorrencia_racismo", "ocorrencia_injuria"} <= set(
        resultado.columns
    )
    # tipagem: todas as colunas numéricas (exceto as chaves) devem ser int
    for col in resultado.columns:
        if col not in ("ano", "regiao_administrativa"):
            assert pd.api.types.is_integer_dtype(resultado[col])
    # deve estar ordenado por ano, regiao_administrativa
    assert list(resultado["ano"]) == sorted(resultado["ano"])
