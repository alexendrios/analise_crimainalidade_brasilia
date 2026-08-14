from unittest.mock import patch

import pandas as pd

from domain.crimes_patrimoniais import CrimesPatrimoniaisService


def _df_wide():
    return pd.DataFrame(
        {
            "Região Administrativa": ["CEILANDIA", "GAMA"],
            "inserido_em": ["2024-01-01", "2024-01-01"],
            "2020": [1, 2],
            "2021": [3, 4],
        }
    )


@patch("ingestion.repository_adapter.Repository.load")
def test_consolidar_mescla_os_cinco_datasets_do_config(mock_load):
    # DATASETS_CONFIG tem 5 entradas -> Repository.load é chamado 5 vezes
    mock_load.side_effect = [_df_wide() for _ in range(5)]

    resultado = CrimesPatrimoniaisService.consolidar()

    esperado = {
        "ano",
        "regiao_administrativa",
        "ocorrencia_roubo_pedestre",
        "ocorrencia_roubo_comercio",
        "ocorrencia_roubo_transporte_coletivo",
        "ocorrencia_roubo_veiculo",
        "ocorrencia_furto_em_veiculo",
    }
    assert esperado <= set(resultado.columns)
    assert mock_load.call_count == 5
    assert not resultado.isna().any().any()  # fillna(0) aplicado no final
