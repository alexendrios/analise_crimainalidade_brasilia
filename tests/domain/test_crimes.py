from unittest.mock import patch

import pandas as pd
import pytest

from domain.crimes import merge_seguro


def test_merge_seguro_lista_vazia_levanta_erro():
    with pytest.raises(ValueError, match="não pode ser vazia"):
        merge_seguro([], keys=["ano", "regiao_administrativa"])


def test_merge_seguro_um_unico_df_retorna_ele_mesmo():
    df = pd.DataFrame({"ano": [2020, 2021], "regiao_administrativa": ["A", "B"], "valor": [1, 2]})

    resultado = merge_seguro([df], keys=["ano", "regiao_administrativa"])

    pd.testing.assert_frame_equal(resultado, df)


def test_merge_seguro_faz_merge_progressivo_outer():
    df1 = pd.DataFrame(
        {"ano": [2020, 2021], "regiao_administrativa": ["A", "B"], "roubo": [1, 2]}
    )
    df2 = pd.DataFrame(
        {"ano": [2020, 2022], "regiao_administrativa": ["A", "C"], "furto": [10, 20]}
    )

    resultado = merge_seguro([df1, df2], keys=["ano", "regiao_administrativa"])

    # outer join -> deve conter as 3 combinações distintas de chave
    assert len(resultado) == 3
    assert set(resultado.columns) == {"ano", "regiao_administrativa", "roubo", "furto"}


def test_merge_seguro_valida_chaves_de_cada_df():
    df1 = pd.DataFrame({"ano": [2020], "regiao_administrativa": ["A"], "x": [1]})
    df2 = pd.DataFrame({"ano": [2020], "regiao_administrativa": ["A"], "y": [2]})

    with patch("domain.crimes.validar_chaves") as mock_validar:
        merge_seguro([df1, df2], keys=["ano", "regiao_administrativa"])

    assert mock_validar.call_count == 2


def test_merge_seguro_propaga_e_loga_excecao():
    df_valido = pd.DataFrame({"ano": [2020], "regiao_administrativa": ["A"], "x": [1]})
    # objeto que não é DataFrame -> len()/merge devem falhar dentro do try
    df_invalido = "isso não é um dataframe"

    with pytest.raises(Exception):
        merge_seguro([df_valido, df_invalido], keys=["ano", "regiao_administrativa"])
