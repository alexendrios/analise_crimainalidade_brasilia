from unittest.mock import patch

import pandas as pd

from domain.violencia_idosos import ViolenciaIdososService


@patch("domain.violencia_idosos.Repository.load")
def test_carregar_resumo_seleciona_colunas_esperadas(mock_load):
    mock_load.return_value = pd.DataFrame(
        {
            "ranking": [1],
            "regiao_administrativa": ["CEILANDIA"],
            "jan_ago_2016": [10],
            "jan_ago_2017": [12],
            "outra_coluna": ["ignorar"],
        }
    )

    resultado = ViolenciaIdososService.carregar_resumo()

    mock_load.assert_called_once_with("violencia_idosos")
    assert list(resultado.columns) == [
        "ranking",
        "regiao_administrativa",
        "jan_ago_2016",
        "jan_ago_2017",
    ]


@patch("domain.violencia_idosos.Repository.load")
def test_carregar_mensal_remove_colunas(mock_load):
    mock_load.return_value = pd.DataFrame(
        {"ano": [2020], "inserido_em": ["x"], "subnotificacao": [1]}
    )

    resultado = ViolenciaIdososService.carregar_mensal()

    mock_load.assert_called_once_with("violencia_idosos_mensais")
    assert list(resultado.columns) == ["ano"]


@patch("domain.violencia_idosos.Repository.load")
def test_carregar_ocorrencias_remove_inserido_em(mock_load):
    mock_load.return_value = pd.DataFrame({"ano": [2020], "inserido_em": ["x"]})

    resultado = ViolenciaIdososService.carregar_ocorrencias()

    mock_load.assert_called_once_with("violencia_idosos_ocorrencias")
    assert list(resultado.columns) == ["ano"]


@patch("domain.violencia_idosos.Repository.load")
def test_carregar_sexo_remove_colunas(mock_load):
    mock_load.return_value = pd.DataFrame(
        {"ano": [2020], "inserido_em": ["x"], "total": [100]}
    )

    resultado = ViolenciaIdososService.carregar_sexo()

    mock_load.assert_called_once_with("violencia_idosos_sexo")
    assert list(resultado.columns) == ["ano"]
