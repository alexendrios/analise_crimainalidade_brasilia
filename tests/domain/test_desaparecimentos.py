from unittest.mock import patch

import pandas as pd

from domain.desaparecimentos import DesaparecimentosService


@patch("domain.desaparecimentos.Repository.load")
def test_carregar_desaparecidos_idade_sexo_remove_inserido_em(mock_load):
    mock_load.return_value = pd.DataFrame(
        {"idade": [10], "sexo": ["M"], "inserido_em": ["2024-01-01"]}
    )

    resultado = DesaparecimentosService.carregar_desaparecidos_idade_sexo()

    mock_load.assert_called_once_with("desaparecidos_idade_sexo")
    assert "inserido_em" not in resultado.columns
    assert list(resultado.columns) == ["idade", "sexo"]


@patch("domain.desaparecimentos.Repository.load")
def test_carregar_desaparecidos_localizados(mock_load):
    mock_load.return_value = pd.DataFrame(
        {"ano": [2020], "localizados": [5], "inserido_em": ["2024-01-01"]}
    )

    resultado = DesaparecimentosService.carregar_desaparecidos_localizados()

    mock_load.assert_called_once_with("desaparecimento_localizados")
    assert "inserido_em" not in resultado.columns


@patch("domain.desaparecimentos.Repository.load")
def test_carregar_desaparecidos_regiao_remove_colunas_extras(mock_load):
    mock_load.return_value = pd.DataFrame(
        {
            "ano": [2020],
            "regiao_administrativa": ["CEILANDIA"],
            "inserido_em": ["2024-01-01"],
            "variacao_absoluta": [1],
            "participacao_percentual_2021": [0.5],
        }
    )

    resultado = DesaparecimentosService.carregar_desaparecidos_regiao()

    mock_load.assert_called_once_with("desaparecimento_regiao")
    assert set(resultado.columns) == {"ano", "regiao_administrativa"}


@patch("domain.desaparecimentos.Repository.load")
def test_load_and_clean_ignora_colunas_ausentes(mock_load):
    """errors='ignore' -> não deve quebrar se alguma coluna a remover não existir."""
    mock_load.return_value = pd.DataFrame({"ano": [2020], "regiao_administrativa": ["GAMA"]})

    resultado = DesaparecimentosService.carregar_desaparecidos_regiao()

    assert list(resultado.columns) == ["ano", "regiao_administrativa"]
