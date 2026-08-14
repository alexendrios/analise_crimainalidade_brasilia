import pandas as pd

from processing.post_processing import ordenar_padrao


def test_ordenar_padrao_ordena_por_ano_e_regiao():
    df = pd.DataFrame(
        {
            "ano": [2021, 2020, 2020],
            "regiao_administrativa": ["GAMA", "GAMA", "CEILANDIA"],
            "valor": [1, 2, 3],
        }
    )

    resultado = ordenar_padrao(df)

    assert resultado["ano"].tolist() == [2020, 2020, 2021]
    assert resultado.iloc[0]["regiao_administrativa"] == "CEILANDIA"
