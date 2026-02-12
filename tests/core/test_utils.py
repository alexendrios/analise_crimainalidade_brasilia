import pandas as pd
from src.tratamento_crimes import normalizar_texto, padronizando_colunas


def test_normalizar_texto_string_com_acento():
    assert normalizar_texto("Águas Claras ") == "AGUAS CLARAS"


def test_normalizar_texto_none():
    assert normalizar_texto(None) is None


def test_padronizando_colunas():
    df = pd.DataFrame(columns=[" Nome Coluna ", "Outra Coluna"])
    df = padronizando_colunas(df)

    assert list(df.columns) == ["nome_coluna", "outra_coluna"]
