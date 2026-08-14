import pandas as pd
import pytest

from processing.transform import processar_dataset_base


def _df_wide(coluna_regiao="regiao_administrativa"):
    return pd.DataFrame(
        {
            coluna_regiao: ["CEILANDIA", "ANALISE DE 2024", "GAMA"],
            "inserido_em": ["2024-01-01"] * 3,
            "2020": [1, 99, 2],
            "2021": [3, 98, 4],
        }
    )


def test_processar_dataset_base_fluxo_completo_sem_filtro():
    df = _df_wide()

    resultado = processar_dataset_base(
        df=df,
        coluna_regiao="regiao_administrativa",
        nome_valor="ocorrencia_teste",
        drop=["inserido_em"],
    )

    assert "inserido_em" not in resultado.columns
    assert "ocorrencia_teste" in resultado.columns
    # sem filtro, a linha "ANALISE DE 2024" permanece
    assert "ANALISE DE 2024" in set(resultado["regiao_administrativa"])


def test_processar_dataset_base_aplica_filtro_removendo_linha():
    df = _df_wide()

    resultado = processar_dataset_base(
        df=df,
        coluna_regiao="regiao_administrativa",
        nome_valor="ocorrencia_teste",
        drop=["inserido_em"],
        filtro="analise de 2024",
    )

    assert "ANALISE DE 2024" not in set(resultado["regiao_administrativa"])
    assert {"CEILANDIA", "GAMA"} <= set(resultado["regiao_administrativa"])


def test_processar_dataset_base_avisa_quando_coluna_para_drop_nao_existe():
    df = _df_wide()

    # "coluna_fantasma" não existe -> deve apenas logar warning e seguir
    resultado = processar_dataset_base(
        df=df,
        coluna_regiao="regiao_administrativa",
        nome_valor="ocorrencia_teste",
        drop=["inserido_em", "coluna_fantasma"],
    )

    assert "ocorrencia_teste" in resultado.columns


def test_processar_dataset_base_propaga_excecao():
    df = pd.DataFrame({"outra_coluna": [1, 2, 3]})

    with pytest.raises(Exception):
        # coluna_regiao inexistente -> padronizar_regiao levanta ValueError
        processar_dataset_base(
            df=df,
            coluna_regiao="regiao_administrativa",
            nome_valor="ocorrencia_teste",
        )
