import numpy as np
import pandas as pd
import pytest

from util.padronizacao import (
    renomear_linha,
    recriar_regiao_com_valor,
    remover_acentos,
    normalizar_colunas,
    padronizar_regiao,
    transformar_wide_para_long,
    ordenar_por_ano,
    comparar_datasets,
    comparar_coluna_entre_datasets,
)


# ============================================================
# renomear_linha
# ============================================================
def test_renomear_linha_coluna_inexistente():
    df = pd.DataFrame({"regiao": ["A", "B"]})
    with pytest.raises(ValueError, match="não existe"):
        renomear_linha(df, "coluna_fantasma", "A", "A2")


def test_renomear_linha_registro_nao_encontrado_retorna_df_intacto():
    df = pd.DataFrame({"regiao": ["A", "B"]})
    resultado = renomear_linha(df, "regiao", "ZZZ", "NOVO")

    pd.testing.assert_frame_equal(resultado, df)


def test_renomear_linha_renomeia_registros_correspondentes():
    df = pd.DataFrame({"regiao": ["Brasilia", "Ceilandia", "Brasilia"]})
    resultado = renomear_linha(df, "regiao", "Brasilia", "Plano Piloto")

    assert list(resultado["regiao"]) == ["Plano Piloto", "Ceilandia", "Plano Piloto"]


# ============================================================
# recriar_regiao_com_valor
# ============================================================
@pytest.mark.parametrize(
    "coluna_ausente",
    ["região_administrativa", "ano", "crimes_contra_mulher"],
)
def test_recriar_regiao_coluna_ausente(coluna_ausente):
    df = pd.DataFrame(
        {
            "região_administrativa": ["Ceilandia"],
            "ano": [2020],
            "crimes_contra_mulher": [5],
        }
    ).drop(columns=[coluna_ausente])

    with pytest.raises(ValueError, match="não existe"):
        recriar_regiao_com_valor(df, "Ceilandia")


def test_recriar_regiao_recria_para_todos_os_anos_com_valor_padrao():
    df = pd.DataFrame(
        {
            "região_administrativa": ["Ceilandia", "Ceilandia", "Taguatinga"],
            "ano": [2020, 2021, 2020],
            "crimes_contra_mulher": [10, 12, 7],
        }
    )

    resultado = recriar_regiao_com_valor(df, "Ceilandia", valor_padrao=0)

    linhas_ceilandia = resultado[resultado["região_administrativa"] == "Ceilandia"]

    # Uma linha nova por ano único existente no dataset (2020 e 2021)
    assert len(linhas_ceilandia) == 2
    assert set(linhas_ceilandia["ano"]) == {2020, 2021}
    assert (linhas_ceilandia["crimes_contra_mulher"] == 0).all()

    # Taguatinga não deve ter sido alterada
    assert (
        resultado[resultado["região_administrativa"] == "Taguatinga"][
            "crimes_contra_mulher"
        ]
        == 7
    ).all()

    # Total de linhas = 2 (recriadas) + 1 (Taguatinga, que não é a região alvo)
    assert len(resultado) == 3


# ============================================================
# remover_acentos
# ============================================================
def test_remover_acentos_remove_acentuacao():
    assert remover_acentos("São Paulo") == "Sao Paulo"
    assert remover_acentos("Ceilândia") == "Ceilandia"


def test_remover_acentos_valor_nulo_retorna_o_proprio_valor():
    assert remover_acentos(None) is None
    assert pd.isna(remover_acentos(np.nan))


def test_remover_acentos_erro_em_tipo_invalido_propaga_excecao():
    with pytest.raises(TypeError):
        remover_acentos(12345)  # int não tem .encode/.normalize


# ============================================================
# normalizar_colunas
# ============================================================
def test_normalizar_colunas_padroniza_nomes():
    df = pd.DataFrame(
        columns=[
            "Região Administrativa",
            "Ano-Referência",
            "  Valor (R$)  ",
            "Já_Normalizado",
        ]
    )

    resultado = normalizar_colunas(df)

    assert list(resultado.columns) == [
        "regiao_administrativa",
        "ano_referencia",
        "valor_r",
        "ja_normalizado",
    ]


def test_normalizar_colunas_erro_propaga_excecao(monkeypatch):
    df = pd.DataFrame(columns=["a", "b"])

    def _quebra(_):
        raise RuntimeError("falha simulada")

    monkeypatch.setattr("util.padronizacao.remover_acentos", _quebra)

    with pytest.raises(RuntimeError, match="falha simulada"):
        normalizar_colunas(df)


# ============================================================
# padronizar_regiao
# ============================================================
def test_padronizar_regiao_coluna_inexistente():
    df = pd.DataFrame({"regiao": ["a"]})
    with pytest.raises(ValueError, match="não existe"):
        padronizar_regiao(df, "coluna_fantasma")


def test_padronizar_regiao_normaliza_maiuscula_sem_acento_sem_espacos():
    df = pd.DataFrame({"regiao": ["  ceilândia  ", "Águas Claras", None]})

    resultado = padronizar_regiao(df, "regiao")

    assert resultado["regiao"].tolist()[0] == "CEILANDIA"
    assert resultado["regiao"].tolist()[1] == "AGUAS CLARAS"
    assert pd.isna(resultado["regiao"].tolist()[2])


# ============================================================
# transformar_wide_para_long
# ============================================================
def test_transformar_wide_para_long_converte_e_remove_distrito_federal():
    df = pd.DataFrame(
        {
            "regiao": ["Ceilandia", "Distrito Federal", "Taguatinga"],
            "2020": [10, 999, 5],
            "2021": [12, 888, 7],
        }
    )

    resultado = transformar_wide_para_long(df, "regiao", "valor")

    # Distrito Federal (totalizador) deve ser removido
    assert "DISTRITO FEDERAL" not in resultado["regiao"].unique()

    # Restam Ceilandia e Taguatinga, para os 2 anos = 4 linhas
    assert len(resultado) == 4
    assert set(resultado["ano"].unique()) == {2020, 2021}
    assert resultado["valor"].dtype.name == "Int64"


def test_transformar_wide_para_long_valores_nao_numericos_viram_zero():
    df = pd.DataFrame(
        {
            "regiao": ["Ceilandia"],
            "2020": ["não informado"],
        }
    )

    resultado = transformar_wide_para_long(df, "regiao", "valor")

    assert resultado["valor"].iloc[0] == 0


# ============================================================
# ordenar_por_ano
# ============================================================
def test_ordenar_por_ano_coluna_inexistente():
    df = pd.DataFrame({"valor": [1, 2, 3]})
    with pytest.raises(ValueError, match="não existe"):
        ordenar_por_ano(df, coluna="ano")


def test_ordenar_por_ano_ordena_ascendente():
    df = pd.DataFrame({"ano": [2022, 2020, 2021], "valor": ["c", "a", "b"]})

    resultado = ordenar_por_ano(df)

    assert resultado["ano"].tolist() == [2020, 2021, 2022]
    assert resultado["valor"].tolist() == ["a", "b", "c"]
    # reset_index(drop=True) -> índice deve começar em 0 e ser sequencial
    assert resultado.index.tolist() == [0, 1, 2]


# ============================================================
# comparar_datasets
# ============================================================
def test_comparar_datasets_gera_resumo_correto():
    df1 = pd.DataFrame({"a": [1, None, 3], "b": [1, 2, 3]})
    df2 = pd.DataFrame({"a": [1, 2]})

    resultado = comparar_datasets({"df1": df1, "df2": df2}, exibir=False)

    linha_df1 = resultado[resultado["dataset"] == "df1"].iloc[0]
    linha_df2 = resultado[resultado["dataset"] == "df2"].iloc[0]

    assert linha_df1["linhas"] == 3
    assert linha_df1["colunas"] == 2
    assert linha_df1["nulos_total"] == 1
    assert linha_df1["colunas_com_nulos"] == 1

    assert linha_df2["linhas"] == 2
    assert linha_df2["colunas"] == 1
    assert linha_df2["nulos_total"] == 0


def test_comparar_datasets_com_exibir_true_loga_detalhes():
    """Cobre o branch `if exibir:` (logs adicionais de linhas/colunas/head)."""
    df1 = pd.DataFrame({"a": [1, 2]})
    resultado = comparar_datasets({"df1": df1}, exibir=True)
    assert len(resultado) == 1


def test_recriar_regiao_loga_warning_se_concat_gerar_tamanho_inconsistente(monkeypatch):
    """Cobre o log defensivo de inconsistência após o concat (linha 86)."""
    df = pd.DataFrame(
        {
            "região_administrativa": ["Ceilandia"],
            "ano": [2020],
            "crimes_contra_mulher": [10],
        }
    )

    concat_original = pd.concat

    def _concat_com_linha_extra(objs, **kwargs):
        resultado = concat_original(objs, **kwargs)
        # força uma linha a mais para simular inconsistência
        return concat_original([resultado, resultado.iloc[[0]]], **kwargs)

    monkeypatch.setattr("util.padronizacao.pd.concat", _concat_com_linha_extra)

    resultado = recriar_regiao_com_valor(df, "Ceilandia")
    # não valida o conteúdo (é um cenário forçado/artificial), só garante que
    # a função não quebra e retorna algo maior que o esperado normalmente
    assert len(resultado) >= 2


# ============================================================
# transformar_wide_para_long — caminho de exceção
# ============================================================
def test_transformar_wide_para_long_propaga_excecao_em_coluna_ausente():
    df = pd.DataFrame({"regiao": ["Ceilandia"], "2020": [10]})

    with pytest.raises(KeyError):
        transformar_wide_para_long(df, "coluna_que_nao_existe", "valor")


# ============================================================
# comparar_datasets — caminho de exceção
# ============================================================
def test_comparar_datasets_propaga_excecao_com_entrada_invalida():
    with pytest.raises(AttributeError):
        comparar_datasets({"invalido": None}, exibir=False)


# ============================================================
# comparar_coluna_entre_datasets
# ============================================================
def test_comparar_coluna_entre_datasets_coluna_ausente():
    datasets = {
        "df1": pd.DataFrame({"regiao": ["A", "B"]}),
        "df2": pd.DataFrame({"outra_coluna": [1, 2]}),
    }

    with pytest.raises(ValueError, match="não encontrada em df2"):
        comparar_coluna_entre_datasets(datasets, "regiao")


def test_comparar_coluna_entre_datasets_calcula_diferencas():
    datasets = {
        "df1": pd.DataFrame({"regiao": ["A", "B", "C"]}),
        "df2": pd.DataFrame({"regiao": ["B", "C", "D"]}),
    }

    resultado = comparar_coluna_entre_datasets(datasets, "regiao")

    chave = "df1 vs df2"
    assert resultado[chave]["somente_df1"] == {"A"}
    assert resultado[chave]["somente_df2"] == {"D"}
