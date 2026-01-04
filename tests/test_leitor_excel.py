import os
import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import patch

from util.leitor_excel import (
    extrair_ano,
    normalizar_colunas,
    filtrar_distrito_federal,
    encontrar_coluna_populacao,
    limpar_populacao,
    listar_arquivos_populacao,
    processar_arquivo,
    consolidar_historico,
    salvar_historico_csv,
)


# =====================================================
# 🔢 extrair_ano
# =====================================================
def test_extrair_ano_pop_00():
    assert extrair_ano("POP-00-DOU.xls") == 2001


def test_extrair_ano_explicito():
    assert extrair_ano("populacao_2015.xls") == 2015
    assert extrair_ano("estimativa_1999.xls") == 1999


def test_extrair_ano_nao_encontrado_retorna_2010(caplog):
    ano = extrair_ano("arquivo_sem_ano.xls")
    assert ano == 2010
    assert "Ano não identificado" in caplog.text


# =====================================================
# 🧹 normalizar_colunas
# =====================================================
def test_normalizar_colunas():
    df = pd.DataFrame(columns=[" População Total ", "Ano-Base"])
    df2 = normalizar_colunas(df)

    assert list(df2.columns) == ["população_total", "ano_base"]


# =====================================================
# 📍 filtrar_distrito_federal
# =====================================================
def test_filtrar_df_por_nome():
    df = pd.DataFrame({"local": ["São Paulo", "Distrito Federal"], "valor": [1, 2]})
    res = filtrar_distrito_federal(df)

    assert len(res) == 1
    assert res.iloc[0]["valor"] == 2


def test_filtrar_df_por_sigla():
    df = pd.DataFrame({"uf": ["SP", "DF"], "valor": [1, 2]})
    res = filtrar_distrito_federal(df)

    assert len(res) == 1
    assert res.iloc[0]["valor"] == 2


def test_filtrar_df_nao_encontrado(caplog):
    df = pd.DataFrame({"cidade": ["São Paulo", "Rio"]})
    res = filtrar_distrito_federal(df)

    assert res.empty
    assert "Distrito Federal não encontrado" in caplog.text


# =====================================================
# 👥 encontrar_coluna_populacao
# =====================================================
def test_encontrar_coluna_populacao_por_nome():
    df = pd.DataFrame({"populacao_total": [1000]})
    assert encontrar_coluna_populacao(df) == "populacao_total"


def test_encontrar_coluna_populacao_por_valor():
    df = pd.DataFrame(
        {
            "col1": ["abc"],
            "col2": ["1.234.567"],
        }
    )
    assert encontrar_coluna_populacao(df) == "col2"


def test_encontrar_coluna_populacao_erro():
    df = pd.DataFrame({"a": [1], "b": [2]})
    with pytest.raises(ValueError):
        encontrar_coluna_populacao(df)


# =====================================================
# 🧽 limpar_populacao
# =====================================================
def test_limpar_populacao_valido():
    assert limpar_populacao("1.234.567") == 1234567


def test_limpar_populacao_none():
    assert limpar_populacao(None) is None


def test_limpar_populacao_vazio():
    assert limpar_populacao("") is None


# =====================================================
# 📂 listar_arquivos_populacao
# =====================================================
def test_listar_arquivos_populacao(tmp_path):
    arquivos_validos = [
        "pop_2010.xls",
        "populacao_2015.xlsx",
        "estimativa_2020.xls",
        "UF_Municipio.xls",
    ]
    arquivos_invalidos = ["teste.txt", "dados.csv"]

    for nome in arquivos_validos + arquivos_invalidos:
        (tmp_path / nome).touch()

    resultado = listar_arquivos_populacao(tmp_path)

    assert len(resultado) == len(arquivos_validos)
    for nome in arquivos_validos:
        assert any(nome in r for r in resultado)


# =====================================================
# 🧠 processar_arquivo
# =====================================================
def test_processar_arquivo_fluxo_completo(tmp_path):
    arquivo = tmp_path / "populacao_2015.xls"

    df_fake = pd.DataFrame(
        {
            "Localidade": ["Distrito Federal"],
            "População": ["2.500.000"],
        }
    )

    with patch("util.leitor_excel.pd.read_excel", return_value=df_fake):
        df = processar_arquivo(str(arquivo))

    assert not df.empty
    assert df.iloc[0]["ano"] == 2015
    assert df.iloc[0]["populacao"] == 2500000


def test_processar_arquivo_df_vazio(tmp_path):
    arquivo = tmp_path / "pop_2010.xls"
    df_fake = pd.DataFrame({"cidade": ["SP"]})

    with patch("util.leitor_excel.pd.read_excel", return_value=df_fake):
        df = processar_arquivo(str(arquivo))

    assert df.empty


# =====================================================
# 📊 consolidar_historico
# =====================================================
def test_consolidar_historico_sucesso(tmp_path):
    arquivos = ["a.xls", "b.xls"]

    df1 = pd.DataFrame(
        {
            "ano": [2010],
            "uf": ["DF"],
            "localidade": ["Distrito Federal"],
            "populacao": [100],
            "arquivo": ["a.xls"],
        }
    )
    df2 = pd.DataFrame(
        {
            "ano": [2011],
            "uf": ["DF"],
            "localidade": ["Distrito Federal"],
            "populacao": [200],
            "arquivo": ["b.xls"],
        }
    )

    with patch("util.leitor_excel.processar_arquivo", side_effect=[df1, df2]):
        df = consolidar_historico(arquivos)

    assert list(df["ano"]) == [2010, 2011]


def test_consolidar_historico_com_erro(caplog):
    with patch("util.leitor_excel.processar_arquivo", side_effect=Exception("erro")):
        df = consolidar_historico(["x.xls"])

    assert df.empty
    assert "Erro ao processar" in caplog.text


# =====================================================
# 💾 salvar_historico_csv
# =====================================================
def test_salvar_historico_csv_sucesso(tmp_path):
    df = pd.DataFrame(
        {
            "ano": [2020],
            "uf": ["DF"],
            "localidade": ["Distrito Federal"],
            "populacao": [300],
            "arquivo": ["teste.xls"],
        }
    )

    caminho = tmp_path / "saida" / "arquivo.csv"
    salvar_historico_csv(df, caminho)

    assert caminho.exists()


def test_salvar_historico_csv_df_vazio(caplog, tmp_path):
    df = pd.DataFrame()
    salvar_historico_csv(df, tmp_path / "x.csv")

    assert "DataFrame vazio" in caplog.text
