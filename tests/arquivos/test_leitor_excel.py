import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from util.leitor_excel import processar_dados_crimes, processar_populacao, processar_crimes

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
    listar_arquivos_por_padrao,
    listar_arquivos_crimes,
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
    with caplog.at_level("WARNING"):
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
@pytest.mark.parametrize(
    "valor, esperado",
    [
        ("1.234.567", 1234567),
        ("", None),
        (None, None),
    ],
)
def test_limpar_populacao(valor, esperado):
    assert limpar_populacao(valor) == esperado


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

def test_listar_arquivos_por_padrao_diretorio_inexistente(tmp_path):
    resultado = listar_arquivos_por_padrao(
        tmp_path / "nao_existe",
        padroes=("pop",),
    )

    assert resultado == []

def test_listar_arquivos_por_padrao_diretorio_vazio(tmp_path):
    resultado = listar_arquivos_por_padrao(
        tmp_path,
        padroes=("pop",),
    )

    assert resultado == []

def test_listar_arquivos_diretorio_inexistente(tmp_path):
    resultado = listar_arquivos_populacao(tmp_path / "nao_existe")
    assert resultado == []

def test_listar_arquivos_diretorio_vazio(tmp_path):
    resultado = listar_arquivos_populacao(tmp_path)
    assert resultado == []

def test_listar_arquivos_ignora_extensao_invalida(tmp_path):
    arquivo = tmp_path / "populacao.txt"
    arquivo.write_text("fake")

    resultado = listar_arquivos_populacao(tmp_path)
    assert resultado == []

def test_listar_arquivos_nome_sem_padrao(tmp_path):
    arquivo = tmp_path / "dados.xlsx"
    arquivo.write_text("fake")

    resultado = listar_arquivos_populacao(tmp_path)
    assert resultado == []

def test_listar_arquivos_crimes(tmp_path):
    arquivos = [
        "roubo_2020.xls",
        "homicidio_2019.xlsx",
        "crime_total.xls",
        "populacao.xls",  # inválido
    ]

    for nome in arquivos:
        (tmp_path / nome).touch()

    resultado = listar_arquivos_crimes(tmp_path)

    assert len(resultado) == 3
    assert all("populacao" not in r for r in resultado)

def test_listar_arquivos_ignora_subdiretorios(tmp_path):
    # cria subdiretório
    (tmp_path / "subdir").mkdir()

    # cria arquivo válido também (para garantir iteração mista)
    (tmp_path / "pop_2020.xls").touch()

    resultado = listar_arquivos_populacao(tmp_path)

    # só o arquivo deve aparecer
    assert len(resultado) == 1
    assert "pop_2020.xls" in resultado[0]


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

def test_processar_arquivo_erro_read_excel(tmp_path):
    arquivo = tmp_path / "pop_2010.xls"

    with patch("util.leitor_excel.pd.read_excel", side_effect=Exception("boom")):
        with pytest.raises(Exception):
            processar_arquivo(str(arquivo))


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

def test_salvar_historico_csv_log_sucesso(caplog, tmp_path):
    df = pd.DataFrame(
        {
            "ano": [2021],
            "uf": ["DF"],
            "localidade": ["Distrito Federal"],
            "populacao": [300],
            "arquivo": ["a.xls"],
        }
    )

    caminho = tmp_path / "out.csv"
    salvar_historico_csv(df, caminho)

    assert "Arquivo CSV salvo com sucesso" in caplog.text

def test_salvar_historico_csv_chama_logger_info(tmp_path):
    df = pd.DataFrame(
        {
            "ano": [2021],
            "uf": ["DF"],
            "localidade": ["Distrito Federal"],
            "populacao": [300],
            "arquivo": ["a.xls"],
        }
    )

    caminho = tmp_path / "out.csv"

    with patch("util.leitor_excel.logger.info") as mock_info:
        salvar_historico_csv(df, caminho)

    mock_info.assert_called_once()
# =====================================================
# 📂 listar_arquivos_crimes
# =====================================================
def test_listar_arquivos_crimes(tmp_path):
    arquivos_validos = [
        "roubo_2020.xls",
        "homicidio_2019.xlsx",
        "crime_total.xls",
    ]
    arquivos_invalidos = ["populacao.xls", "teste.txt"]

    for nome in arquivos_validos + arquivos_invalidos:
        (tmp_path / nome).touch()

    resultado = listar_arquivos_crimes(tmp_path)
    assert len(resultado) == len(arquivos_validos)
    for nome in arquivos_validos:
        assert any(nome in r for r in resultado)


# =====================================================
# 🧠 processar_dados_crimes
# =====================================================
def test_processar_dados_crimes_fluxo_completo(tmp_path):
    arquivo = tmp_path / "crimes_2020.xlsx"
    df_fake = pd.DataFrame({"crime": [1, 2]})

    mock_xls = MagicMock()
    mock_xls.sheet_names = ["Sheet1"]

    with patch("util.leitor_excel.pd.ExcelFile", return_value=mock_xls):
        with patch("util.leitor_excel.pd.read_excel", return_value=df_fake):
            df = processar_dados_crimes(str(arquivo))  # <-- corrigido

    assert not df.empty
    assert "arquivo" in df.columns
    assert len(df) == 2


def test_processar_dados_crimes_vazio(tmp_path, caplog):
    arquivo = tmp_path / "crimes_2020.xlsx"
    df_fake = pd.DataFrame()

    mock_xls = MagicMock()
    mock_xls.sheet_names = ["Sheet1"]

    with patch("util.leitor_excel.pd.ExcelFile", return_value=mock_xls):
        with patch("util.leitor_excel.pd.read_excel", return_value=df_fake):
            with caplog.at_level("WARNING"):
                df = processar_dados_crimes(str(arquivo))  # <-- corrigido

    assert df.empty
    assert "Arquivo de crimes sem dados válidos" in caplog.text


def test_processar_dados_crimes_erro(tmp_path):
    arquivo = tmp_path / "crimes_2020.xlsx"

    with patch("util.leitor_excel.pd.ExcelFile", side_effect=Exception("boom")):
        with pytest.raises(Exception):
            processar_dados_crimes(str(arquivo))


# =====================================================
# ✅ listar_arquivos_por_padrao com mocks Path
# =====================================================
def test_listar_arquivos_por_padrao_misto(tmp_path):
    (tmp_path / "pop_2010.xls").touch()
    (tmp_path / "dados.csv").touch()
    (tmp_path / "subdir").mkdir()

    resultado = listar_arquivos_por_padrao(tmp_path, padroes=("pop",))
    assert len(resultado) == 1
    assert "pop_2010.xls" in resultado[0]
def test_processar_populacao_fluxo_completo(tmp_path):
    # Mock das funções internas
    with patch(
        "util.leitor_excel.listar_arquivos_populacao", return_value=[tmp_path / "a.xls"]
    ):
        with patch(
            "util.leitor_excel.consolidar_historico",
            return_value=MagicMock(empty=False),
        ):
            with patch("util.leitor_excel.salvar_historico_csv") as mock_salvar:
                processar_populacao()
                mock_salvar.assert_called_once()


def test_processar_crimes_fluxo_completo(tmp_path):
    # Cria arquivos fake
    arquivo1 = tmp_path / "crime1.xls"
    arquivo2 = tmp_path / "~$temp_crime2.xls"  # deve ser ignorado
    arquivo1.write_text("fake")
    arquivo2.write_text("fake")

    caminho_saida = tmp_path / "saida"
    caminho_saida.mkdir()

    with patch(
        "util.leitor_excel.listar_arquivos_crimes", return_value=[arquivo1, arquivo2]
    ):
        with patch(
            "util.leitor_excel.processar_dados_crimes",
            return_value=MagicMock(empty=False),
        ) as mock_processar:
            with patch("util.leitor_excel.salvar_historico_csv") as mock_salvar:
                processar_crimes(tmp_path, caminho_saida)

                # Apenas arquivo1 deve ser processado
                mock_processar.assert_called_once_with(arquivo1)
                assert mock_salvar.call_count == 1