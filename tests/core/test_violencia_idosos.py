from src.tratamento_crimes import tratar_violencia_idosos
from unittest.mock import patch
import pytest


@pytest.mark.parametrize("saida", ["arquivo.csv", 123, None])
def test_tratar_violencia_idosos_saida_invalida(tmp_path, saida):
    arquivo_entrada = tmp_path / "entrada.txt"

    conteudo = """Tabela 4:
ANO;OCORRENCIAS;DENTRO
2016;10;5
Tabela 5:
ANO;M;F;TOTAL
2016;3;7;10
"""

    arquivo_entrada.write_text(conteudo, encoding="latin1")

    with pytest.raises(ValueError):
        tratar_violencia_idosos(str(arquivo_entrada), saida)


@pytest.mark.parametrize(
    "linhas_arquivo, excecao_esperada, mensagem_logger",
    [
        # Caso Tabela 4 sem header "ANO" → inclui Tabela 5 mínima para passar essa validação
        (
            [
                "Tabela 4:",
                "2016;10;5",
                "2017;20;10",
                "Tabela 5:",
                "ANO;M;F;TOTAL",
                "2016;3;7;10",
            ],
            "Tabela 4 sem header válido",
            "Header ANO não encontrado na Tabela 4",
        ),
        # Caso Tabela 5 ausente
        (
            [
                "Tabela 4:",
                "ANO;OCORRENCIAS;DENTRO",
                "2016;10;5",
                "2017;20;10",
            ],
            "Tabela 5 não encontrada no arquivo",
            "Não foi possível localizar Tabela 5 no arquivo",
        ),
    ],
)
def test_tabela4_e_tabela5_sem_header(mock_csv, linhas_arquivo, excecao_esperada, mensagem_logger):
    with mock_csv(linhas_arquivo):
        with patch("src.tratamento_crimes.logger.warning") as mock_logger_warning:
            with pytest.raises(ValueError, match=excecao_esperada):
                tratar_violencia_idosos("entrada.csv", ["saida_t4.csv", "saida_t5.csv"])
            mock_logger_warning.assert_called_once_with(mensagem_logger)


def test_tratar_violencia_idosos_fluxo_completo(tmp_path, mock_csv):
    linhas = [
        "Tabela 4:",
        "ANO;OCORRENCIAS;DENTRO",
        "2020;100;30",
        "2021;110;35",
        "Tabela 5:",
        "ANO;M;F;TOTAL",
        "",
        "2020;40;60;100",
        "2021;45;65;110",
    ]

    saida_t4 = tmp_path / "saida_t4.csv"
    saida_t5 = tmp_path / "saida_t5.csv"

    with mock_csv(linhas):
        df_t4, df_t5 = tratar_violencia_idosos("entrada.csv", [str(saida_t4), str(saida_t5)])

    assert list(df_t4.columns) == ["ano", "ocorrencias", "violencia_dentro_de_casa"]
    assert df_t4["ano"].tolist() == [2020, 2021]
    assert list(df_t5.columns) == ["ano", "masculino", "feminino", "total"]
    assert df_t5["total"].tolist() == [100, 110]


def test_tabela5_sem_header_levanta_erro(mock_csv):
    linhas = [
        "Tabela 4:",
        "ANO;OCORRENCIAS;DENTRO",
        "2020;100;30",
        "Tabela 5:",
        "2020;40;60;100",  # sem linha "ANO;..." de header
    ]

    with mock_csv(linhas):
        with pytest.raises(ValueError, match="Tabela 5 sem header válido"):
            tratar_violencia_idosos("entrada.csv", ["saida_t4.csv", "saida_t5.csv"])


def test_tabela5_numero_de_colunas_inesperado_levanta_erro(mock_csv):
    linhas = [
        "Tabela 4:",
        "ANO;OCORRENCIAS;DENTRO",
        "2020;100;30",
        "Tabela 5:",
        "ANO;M;F",  # só 3 colunas, esperado 4
        "2020;40;60",
    ]

    with mock_csv(linhas):
        with pytest.raises(ValueError, match="número inesperado de colunas"):
            tratar_violencia_idosos("entrada.csv", ["saida_t4.csv", "saida_t5.csv"])


def test_tabela5_valores_nao_numericos_levanta_erro(mock_csv):
    linhas = [
        "Tabela 4:",
        "ANO;OCORRENCIAS;DENTRO",
        "2020;100;30",
        "Tabela 5:",
        "ANO;M;F;TOTAL",
        "2020;abc;60;100",  # valor não numérico
    ]

    with mock_csv(linhas):
        with pytest.raises(ValueError, match="valores não numéricos"):
            tratar_violencia_idosos("entrada.csv", ["saida_t4.csv", "saida_t5.csv"])