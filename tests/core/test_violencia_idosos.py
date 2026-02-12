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