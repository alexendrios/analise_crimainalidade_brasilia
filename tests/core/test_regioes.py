import pytest
from unittest.mock import mock_open, patch
from src.tratamento_crimes import (
    tratar_crimes_idosos_por_mes,
    tratar_injuria_racial_por_regiao,
    tratar_latrocinio_por_regiao,
    tratar_lesao_corporal_morte_por_regiao,
    tratar_lesao_corporal_morte,
    tratar_violencia_idosos,
    tratar_racismo,
    roubo_comercio,
    tratar_roubo_pedestre,
    tratar_roubo_veiculo,
    roubo_transporte_coletivo,
    _processar_tabela4,
    _processar_tabela5,
)

# Define a exceção esperada para cada função
EXCECAO_ESPERADA = {
    "tratar_crimes_idosos_por_mes": "tipo deve ser 'registro' ou 'fato'",
    "_processar_tabela5": "Tabela 5 não contém dados válidos",
    "_processar_tabela4": "Tabela 4 não contém dados válidos",
    "tratar_violencia_idosos": "Tabela 5 não encontrada no arquivo",
    "tratar_injuria_racial_por_regiao": "Header não encontrado no CSV",
    "tratar_latrocinio_por_regiao": "Header não encontrado no CSV",
    "tratar_lesao_corporal_morte_por_regiao": "Header não encontrado no CSV",
    "tratar_lesao_corporal_morte": "Header não encontrado no CSV",
    "tratar_racismo": "Header não encontrado no CSV",
    "tratar_roubo_pedestre": "Header não encontrado no CSV",
    "tratar_roubo_veiculo": "Header não encontrado no CSV",
    "roubo_comercio": "Header não encontrado no CSV",
    "roubo_transporte_coletivo": "Header não encontrado no CSV",
}


@pytest.mark.parametrize(
    "func",
    [
        tratar_crimes_idosos_por_mes,
        tratar_violencia_idosos,
        tratar_injuria_racial_por_regiao,
        tratar_latrocinio_por_regiao,
        tratar_lesao_corporal_morte_por_regiao,
        tratar_lesao_corporal_morte,
        tratar_racismo,
        tratar_roubo_pedestre,
        tratar_roubo_veiculo,
        roubo_comercio,
        roubo_transporte_coletivo,
        _processar_tabela4,
        _processar_tabela5,
    ],
)
def test_header_nao_encontrado_generico(func, df_sem_header):
    """
    Teste genérico para funções que falham quando header ou Tabela 5 não é encontrado.
    Usa mock do open e valida ValueError e log de erro.
    """
    excecao_esperada = EXCECAO_ESPERADA[func.__name__]

    # Mock do arquivo para não depender de disco
    m = mock_open(read_data="linha1\nlinha2\nlinha3")
    with patch("builtins.open", m):
        # Mock do logger para validar warnings/errors
        with (
            patch("src.tratamento_crimes.logger.error") as mock_logger_error,
            patch("src.tratamento_crimes.logger.warning") as mock_logger_warning,
        ):
            with pytest.raises(ValueError, match=excecao_esperada):
                try:
                    func("entrada.csv", "saida.csv")
                except TypeError:
                    # Caso a função só aceite 1 argumento
                    func("entrada.csv")

            # Garante que algum log de warning ou error foi chamado
            assert mock_logger_warning.call_count + mock_logger_error.call_count > 0
