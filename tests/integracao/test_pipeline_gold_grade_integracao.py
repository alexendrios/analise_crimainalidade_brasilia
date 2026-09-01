# tests/integracao/test_pipeline_gold_grade_integracao.py
"""
Matriz de integração do pipeline gold com o PostgreSQL real, parametrizada
pelas 12 tabelas gold: persistência fiel, FULL REFRESH (reescrita) e
análise posterior via repositório. O pipeline roda pelo caminho de
produção (src.pipeline_tabela_gold.criar_tabela_gold) apenas trocando o
executor pelos dados fiéis dos schemas GOLD.
"""

from unittest.mock import patch

import pytest

from api.config import TABELAS_GOLD
from database.repository import repository as repo
from tests.integracao.dados_gold import df_da_gold

pytestmark = pytest.mark.integracao

TABELAS = list(TABELAS_GOLD)
MODULO = "src.pipeline_tabela_gold"


def _executar_pipeline():
    from src.pipeline_tabela_gold import STEPS

    return {step.nome: df_da_gold(step.output) for step in STEPS}


@pytest.fixture(scope="module")
def gold_materializada(banco_env):
    from src.pipeline_tabela_gold import criar_tabela_gold

    with patch(f"{MODULO}.executar_pipeline", return_value=_executar_pipeline()):
        criar_tabela_gold(max_workers=2)


@pytest.fixture(scope="module")
def gold_reescrita(gold_materializada):
    from src.pipeline_tabela_gold import STEPS, criar_tabela_gold

    com_uma_linha = {step.nome: df_da_gold(step.output).iloc[:1] for step in STEPS}
    with patch(f"{MODULO}.executar_pipeline", return_value=com_uma_linha):
        criar_tabela_gold(max_workers=2)


@pytest.mark.parametrize("tabela", TABELAS)
def test_pipeline_persistencia_por_tabela(engine, gold_materializada, tabela):
    df = repo.carregar_tabela(tabela)
    esperado = df_da_gold(tabela)

    assert df is not None
    assert len(df) == len(esperado)
    assert set(df.columns) >= set(esperado.columns)

    for coluna in esperado.select_dtypes(include="number").columns:
        assert df[coluna].iloc[0] == esperado[coluna].iloc[0]


@pytest.mark.parametrize("tabela", TABELAS)
def test_pipeline_reescrita_full_refresh_por_tabela(engine, gold_reescrita, tabela):
    df = repo.carregar_tabela(tabela)

    assert len(df) == 1
    esperado = df_da_gold(tabela).iloc[:1]
    for coluna in esperado.columns:
        assert df[coluna].iloc[0] == esperado[coluna].iloc[0]


@pytest.mark.parametrize("tabela", TABELAS)
def test_pipeline_resumo_apos_reescrita_por_tabela(engine, gold_reescrita, tabela):
    resumo = repo.analisar_tabela(tabela)

    assert resumo["tabela"] == tabela
    assert resumo["linhas"] == 1