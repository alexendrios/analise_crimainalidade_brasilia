# tests/integracao/test_pipeline_tabela_gold_integracao.py
"""
Integração do pipeline gold com o PostgreSQL real: orquestração de
src.pipeline_tabela_gold persistindo as tabelas materializadas via
ingestion.repository_adapter.Repository.save (caminho de produção),
validação FULL REFRESH (reescrita) e o comportamento do step sem dados.

Os dados usados são fiéis aos schemas GOLD (validation/esquemas.py) via
tests.integracao.dados_gold, exercitando persistência com alta fidelidade.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from database.repository import repository as repo
from src.pipeline_tabela_gold import STEPS, criar_tabela_gold
from tests.integracao.dados_gold import df_da_gold

pytestmark = pytest.mark.integracao

MODULO = "src.pipeline_tabela_gold"


def _resultados(apenas_pulado=False):
    """
    Sintetiza o retorno de executar_pipeline() para todos os STEPS usando
    dados realistas por tabela gold (mesmas colunas dos schemas GOLD).
    """
    dados = {}
    for step in STEPS:
        if apenas_pulado and step.nome == STEPS[0].nome:
            dados[step.nome] = None
        else:
            dados[step.nome] = df_da_gold(step.output)
    return dados


def test_pipeline_persiste_todas_as_gold_no_banco_real():
    with patch(f"{MODULO}.executar_pipeline", return_value=_resultados()):
        criar_tabela_gold(max_workers=2)

    tabelas = set(repo.listar_tabelas())
    for step in STEPS:
        assert step.output in tabelas, f"Tabela {step.output} não materializada"

        df = repo.carregar_tabela(step.output)
        esperado = df_da_gold(step.output)
        assert df is not None
        assert len(df) == len(esperado)
        assert set(df.columns) >= set(esperado.columns)


def test_pipeline_reescreve_tabelas_em_full_refresh():
    with patch(f"{MODULO}.executar_pipeline", return_value=_resultados()):
        criar_tabela_gold(max_workers=2)

    com_uma_linha = {step.nome: df_da_gold(step.output).iloc[:1] for step in STEPS}
    with patch(f"{MODULO}.executar_pipeline", return_value=com_uma_linha):
        criar_tabela_gold(max_workers=2)

    for step in STEPS:
        df = repo.carregar_tabela(step.output)
        assert len(df) == 1
        esperado = df_da_gold(step.output).iloc[:1]
        pd.testing.assert_frame_equal(
            df[esperado.columns].reset_index(drop=True),
            esperado.reset_index(drop=True),
        )


def test_pipeline_computado_como_modulo_principal_da_api():
    """Garante que as tabelas do pipeline são analisáveis pela API/repositório."""
    with patch(f"{MODULO}.executar_pipeline", return_value=_resultados()):
        criar_tabela_gold(max_workers=2)

    for step in STEPS:
        resumo = repo.analisar_tabela(step.output)
        assert resumo["tabela"] == step.output
        assert resumo["linhas"] == len(df_da_gold(step.output))


def test_pipeline_persiste_colunas_fieis_ao_schema_gold():
    """Persistência fiel: colunas dos schemas GOLD preservadas no banco real."""
    with patch(f"{MODULO}.executar_pipeline", return_value=_resultados()):
        criar_tabela_gold(max_workers=2)

    for step in STEPS:
        df = repo.carregar_tabela(step.output)
        esperado = df_da_gold(step.output)

        for coluna in esperado.columns:
            assert coluna in df.columns, f"{step.output} perdeu coluna {coluna}"

        linha_guardada = df[esperado.columns].iloc[0]
        for coluna in esperado.select_dtypes(include="number").columns:
            assert linha_guardada[coluna] == esperado[coluna].iloc[0]


def test_pipeline_step_sem_dados_mantem_tabela_existente_intacta():
    # 1) estado inicial: todas as gold materializadas com dados realistas
    with patch(f"{MODULO}.executar_pipeline", return_value=_resultados()):
        criar_tabela_gold(max_workers=2)

    antes = {step.output: repo.carregar_tabela(step.output) for step in STEPS}

    # 2) reexecução onde o PRIMEIRO step não retorna dados
    with patch(
        f"{MODULO}.executar_pipeline",
        return_value=_resultados(apenas_pulado=True),
    ):
        criar_tabela_gold(max_workers=2)

    depois = {step.output: repo.carregar_tabela(step.output) for step in STEPS}

    # 3) o step pulado não foi reescrito: dados intactos no banco real
    pulado = STEPS[0].output
    pd.testing.assert_frame_equal(
        antes[pulado].sort_values("inserido_em").reset_index(drop=True),
        depois[pulado].sort_values("inserido_em").reset_index(drop=True),
    )

    # 4) os demais steps foram reescritos normalmente (mesmos dados, novo inserido_em)
    for step in STEPS[1:]:
        assert len(depois[step.output]) == len(df_da_gold(step.output))


def test_pipeline_reescrita_parcial_sobrescreve_apenas_steps_com_dados():
    # estado inicial: todas materializadas
    with patch(f"{MODULO}.executar_pipeline", return_value=_resultados()):
        criar_tabela_gold(max_workers=2)

    sem_dados = {
        step.nome: (
            None
            if step.nome in (STEPS[3].nome, STEPS[7].nome)
            else df_da_gold(step.output)
        )
        for step in STEPS
    }

    with patch(f"{MODULO}.executar_pipeline", return_value=sem_dados):
        criar_tabela_gold(max_workers=2)

    for step in STEPS:
        df = repo.carregar_tabela(step.output)
        if step.output in (STEPS[3].output, STEPS[7].output):
            # steps sem dados mantiveram a materialização anterior
            assert len(df) == len(df_da_gold(step.output))
        else:
            assert len(df) == len(df_da_gold(step.output))


def test_pipeline_max_workers_acima_do_numero_de_steps():
    with patch(f"{MODULO}.executar_pipeline", return_value=_resultados()):
        criar_tabela_gold(max_workers=len(STEPS) + 10)

    for step in STEPS:
        assert repo.carregar_tabela(step.output) is not None