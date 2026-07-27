from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.pipeline_tabela_gold import criar_tabela_gold, STEPS

MODULO = "src.pipeline_tabela_gold"


def _resultados_fake(valor_por_step=None):
    """
    Gera um dict {nome_do_step: dataframe} cobrindo todos os STEPS reais
    do pipeline gold, para simular o retorno de executar_pipeline().
    """
    valor_por_step = valor_por_step or {}
    return {
        step.nome: valor_por_step.get(step.nome, pd.DataFrame({"valor": [1, 2, 3]}))
        for step in STEPS
    }


def test_criar_tabela_gold_fluxo_completo_salva_todos_os_steps():
    resultados = _resultados_fake()

    with (
        patch(f"{MODULO}.executar_pipeline", return_value=resultados) as mock_exec,
        patch(f"{MODULO}.Repository.save") as mock_save,
    ):
        criar_tabela_gold(max_workers=3)

        # executar_pipeline deve ser chamado uma vez, com os STEPS reais
        mock_exec.assert_called_once()
        args, kwargs = mock_exec.call_args
        assert args[1] == STEPS
        assert kwargs["max_workers"] == 3

        # Repository.save deve ser chamado uma vez por step, com o output correto
        assert mock_save.call_count == len(STEPS)
        outputs_salvos = {call.args[1] for call in mock_save.call_args_list}
        assert outputs_salvos == {step.output for step in STEPS}


def test_criar_tabela_gold_pula_step_sem_dados():
    """Se um step não retorna dataframe (None), ele é pulado (sem Repository.save)."""
    resultados = _resultados_fake()
    step_sem_dados = STEPS[0].nome
    resultados[step_sem_dados] = None

    with (
        patch(f"{MODULO}.executar_pipeline", return_value=resultados),
        patch(f"{MODULO}.Repository.save") as mock_save,
        patch(f"{MODULO}.logger.warning") as mock_warning,
    ):
        criar_tabela_gold()

        # Um step a menos deve ter sido salvo
        assert mock_save.call_count == len(STEPS) - 1
        outputs_salvos = {call.args[1] for call in mock_save.call_args_list}
        assert STEPS[0].output not in outputs_salvos

        mock_warning.assert_called_once()


def test_criar_tabela_gold_erro_ao_salvar_nao_interrompe_os_demais():
    """Se Repository.save falha para um step, os outros ainda devem ser salvos."""
    resultados = _resultados_fake()

    def save_com_falha_no_primeiro(df, output):
        if output == STEPS[0].output:
            raise Exception("Erro de conexão simulado")
        return None

    with (
        patch(f"{MODULO}.executar_pipeline", return_value=resultados),
        patch(f"{MODULO}.Repository.save", side_effect=save_com_falha_no_primeiro) as mock_save,
        patch(f"{MODULO}.logger.error") as mock_error,
    ):
        # não deve levantar, o erro de um step é capturado individualmente
        criar_tabela_gold()

        assert mock_save.call_count == len(STEPS)
        mock_error.assert_called_once()
        assert "Erro de conexão simulado" in str(mock_error.call_args)


def test_criar_tabela_gold_erro_geral_relanca_excecao():
    """Se executar_pipeline falhar (erro geral), a exceção deve propagar."""
    with (
        patch(f"{MODULO}.executar_pipeline", side_effect=Exception("Falha geral")),
        patch(f"{MODULO}.Repository.save") as mock_save,
        patch(f"{MODULO}.logger.error") as mock_error,
    ):
        with pytest.raises(Exception, match="Falha geral"):
            criar_tabela_gold()

        mock_save.assert_not_called()
        mock_error.assert_called_once()


def test_criar_tabela_gold_dataframe_sem_len_usa_na():
    """
    Cobre o ramo `linhas = len(df) if hasattr(df, "__len__") else "N/A"`
    quando o resultado de um step não é um objeto com __len__.
    """
    resultados = _resultados_fake()
    resultados[STEPS[0].nome] = object()  # sem __len__

    with (
        patch(f"{MODULO}.executar_pipeline", return_value=resultados),
        patch(f"{MODULO}.Repository.save") as mock_save,
    ):
        criar_tabela_gold()

        # mesmo sem __len__, o step ainda deve ser salvo normalmente
        assert mock_save.call_count == len(STEPS)


def test_modulo_executado_como_main_chama_criar_tabela_gold():
    import runpy

    with (
        patch("src.core.executor.executar_pipeline", return_value={}) as mock_exec,
        patch("ingestion.repository_adapter.Repository.save") as mock_save,
    ):
        runpy.run_module(MODULO, run_name="__main__")

    # comprova que o bloco __main__ chamou criar_tabela_gold(max_workers=6),
    # que por sua vez invocou executar_pipeline com esse max_workers
    mock_exec.assert_called_once()
    _, kwargs = mock_exec.call_args
    assert kwargs["max_workers"] == 6
    mock_save.assert_not_called()  # resultados vazios -> nenhum step salvo
