from unittest.mock import patch

import pandas as pd
import pytest

from src.core.pipeline_step import PipelineStep
from src.core.executor import executar_com_retry, executar_pipeline

MODULO = "src.core.executor"


# ============================================================
# executar_com_retry
# ============================================================
def test_executar_com_retry_sucesso_primeira_tentativa():
    df_esperado = pd.DataFrame({"a": [1, 2, 3]})
    step = PipelineStep(nome="passo1", func=lambda: df_esperado, retries=2, timeout=5)

    nome, resultado = executar_com_retry("run-1", step)

    assert nome == "passo1"
    pd.testing.assert_frame_equal(resultado, df_esperado)


def test_executar_com_retry_resultado_sem_len_nao_quebra():
    """Cobre o ramo `linhas = ... else 'N/A'` para retorno sem __len__."""
    step = PipelineStep(nome="passo_sem_len", func=lambda: object(), retries=1, timeout=5)

    nome, resultado = executar_com_retry("run-1", step)

    assert nome == "passo_sem_len"
    assert isinstance(resultado, object)


def test_executar_com_retry_sucesso_apos_falha_intermitente():
    """Falha na 1ª tentativa, sucesso na 2ª — cobre o ramo de retry após Exception."""
    chamadas = {"n": 0}

    def func_intermitente():
        chamadas["n"] += 1
        if chamadas["n"] == 1:
            raise ValueError("falha temporária")
        return pd.DataFrame({"a": [1]})

    step = PipelineStep(nome="passo_intermitente", func=func_intermitente, retries=2, timeout=5)

    with patch(f"{MODULO}.logger.warning") as mock_warning:
        nome, resultado = executar_com_retry("run-1", step)

    assert nome == "passo_intermitente"
    assert chamadas["n"] == 2
    assert len(resultado) == 1
    mock_warning.assert_called_once()  # logou o retry


def test_executar_com_retry_falha_definitiva_apos_esgotar_tentativas():
    def func_sempre_falha():
        raise RuntimeError("erro permanente")

    step = PipelineStep(nome="passo_quebrado", func=func_sempre_falha, retries=2, timeout=5)

    with patch(f"{MODULO}.logger.error") as mock_error:
        nome, resultado = executar_com_retry("run-1", step)

    assert nome == "passo_quebrado"
    assert resultado is None
    # 1 log de erro por tentativa (3 tentativas: inicial + 2 retries) + 1 log de falha definitiva
    assert mock_error.call_count == 4


def test_executar_com_retry_timeout_esgota_tentativas():
    import time as time_module

    def func_lenta():
        time_module.sleep(0.3)
        return pd.DataFrame({"a": [1]})

    step = PipelineStep(nome="passo_lento", func=func_lenta, retries=1, timeout=0.05)

    nome, resultado = executar_com_retry("run-1", step)

    assert nome == "passo_lento"
    assert resultado is None


# ============================================================
# executar_pipeline
# ============================================================
def test_executar_pipeline_executa_todos_os_steps_em_paralelo():
    steps = [
        PipelineStep(nome="a", func=lambda: pd.DataFrame({"x": [1]}), retries=0, timeout=5),
        PipelineStep(nome="b", func=lambda: pd.DataFrame({"x": [1, 2]}), retries=0, timeout=5),
        PipelineStep(nome="c", func=lambda: pd.DataFrame({"x": [1, 2, 3]}), retries=0, timeout=5),
    ]

    resultados = executar_pipeline("run-2", steps, max_workers=2)

    assert set(resultados.keys()) == {"a", "b", "c"}
    assert len(resultados["a"]) == 1
    assert len(resultados["b"]) == 2
    assert len(resultados["c"]) == 3


def test_executar_pipeline_isola_falha_de_um_step_dos_demais():
    def func_com_erro():
        raise Exception("step quebrado")

    steps = [
        PipelineStep(nome="ok", func=lambda: pd.DataFrame({"x": [1]}), retries=0, timeout=5),
        PipelineStep(nome="quebrado", func=func_com_erro, retries=0, timeout=5),
    ]

    resultados = executar_pipeline("run-3", steps, max_workers=2)

    assert resultados["quebrado"] is None
    assert len(resultados["ok"]) == 1
