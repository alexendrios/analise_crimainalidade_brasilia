from unittest.mock import patch

import pandas as pd
import pytest

from src.core.pipeline_step import PipelineStep
from src.core.executor import _executar_passo, executar_com_retry, executar_pipeline

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


# ============================================================
# Hook de validação (data quality)
# ============================================================
def test_validacao_eh_chamada_com_o_resultado_do_step():
    recebido = {}

    def validacao(resultado):
        recebido["resultado"] = resultado

    df = pd.DataFrame({"x": [1]})
    step = PipelineStep(
        nome="validado", func=lambda: df, retries=0, timeout=5, validacao=validacao
    )

    nome, resultado, sucesso = _executar_passo("run-4", step)

    assert sucesso
    pd.testing.assert_frame_equal(recebido["resultado"], df)


def test_validacao_falha_consoma_as_tentativas_e_marca_como_falha():
    from unittest.mock import patch

    chamadas = {"n": 0}

    def func():
        chamadas["n"] += 1
        return pd.DataFrame({"x": [1]})

    def validacao(_resultado):
        raise ValueError("schema inválido")

    step = PipelineStep(
        nome="invalido", func=func, retries=1, timeout=5, validacao=validacao
    )

    with patch(f"{MODULO}.logger.error"):
        nome, resultado, sucesso = _executar_passo("run-5", step)

    assert sucesso is False
    assert resultado is None
    assert chamadas["n"] == 2  # tentativa inicial + retry


def test_validacao_nao_rodando_para_retorno_none():
    chamadas = {"n": 0}

    def validacao(_resultado):
        chamadas["n"] += 1

    step = PipelineStep(
        nome="sem_dados",
        func=lambda: None,
        retries=0,
        timeout=5,
        validacao=validacao,
    )

    nome, resultado, sucesso = _executar_passo("run-6", step)

    assert sucesso and resultado is None
    assert chamadas["n"] == 0


# ============================================================
# Dependências (orquestração unificada)
# ============================================================
def test_dependencias_definem_a_ordem_de_execucao():
    eventos = []

    def passo(nome, retorno=None):
        def func():
            eventos.append(nome)
            return pd.DataFrame({"x": [len(eventos)]}) if retorno is None else retorno

        return func

    steps = [
        PipelineStep(nome="c", func=passo("c"), dependencias=("b",), retries=0, timeout=5),
        PipelineStep(nome="a", func=passo("a"), retries=0, timeout=5),
        PipelineStep(nome="b", func=passo("b"), dependencias=("a",), retries=0, timeout=5),
    ]

    resultados = executar_pipeline("run-7", steps, max_workers=4)

    assert set(resultados) == {"a", "b", "c"}
    assert eventos.index("a") < eventos.index("b") < eventos.index("c")


def test_dependente_nao_executa_quando_dependencia_falha():
    executados = []

    def func_quebra():
        raise RuntimeError("boom")

    def func_bom():
        executados.append("ok")
        return pd.DataFrame({"x": [1]})

    steps = [
        PipelineStep(nome="raiz", func=func_quebra, retries=0, timeout=5),
        PipelineStep(
            nome="ok", func=func_bom, retries=0, timeout=5
        ),
        PipelineStep(
            nome="dependente",
            func=lambda: executados.append("dependente"),
            dependencias=("raiz",),
            retries=0,
            timeout=5,
        ),
    ]

    with patch(f"{MODULO}.logger.error"):
        resultados = executar_pipeline("run-8", steps, max_workers=2)

    assert resultados["raiz"] is None
    assert resultados["dependente"] is None
    assert "dependente" not in executados
    assert len(resultados["ok"]) == 1  # step sem relação segue normal


def test_falha_propaga_transitivamente_pela_cadeia():
    executados = []

    def func_quebra():
        raise RuntimeError("falha na origem")

    steps = [
        PipelineStep(nome="origem", func=func_quebra, retries=0, timeout=5),
        PipelineStep(
            nome="meio",
            func=lambda: executados.append("meio"),
            dependencias=("origem",),
            retries=0,
            timeout=5,
        ),
        PipelineStep(
            nome="fim",
            func=lambda: executados.append("fim"),
            dependencias=("meio",),
            retries=0,
            timeout=5,
        ),
    ]

    with patch(f"{MODULO}.logger.error"):
        resultados = executar_pipeline("run-9", steps, max_workers=2)

    assert set(resultados.values()) == {None}
    assert executados == []


def test_dependencia_circular_nao_trava_o_pipeline():
    steps = [
        PipelineStep(
            nome="x", func=lambda: pd.DataFrame(), dependencias=("y",), retries=0, timeout=5
        ),
        PipelineStep(
            nome="y", func=lambda: pd.DataFrame(), dependencias=("x",), retries=0, timeout=5
        ),
    ]

    with patch(f"{MODULO}.logger.error"):
        resultados = executar_pipeline("run-10", steps, max_workers=2)

    assert resultados == {"x": None, "y": None}


def test_dependencia_inexistente_levanta_valueerror():
    steps = [
        PipelineStep(
            nome="orfao",
            func=lambda: pd.DataFrame(),
            dependencias=("fantasma",),
            retries=0,
            timeout=5,
        )
    ]

    with pytest.raises(ValueError, match="inexistentes"):
        executar_pipeline("run-11", steps)


def test_nome_de_step_duplicado_levanta_valueerror():
    steps = [
        PipelineStep(nome="repetido", func=lambda: pd.DataFrame(), retries=0, timeout=5),
        PipelineStep(nome="repetido", func=lambda: pd.DataFrame(), retries=0, timeout=5),
    ]

    with pytest.raises(ValueError, match="duplicado"):
        executar_pipeline("run-12", steps)
