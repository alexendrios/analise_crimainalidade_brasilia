import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from typing import Any
from util.log import logs

logger = logs()


def _executar_passo(run_id, step):
    """
    Executa um step com retry e timeout. Retorna (nome, resultado, sucesso).

    O hook `step.validacao` roda sobre o resultado dentro do mesmo ciclo de
    retry: falha de validação (ex.: schema check) conta como falha do step.
    """
    tentativa = 0

    while tentativa <= step.retries:
        start = time.time()

        try:
            logger.info(f"[{run_id}] ▶️ {step.nome} | tentativa={tentativa + 1}")

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(step.func)
                result = future.result(timeout=step.timeout)

            if step.validacao is not None and result is not None:
                step.validacao(result)

            tempo = round(time.time() - start, 2)

            linhas = len(result) if hasattr(result, "__len__") else "N/A"

            logger.info(f"[{run_id}] ✅ {step.nome} | linhas={linhas} | tempo={tempo}s")

            return step.nome, result, True

        except TimeoutError:
            logger.error(f"[{run_id}] ⏱️ TIMEOUT em {step.nome}")

        except Exception as e:
            logger.error(
                f"[{run_id}] ❌ ERRO em {step.nome}: {str(e)}",
                exc_info=True,
            )

        tentativa += 1

        if tentativa <= step.retries:
            logger.warning(
                f"[{run_id}] 🔁 Retry {tentativa}/{step.retries} - {step.nome}"
            )

    logger.error(f"[{run_id}] 💥 Falha definitiva: {step.nome}")
    return step.nome, None, False


def executar_com_retry(run_id, step):
    """Compatibilidade: executa o step e retorna (nome, resultado)."""
    nome, resultado, _sucesso = _executar_passo(run_id, step)
    return nome, resultado


def _marcar_falha_sem_execucao(run_id, nome, motivo, resultados, falhas):
    logger.error(
        f"[{run_id}] 🚧 {nome} não será executado ({motivo}); dependências insatisfeitas"
    )
    resultados[nome] = None
    falhas.add(nome)


def executar_pipeline(run_id, steps, max_workers=4):
    """
    Orquestra os steps respeitando `dependencias`.

    Steps sem dependências (ou com todas satisfeitas) rodam em paralelo; um
    step só é submetido quando TODAS as suas dependências concluíram com
    sucesso. Se uma dependência falha definitivamente, os passos dependentes
    são pulados (resultado None) e a falha se propaga pela cadeia.
    """
    por_nome = {}
    for step in steps:
        if step.nome in por_nome:
            raise ValueError(f"Nome de step duplicado no pipeline: '{step.nome}'")
        por_nome[step.nome] = step

    desconhecidas = {
        dep
        for step in steps
        for dep in step.dependencias
        if dep not in por_nome
    }
    if desconhecidas:
        raise ValueError(
            f"Dependências inexistentes no pipeline: {sorted(desconhecidas)}"
        )

    resultados: dict[str, Any] = {}
    sucesso = set()
    falhas = set()
    pendentes = dict(por_nome)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futuros = {}

        while pendentes or futuros:
            prontos = [
                nome
                for nome, step in pendentes.items()
                if all(dep in sucesso for dep in step.dependencias)
            ]
            bloqueados = [
                nome
                for nome, step in pendentes.items()
                if any(dep in falhas for dep in step.dependencias)
            ]

            for nome in bloqueados:
                pendentes.pop(nome)
                _marcar_falha_sem_execucao(
                    run_id,
                    nome,
                    f"falha em {', '.join(step.dependencias)}",
                    resultados,
                    falhas,
                )

            for nome in prontos:
                step = pendentes.pop(nome)
                futuros[pool.submit(_executar_passo, run_id, step)] = nome

            if not futuros:
                if pendentes:
                    # nada rodando e nada pronto: dependência circular
                    nomes_presos = sorted(pendentes)
                    logger.error(
                        f"[{run_id}] ♻️ Dependência circular detectada entre: "
                        f"{nomes_presos}"
                    )
                    for nome in nomes_presos:
                        pendentes.pop(nome)
                        _marcar_falha_sem_execucao(
                            run_id,
                            nome,
                            "dependência circular",
                            resultados,
                            falhas,
                        )
                continue

            concluidos, _restantes = wait(futuros, return_when=FIRST_COMPLETED)

            for future in concluidos:
                nome = futuros.pop(future)
                _nome, resultado, deuCerto = future.result()
                resultados[nome] = resultado
                (sucesso if deuCerto else falhas).add(nome)

    return resultados
