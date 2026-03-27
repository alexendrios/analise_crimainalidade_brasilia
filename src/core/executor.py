import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from util.log import logs

logger = logs()


def executar_com_retry(run_id, step):
    tentativa = 0

    while tentativa <= step.retries:
        start = time.time()

        try:
            logger.info(f"[{run_id}] ▶️ {step.nome} | tentativa={tentativa + 1}")

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(step.func)
                result = future.result(timeout=step.timeout)

            tempo = round(time.time() - start, 2)

            linhas = len(result) if hasattr(result, "__len__") else "N/A"

            logger.info(f"[{run_id}] ✅ {step.nome} | linhas={linhas} | tempo={tempo}s")

            return step.nome, result

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
    return step.nome, None


def executar_pipeline(run_id, steps, max_workers=4):
    resultados = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(executar_com_retry, run_id, step) for step in steps]

        for future in as_completed(futures):
            nome, resultado = future.result()
            resultados[nome] = resultado

    return resultados
