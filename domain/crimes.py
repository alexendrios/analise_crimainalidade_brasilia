import pandas as pd
from functools import reduce
from typing import List
from util.log import logs
from validation.validator import validar_chaves

logger = logs()

def merge_seguro(dfs: List[pd.DataFrame], keys: List[str]) -> pd.DataFrame:
    """
    Realiza merge seguro entre múltiplos DataFrames com validações e logging.
    """

    logger.info(
        "Iniciando merge seguro", extra={"quantidade_dfs": len(dfs), "keys": keys}
    )

    if not dfs:
        logger.error("Lista de DataFrames vazia")
        raise ValueError("A lista de DataFrames não pode ser vazia")

    try:
        # -------------------------
        # VALIDA CHAVES
        # -------------------------
        for i, df in enumerate(dfs):
            logger.info(
                "Validando chaves do DataFrame",
                extra={"indice_df": i, "shape": df.shape},
            )
            validar_chaves(df, keys)

        # -------------------------
        # MERGE PROGRESSIVO
        # -------------------------
        def merge_step(left: pd.DataFrame, right: pd.DataFrame, step: int):
            linhas_left = len(left)
            linhas_right = len(right)

            resultado = pd.merge(left, right, on=keys, how="outer")

            linhas_resultado = len(resultado)

            logger.info(
                "Merge realizado",
                extra={
                    "step": step,
                    "linhas_left": linhas_left,
                    "linhas_right": linhas_right,
                    "linhas_resultado": linhas_resultado,
                    "delta_linhas": linhas_resultado - max(linhas_left, linhas_right),
                },
            )

            return resultado

        resultado = dfs[0]

        for i in range(1, len(dfs)):
            resultado = merge_step(resultado, dfs[i], i)

        logger.info(
            "Merge concluído com sucesso", extra={"shape_final": resultado.shape}
        )

        return resultado

    except Exception:
        logger.exception("Erro durante o merge seguro", extra={"keys": keys})
        raise