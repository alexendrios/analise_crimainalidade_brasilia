from typing import List
import pandas as pd
from util.log import logs

logger = logs()

def validar_chaves(df: pd.DataFrame, keys: List[str]) -> None:
    """
    Valida se há duplicidade nas chaves informadas.
    """
    logger.info("Iniciando validação de chaves", extra={"keys": keys})

    if not keys:
        logger.warning("Nenhuma chave foi fornecida para validação")
        return

    duplicados = df[df.duplicated(keys, keep=False)]

    if not duplicados.empty:
        qtd = len(duplicados)
        exemplo = duplicados.head(5).to_dict(orient="records")

        logger.error(
            "Duplicidade encontrada nas chaves",
            extra={
                "keys": keys,
                "quantidade_duplicados": qtd,
                "exemplo": exemplo
            }
        )

        raise ValueError(
            f"Duplicidade encontrada nas chaves {keys}. "
            f"Quantidade: {qtd}. Verifique os dados."
        )

    logger.info("Validação de chaves concluída sem duplicidades", extra={"keys": keys})


def validar_colunas(df: pd.DataFrame, colunas: List[str]) -> None:
    """
    Valida se todas as colunas esperadas existem no DataFrame.
    """
    logger.info("Iniciando validação de colunas", extra={"colunas_esperadas": colunas})

    if not colunas:
        logger.warning("Nenhuma coluna foi fornecida para validação")
        return

    colunas_df = set(df.columns)
    colunas_esperadas = set(colunas)

    faltando = colunas_esperadas - colunas_df

    if faltando:
        logger.error(
            "Colunas faltando no DataFrame",
            extra={
                "colunas_esperadas": list(colunas_esperadas),
                "colunas_encontradas": list(colunas_df),
                "faltando": list(faltando)
            }
        )

        raise ValueError(f"Colunas faltando: {faltando}")

    logger.info("Validação de colunas concluída com sucesso")
