import pandas as pd
from util.padronizacao import (
    padronizar_regiao,
    transformar_wide_para_long,
    normalizar_colunas,
    remover_acentos,
)
from typing import List, Optional
from util.log import logs

logger = logs()


def processar_dataset_base(
    df: pd.DataFrame,
    coluna_regiao: str,
    nome_valor: str,
    drop: Optional[List[str]] = None,
    filtro: Optional[str] = None,
) -> pd.DataFrame:
    """
    Pipeline base de processamento de dataset com observabilidade.
    """

    logger.info(
        "Iniciando processamento do dataset",
        extra={
            "shape_inicial": df.shape,
            "coluna_regiao": coluna_regiao,
            "nome_valor": nome_valor,
        },
    )

    try:

        if drop:
            colunas_existentes = [c for c in drop if c in df.columns]
            colunas_ausentes = list(set(drop) - set(df.columns))

            if colunas_ausentes:
                logger.warning(
                    "Colunas para drop não existem no DataFrame",
                    extra={"colunas_ausentes": colunas_ausentes},
                )

            df = df.drop(columns=colunas_existentes)

            logger.info(
                "Colunas removidas",
                extra={
                    "colunas_removidas": colunas_existentes,
                    "shape_pos_drop": df.shape,
                },
            )

        df = padronizar_regiao(df, coluna_regiao)

        logger.info("Região padronizada", extra={"coluna_regiao": coluna_regiao})

        shape_antes = df.shape

        df = transformar_wide_para_long(df, coluna_regiao, nome_valor)

        logger.info(
            "Transformação wide_to_long concluída",
            extra={"shape_antes": shape_antes, "shape_depois": df.shape},
        )

        if filtro:
            filtro_original = filtro
            filtro = remover_acentos(filtro.upper())

            linhas_antes = len(df)

            df = df[df[coluna_regiao] != filtro]

            linhas_depois = len(df)

            logger.info(
                "Filtro aplicado",
                extra={
                    "filtro_original": filtro_original,
                    "filtro_normalizado": filtro,
                    "linhas_removidas": linhas_antes - linhas_depois,
                },
            )

        df = normalizar_colunas(df)

        logger.info("Colunas normalizadas", extra={"colunas_finais": list(df.columns)})

        logger.info(
            "Processamento concluído com sucesso", extra={"shape_final": df.shape}
        )

        return df

    except Exception as e:
        logger.exception(
            "Erro no processamento do dataset",
            extra={"coluna_regiao": coluna_regiao, "nome_valor": nome_valor},
        )
        raise