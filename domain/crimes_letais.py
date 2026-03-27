import pandas as pd
from functools import reduce
from ingestion.repository_adapter import Repository
from processing.transform import processar_dataset_base
from util.padronizacao import (
    padronizar_regiao,
    transformar_wide_para_long,
    recriar_regiao_com_valor,
    normalizar_colunas,
)
from validation.validator import validar_chaves


class CrimesLetaisService:
    @staticmethod
    def carregar_homicidio():
        return processar_dataset_base(
            df=Repository.load("homicidio"),
            coluna_regiao="regiao_administrativa",
            nome_valor="ocorrencia_homicidio",
            drop=["inserido_em"],
        )

    @staticmethod
    def _processar_wide_padrao(nome_tabela, nome_valor):
        """
        🔁 padrão reutilizável para latrocínio e lesão morte
        """
        df = Repository.load(nome_tabela).copy()

        df = df.drop(columns=["inserido_em"])
        df = df.rename(columns={"regiao": "regiao_administrativa"})

        df = padronizar_regiao(df, "regiao_administrativa")

        df = transformar_wide_para_long(
            df,
            "regiao_administrativa",
            nome_valor,
        )

        df = recriar_regiao_com_valor(
            df=df,
            coluna_regiao="regiao_administrativa",
            nome_regiao="UNIDADES PRISIONAIS",
            coluna_valor=nome_valor,
        )

        return normalizar_colunas(df)

    @staticmethod
    def carregar_latrocinio():
        return CrimesLetaisService._processar_wide_padrao(
            "latrocinio", "ocorrencia_latrocinio"
        )

    @staticmethod
    def carregar_lesao_morte():
        return CrimesLetaisService._processar_wide_padrao(
            "lesao_corporal_morte", "ocorrencia_lesao_morte"
        )

    @staticmethod
    def consolidar():
        df_homicidio = CrimesLetaisService.carregar_homicidio()
        df_latrocinio = CrimesLetaisService.carregar_latrocinio()
        df_lesao = CrimesLetaisService.carregar_lesao_morte()

        dfs = [df_homicidio, df_latrocinio, df_lesao]

        # 🔒 validação antes do merge
        for df in dfs:
            validar_chaves(df, ["ano", "regiao_administrativa"])

        df_final = reduce(
            lambda left, right: pd.merge(
                left, right, on=["ano", "regiao_administrativa"], how="outer"
            ),
            dfs,
        )

        df_final = df_final.fillna(0)

        return df_final
