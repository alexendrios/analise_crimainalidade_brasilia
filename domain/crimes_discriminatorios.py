import pandas as pd
from functools import reduce
from ingestion.repository_adapter import Repository
from util.padronizacao import (
    padronizar_regiao,
    transformar_wide_para_long,
    normalizar_colunas,
)
from validation.validator import validar_chaves


class CrimesDiscriminatoriosService:
    @staticmethod
    def _processar_padrao(nome_tabela, nome_valor):
        df = Repository.load(nome_tabela).copy()

        df = df.drop(columns=["inserido_em"])
        df = df.rename(columns={"regiao": "regiao_administrativa"})

        df = padronizar_regiao(df, "regiao_administrativa")

        df = transformar_wide_para_long(
            df,
            "regiao_administrativa",
            nome_valor,
        )

        return normalizar_colunas(df)

    @staticmethod
    def carregar_racismo():
        return CrimesDiscriminatoriosService._processar_padrao(
            "racismo", "ocorrencia_racismo"
        )

    @staticmethod
    def carregar_injuria():
        return CrimesDiscriminatoriosService._processar_padrao(
            "injuria_racial", "ocorrencia_injuria"
        )

    @staticmethod
    def consolidar():
        df_racismo = CrimesDiscriminatoriosService.carregar_racismo()
        df_injuria = CrimesDiscriminatoriosService.carregar_injuria()

        dfs = [df_racismo, df_injuria]

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

        # 🔧 tipagem
        for col in df_final.columns:
            if col not in ["ano", "regiao_administrativa"]:
                df_final[col] = df_final[col].astype(int)

        return df_final.sort_values(["ano", "regiao_administrativa"])
