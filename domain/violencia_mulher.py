import pandas as pd
from ingestion.repository_adapter import Repository
from util.padronizacao import (
    padronizar_regiao,
    renomear_linha,
    recriar_regiao_com_valor,
    normalizar_colunas,
)
from processing.transform import processar_dataset_base
from validation.validator import validar_chaves


class ViolenciaMulherService:
    @staticmethod
    def carregar_feminicidio():
        return processar_dataset_base(
            df=Repository.load("feminicidio"),
            coluna_regiao="região_administrativa",
            nome_valor="casos_feminicidios",
            drop=["inserido_em"],
        ).replace(
            {
                "regiao_administrativa": {
                    "BRASILIA (PLANO PILOTO)": "PLANO PILOTO",
                    "SIA": "SIA (SETOR DE INDUSTRIA E ABASTECIMENTO)",
                    "SOL NASCENTE/POR DO SOL": "POR DO SOL/SOL NASCENTE",
                    "ARNIQUEIRA": "ARNIQUEIRAS",
                }
            }
        )

    @staticmethod
    def carregar_crimes_contra_mulher():
        df = Repository.load("crimes_contra_mulher")

        df["data_do_crime"] = pd.to_datetime(df["data_do_crime"])
        df["ano"] = df["data_do_crime"].dt.year
        _ = df
        
        df = (
            df.groupby(["ano", "ra"])["#_casos"]
            .sum()
            .reset_index()
            .rename(columns={"ra": "regiao_administrativa"})
        )

        df = padronizar_regiao(df, "regiao_administrativa")

        df = df.rename(columns={"#_casos": "crimes_contra_mulher"})

        # regras de negócio
        df = renomear_linha(
            df, "regiao_administrativa", "SCIA E ESTRUTURAL", "SCIA/ESTRUTURAL"
        )
        df = renomear_linha(
            df, "regiao_administrativa", "SUDOESTE", "SUDOESTE/OCTOGONAL"
        )
            
        df = recriar_regiao_com_valor(df, nome_regiao="VARJAO", coluna_regiao="regiao_administrativa", valor_padrao=0)
        df = recriar_regiao_com_valor(df, nome_regiao="LAGO NORTE", coluna_regiao="regiao_administrativa", valor_padrao=0)

        return normalizar_colunas(df), _

    @staticmethod
    def consolidar():
        df_feminicidio = ViolenciaMulherService.carregar_feminicidio()
        df_crimes, _ = ViolenciaMulherService.carregar_crimes_contra_mulher()
        # 🔒 validação antes do merge
        validar_chaves(df_feminicidio, ["ano", "regiao_administrativa"])
        validar_chaves(df_crimes, ["ano", "regiao_administrativa"])

        df = pd.merge(
            df_crimes,
            df_feminicidio,
            on=["ano", "regiao_administrativa"],
            how="outer",
        )

        df = df.fillna(0)

        df["crimes_contra_mulher"] = df["crimes_contra_mulher"].astype(int)

        df = df[df["ano"] != 0]

        return df
