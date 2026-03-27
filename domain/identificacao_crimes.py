import pandas as pd
from ingestion.repository_adapter import Repository
from domain.violencia_mulher import ViolenciaMulherService
from util.padronizacao import (
    padronizar_regiao,
    renomear_linha,
    normalizar_colunas,
)


class IdentificacaoCrimesService:
    @staticmethod
    def carregar():
        _, crimes_contra_mulher = ViolenciaMulherService.carregar_crimes_contra_mulher()
        df = crimes_contra_mulher
        df = df[
            [
                "ano",
                "ra",
                "meio_utilizado",
                "local",
                "motivação",
                "idade___vítima",
                "idade___autor",
                "data_do_crime",
            ]
        ].copy()

        df = df.rename(columns={"ra": "regiao_administrativa"})

        # 🔹 padronização múltipla (melhorado)
        colunas_padronizar = [
            "regiao_administrativa",
            "meio_utilizado",
            "local",
            "motivação",
        ]

        for col in colunas_padronizar:
            df = padronizar_regiao(df, col)

        # 🔹 regras de negócio
        df = renomear_linha(
            df, "regiao_administrativa", "SUDOESTE", "SUDOESTE/OCTOGONAL"
        )
        df = renomear_linha(
            df, "regiao_administrativa", "SCIA E ESTRUTURAL", "SCIA/ESTRUTURAL"
        )

        # 🔹 tipagem segura
        df[["idade___vítima", "idade___autor"]] = (
            df[["idade___vítima", "idade___autor"]]
            .apply(pd.to_numeric, errors="coerce")
            .astype("Int64")
        )

        df["idade___autor"] = df["idade___autor"].fillna(0)
        df["idade___vítima"] = df["idade___vítima"].fillna(0)

        return normalizar_colunas(df)
