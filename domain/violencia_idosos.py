from ingestion.repository_adapter import Repository


class ViolenciaIdososService:
    @staticmethod
    def carregar_resumo():
        df = Repository.load("violencia_idosos")

        return df[
            [
                "ranking",
                "regiao_administrativa",
                "jan_ago_2016",
                "jan_ago_2017",
            ]
        ].copy()

    @staticmethod
    def carregar_mensal():
        df = Repository.load("violencia_idosos_mensais")
        return df.drop(columns=["inserido_em", "subnotificacao"])

    @staticmethod
    def carregar_ocorrencias():
        df = Repository.load("violencia_idosos_ocorrencias")
        return df.drop(columns=["inserido_em"])

    @staticmethod
    def carregar_sexo():
        df = Repository.load("violencia_idosos_sexo")
        return df.drop(columns=["inserido_em", "total"])
