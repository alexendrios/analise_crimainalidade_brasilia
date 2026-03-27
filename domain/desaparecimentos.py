from ingestion.repository_adapter import Repository


class DesaparecimentosService:
    @staticmethod
    def _load_and_clean(dataset_name: str, columns_to_drop: list[str]):
        df = Repository.load(dataset_name)
        return df.drop(columns=columns_to_drop, errors="ignore")

    @staticmethod
    def carregar_desaparecidos_idade_sexo():
        return DesaparecimentosService._load_and_clean(
            "desaparecidos_idade_sexo", ["inserido_em"]
        )

    @staticmethod
    def carregar_desaparecidos_localizados():
        return DesaparecimentosService._load_and_clean(
            "desaparecimento_localizados", ["inserido_em"]
        )

    @staticmethod
    def carregar_desaparecidos_regiao():
        return DesaparecimentosService._load_and_clean(
            "desaparecimento_regiao",
            [
                "inserido_em",
                "variacao_absoluta",
                "participacao_percentual_2021",
            ],
        )
