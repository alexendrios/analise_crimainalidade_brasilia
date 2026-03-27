class CrimesPatrimoniaisService:
    @staticmethod
    def consolidar():
        from config.datasets_config import DATASETS_CONFIG
        from ingestion.repository_adapter import Repository
        from processing.transform import processar_dataset_base
        from domain.crimes import merge_seguro

        dfs = [
            processar_dataset_base(
                df=Repository.load(cfg["nome"]),
                coluna_regiao=cfg["coluna_regiao"],
                nome_valor=cfg["valor"],
                drop=cfg.get("drop"),
                filtro=cfg.get("filtro"),
            )
            for cfg in DATASETS_CONFIG
        ]

        df_final = merge_seguro(
            dfs,
            keys=["ano", "regiao_administrativa"],
        )

        return df_final.fillna(0)
