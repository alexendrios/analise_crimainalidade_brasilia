import pandas as pd
import logging

logger = logging.getLogger(__name__)


class DataHandler:
    @staticmethod
    def ler_csv(
        caminho_arquivo: str, sep: str = ";", engine: str = "python"
    ) -> pd.DataFrame:
        logger.info(
            f"Lendo CSV | path={caminho_arquivo} | sep='{sep}' | engine={engine}"
        )
        return pd.read_csv(caminho_arquivo, sep=sep, engine=engine)

    @staticmethod
    def salvar_csv(df: pd.DataFrame, caminho_saida: str, index: bool = False) -> None:
        logger.info(f"Salvando CSV | path={caminho_saida}")
        df.to_csv(caminho_saida, index=index)
