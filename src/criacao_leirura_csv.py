from util.data_handler import DataHandler


def transformacao_dados_csv(
    path_file_origen: str,
    path_file_saida: str,
    separador: str = ";",
) -> None:
    """Função para transformar dados CSV."""
    df = DataHandler.ler_csv(path_file_origen, sep=separador)
    DataHandler.salvar_csv(df, path_file_saida)
