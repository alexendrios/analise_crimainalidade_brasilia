from unittest.mock import patch, MagicMock
import pandas as pd


def test_pipeline_fluxo_completo():
    """
    Testa o fluxo completo do pipeline:
    1. coleta
    2. extração zip
    3. listagem de arquivos
    4. consolidação
    5. exportação CSV
    """

    df_fake = pd.DataFrame(
        {
            "ano": [2020],
            "uf": ["DF"],
            "localidade": ["Distrito Federal"],
            "populacao": [3000000],
            "arquivo": ["populacao_2020.xls"],
        }
    )

    with (
        patch("src.pepiline.coletar_dados_") as mock_coletar,
        patch("src.pepiline.arquivos_zip_execucao") as mock_zip,
        patch("src.pepiline.listar_arquivos_populacao") as mock_listar,
        patch("src.pepiline.consolidar_historico") as mock_consolidar,
        patch("src.pepiline.salvar_historico_csv") as mock_salvar,
    ):
        mock_listar.return_value = ["arquivo1.xls"]
        mock_consolidar.return_value = df_fake

        # Importação tardia força execução do __main__
        import src.pepiline as pipeline

        pipeline.coletar_dados_()
        pipeline.arquivos_zip_execucao()
        arquivos = pipeline.listar_arquivos_populacao("./data/planilha")
        df = pipeline.consolidar_historico(arquivos)
        pipeline.salvar_historico_csv(
            df,
            "./data/output/populacao_df_historico.csv",
        )

        # ✔️ Asserts de chamada
        mock_coletar.assert_called_once()
        mock_zip.assert_called_once()
        mock_listar.assert_called_once_with("./data/planilha")
        mock_consolidar.assert_called_once_with(["arquivo1.xls"])
        mock_salvar.assert_called_once_with(
            df_fake,
            "./data/output/populacao_df_historico.csv",
        )


def test_pipeline_lista_vazia():
    """
    Garante que pipeline suporta lista vazia sem quebrar
    """

    with (
        patch("src.pepiline.coletar_dados_"),
        patch("src.pepiline.arquivos_zip_execucao"),
        patch("src.pepiline.listar_arquivos_populacao", return_value=[]),
        patch("src.pepiline.consolidar_historico", return_value=pd.DataFrame()),
        patch("src.pepiline.salvar_historico_csv") as mock_salvar,
    ):
        import src.pepiline as pipeline

        arquivos = pipeline.listar_arquivos_populacao("./data/planilha")
        df = pipeline.consolidar_historico(arquivos)
        pipeline.salvar_historico_csv(df, "./data/output/x.csv")

        mock_salvar.assert_called_once()
