from unittest.mock import patch
from pathlib import Path
import pandas as pd

# Fake DataFrame para simular consolidação
df_fake = pd.DataFrame(
    {
        "ano": [2020],
        "uf": ["DF"],
        "localidade": ["Distrito Federal"],
        "populacao": [3000000],
        "arquivo": ["populacao_2020.xls"],
    }
)


def test_pipeline_fluxo_completo():
    """
    Testa o fluxo completo do pipeline:
    1. coleta de dados
    2. extração zip
    3. processamento de população
    4. obtenção de dados RA
    5. processamento de crimes
    """

    with (
        patch("src.busca.coletar_dados_") as mock_coletar,
        patch("util.extrator_zip.arquivos_zip_execucao") as mock_zip,
        patch("util.leitor_excel.processar_populacao") as mock_pop,
        patch("src.scraping.obter_dados_ra_populacao") as mock_ra,
        patch("util.leitor_excel.processar_crimes") as mock_crimes,
    ):
        # Mock simples apenas para validar execução
        mock_pop.return_value = None
        mock_ra.return_value = None
        mock_crimes.return_value = None

        # Importação tardia para executar o __main__ do pepiline
        import src.pepiline as pipeline

        pipeline.main()

        # ✅ Asserts de chamada
        mock_coletar.assert_called_once()
        mock_zip.assert_called_once()
        mock_pop.assert_called_once()
        mock_ra.assert_called_once()
        mock_crimes.assert_called_once()


def test_pipeline_crimes_lista_vazia():
    """
    Testa pipeline quando não há planilhas de crimes (lista vazia).
    """
    with (
        patch("src.busca.coletar_dados_"),
        patch("util.extrator_zip.arquivos_zip_execucao"),
        patch("util.leitor_excel.processar_populacao"),
        patch("src.scraping.obter_dados_ra_populacao"),
        patch("util.leitor_excel.processar_crimes") as mock_crimes,
    ):
        mock_crimes.return_value = None

        import src.pepiline as pipeline

        pipeline.main()

        # Como a lista de planilhas está vazia, processar_crimes NÃO deve ser chamado
        mock_crimes.assert_not_called()