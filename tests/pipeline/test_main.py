import runpy
from unittest.mock import patch


def test_main_executa_as_tres_etapas_do_pipeline():
    with (
        patch("src.pipeline_busca_transformacao.busca_transformacao_dados") as mock_busca,
        patch("src.pipeline_tabela_gold.criar_tabela_gold") as mock_gold,
        patch("analysis.data_analyzer.executar_pipeline") as mock_analise,
    ):
        runpy.run_module("src.main", run_name="__main__")

    mock_busca.assert_called_once()
    mock_gold.assert_called_once_with(max_workers=6)
    mock_analise.assert_called_once()
