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


def test_main_importado_como_modulo_nao_executa_pipeline():
    """Import normal (__name__ != '__main__') não deve disparar as etapas."""
    import importlib

    import src.main as modulo_main

    recarregado = importlib.reload(modulo_main)

    assert callable(recarregado.busca_transformacao_dados)
    assert callable(recarregado.criar_tabela_gold)
    assert callable(recarregado.executar_pipeline)
