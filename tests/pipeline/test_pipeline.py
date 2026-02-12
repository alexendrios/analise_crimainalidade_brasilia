from unittest.mock import patch
from src.pipeline import main
import pandas as pd
import importlib


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

    with (
        patch("src.pipeline.coletar_dados_") as mock_coletar,
        patch("src.pipeline.arquivos_zip_execucao") as mock_zip,
        patch("src.pipeline.processar_populacao") as mock_pop,
        patch("src.pipeline.obter_dados_ra_populacao") as mock_ra,
        patch("src.pipeline.processar_crimes") as mock_crimes,
    ):
        import src.pipeline as pipeline

        pipeline.main()

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
        patch("src.pipeline.coletar_dados_"),
        patch("src.pipeline.arquivos_zip_execucao"),
        patch("src.pipeline.processar_populacao"),
        patch("src.pipeline.obter_dados_ra_populacao"),
        patch("src.pipeline.processar_crimes") as mock_crimes,
    ):
        mock_crimes.return_value = None

        main()

        mock_crimes.assert_called_once()


def test_main_cobre_except_e_main():
    """
    Testa o bloco de exceção do main, simulando erro
    em todas as funções do pipeline.
    """

    funcoes_mockadas = [
        "coletar_dados_",
        "arquivos_zip_execucao",
        "processar_populacao",
        "obter_dados_ra_populacao",
        "analisar_populacao",
        "tratar_populacao_regiao_administrativa",
        "processar_crimes",
        "tratar_crimes_contra_mulher",
        "tratar_feminicidio",
        "tratar_desaparecidos_idade_sexo",
        "tratar_desaparecidos_localizados",
        "tratar_desaparecidos_regiao",
        "tratar_furto_veiculo",
        "tratar_homicidio",
        "tratar_violencia_idosos",
        "tratar_crimes_idosos_ranking",
        "crimes_idosos_por_mes",
        "tratar_injuria_racial_por_regiao",
        "tratar_latrocinio_por_regiao",
        "tratar_lesao_corporal_morte_por_regiao",
        "tratar_lesao_corporal_morte",
        "tratar_racismo",
        "tratar_roubo_pedestre",
        "tratar_roubo_veiculo",
        "roubo_comercio",
        "roubo_transporte_coletivo",
    ]

    patches = [patch(f"src.pipeline.{f}") for f in funcoes_mockadas]

    mocks = []
    for p in patches:
        mock = p.start()
        mock.side_effect = Exception("Erro simulado")
        mocks.append(mock)

    with patch("src.pipeline.logger.exception") as mock_logger_exc:
        main()

        mock_logger_exc.assert_called()
        assert "Erro simulado" in str(mock_logger_exc.call_args)

    for p in patches:
        p.stop()
