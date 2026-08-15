from unittest.mock import patch, DEFAULT
import pandas as pd

from src.pipeline_busca_transformacao import (
    busca_transformacao_dados,
    TRATAMENTOS,
)
from src.core.pipeline_step import PipelineStep

MODULO = "src.pipeline_busca_transformacao"

# Todas as funções externas que busca_transformacao_dados() chama diretamente
FUNCOES_MOCKADAS = [
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
    "salvar_tabela",
    "close_engine",
]


def test_pipeline_fluxo_completo():
    """Executa o pipeline feliz: todas as etapas são chamadas uma vez."""
    with patch.multiple(MODULO, **{f: DEFAULT for f in FUNCOES_MOCKADAS}) as mocks:
        busca_transformacao_dados()

        mocks["coletar_dados_"].assert_called_once()
        mocks["arquivos_zip_execucao"].assert_called_once()
        mocks["processar_populacao"].assert_called_once()
        mocks["obter_dados_ra_populacao"].assert_called_once()
        mocks["processar_crimes"].assert_called_once()
        mocks["salvar_tabela"].assert_called_once()
        mocks["close_engine"].assert_called_once()


def test_pipeline_crimes_retorna_vazio_nao_quebra():
    """Se processar_crimes não retorna nada, o pipeline segue sem lançar exceção."""
    with patch.multiple(MODULO, **{f: DEFAULT for f in FUNCOES_MOCKADAS}) as mocks:
        mocks["processar_crimes"].return_value = None

        busca_transformacao_dados()

        mocks["processar_crimes"].assert_called_once()


def test_pipeline_cobre_bloco_except():
    """
    A função captura qualquer exceção internamente (não relança) e loga via
    logger.exception. Simulamos falha logo na coleta para cobrir esse bloco.
    """
    with patch.multiple(MODULO, **{f: DEFAULT for f in FUNCOES_MOCKADAS}) as mocks:
        mocks["coletar_dados_"].side_effect = Exception("Erro simulado")

        with patch(f"{MODULO}.logger.exception") as mock_logger_exc:
            # não deve levantar, pois o except interno engole a exceção
            busca_transformacao_dados()

            mock_logger_exc.assert_called()
            assert "Erro simulado" in str(mock_logger_exc.call_args)

        # etapas posteriores não devem ter sido chamadas
        mocks["salvar_tabela"].assert_not_called()


def test_tratamentos_sao_declarativos_e_com_nomes_unicos():
    """Paridade com o Gold: cada tratamento Silver é um PipelineStep declarativo."""
    assert TRATAMENTOS
    assert all(isinstance(step, PipelineStep) for step in TRATAMENTOS)
    nomes = [step.nome for step in TRATAMENTOS]
    assert len(nomes) == len(set(nomes))


def test_busca_transformacao_usa_executor_paralelo_nos_tratamentos():
    """Paridade com o Gold: executar_pipeline recebe os TRATAMENTOS e max_workers."""
    with (
        patch.multiple(MODULO, **{f: DEFAULT for f in FUNCOES_MOCKADAS}),
        patch(f"{MODULO}.executar_pipeline") as mock_exec,
    ):
        busca_transformacao_dados(max_workers=4)

        mock_exec.assert_called_once()
        args, kwargs = mock_exec.call_args
        assert args[1] == TRATAMENTOS
        assert kwargs["max_workers"] == 4
