from unittest.mock import patch, DEFAULT
import pandas as pd
import pytest

from src.pipeline_busca_transformacao import (
    busca_transformacao_dados,
    TRATAMENTOS,
    TRATAMENTOS_PREPARADOS,
    PIPELINE_SILVER,
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


@pytest.fixture(autouse=True)
def sem_schema_check(monkeypatch):
    """Isola a orquestração dos schema checks (cobertos em tests/validation)."""
    from validation import esquemas as modulo_esquemas

    monkeypatch.setattr(
        modulo_esquemas, "validar_saida_silver", lambda *args, **kwargs: None
    )


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
    Falha na coleta derruba a cadeia por dependência (carga nunca roda) e o
    executor registra o erro; a função externa não propaga a exceção.
    """
    with patch.multiple(MODULO, **{f: DEFAULT for f in FUNCOES_MOCKADAS}) as mocks:
        mocks["coletar_dados_"].side_effect = Exception("Erro simulado")

        with patch("src.core.executor.logger.error") as mock_error:
            # não deve levantar: falhas de step são capturadas pelo executor
            busca_transformacao_dados()

            assert any(
                "Erro simulado" in str(call) for call in mock_error.call_args_list
            )

        # etapas dependentes não devem ter sido chamadas
        mocks["salvar_tabela"].assert_not_called()


def test_tratamentos_sao_declarativos_e_com_nomes_unicos():
    """Paridade com o Gold: cada tratamento Silver é um PipelineStep declarativo."""
    assert TRATAMENTOS
    assert all(isinstance(step, PipelineStep) for step in TRATAMENTOS)
    nomes = [step.nome for step in TRATAMENTOS]
    assert len(nomes) == len(set(nomes))


def test_dag_silver_unificado_tem_nomes_unicos_e_cadeia_integra():
    """
    Orquestração unificada: fases sequenciais + tratamentos + carga formam um
    único DAG executado pelo mesmo motor do pipeline Gold.
    """
    nomes = [step.nome for step in PIPELINE_SILVER]
    assert len(nomes) == len(set(nomes))

    por_nome = {step.nome: step for step in PIPELINE_SILVER}
    assert por_nome["populacao"].dependencias == ("coleta",)
    assert por_nome["planilhas"].dependencias == ("populacao",)

    assert set(por_nome["carga"].dependencias) == {
        step.nome for step in TRATAMENTOS_PREPARADOS
    }
    for nome in ("coleta", "populacao", "planilhas", "carga"):
        assert nome in nomes


def test_tratamentos_dependem_de_planilhas_e_possuem_validacao():
    """Data quality automatizado: todo tratamento silver valida sua saída."""
    for original, preparado in zip(TRATAMENTOS, TRATAMENTOS_PREPARADOS):
        assert preparado.nome == original.nome
        assert preparado.dependencias == ("planilhas",)
        assert callable(preparado.validacao)


def test_busca_transformacao_usa_executor_paralelo_no_dag_unico():
    """executar_pipeline recebe o DAG completo e o max_workers informado."""
    with (
        patch.multiple(MODULO, **{f: DEFAULT for f in FUNCOES_MOCKADAS}),
        patch(f"{MODULO}.executar_pipeline") as mock_exec,
    ):
        busca_transformacao_dados(max_workers=4)

        mock_exec.assert_called_once()
        args, kwargs = mock_exec.call_args
        assert args[1] == PIPELINE_SILVER
        assert kwargs["max_workers"] == 4
