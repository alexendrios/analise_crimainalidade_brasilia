# src/pipeline_busca_transformacao.py
from dataclasses import replace
from pathlib import Path
from util.extrator_zip import arquivos_zip_execucao
from util.leitor_excel import processar_populacao, processar_crimes
from src.busca import coletar_dados_
from src.scraping import obter_dados_ra_populacao
from src.tratamento_populacional import (
    analisar_populacao,
    tratar_populacao_regiao_administrativa,
)
from src.tratamento_crimes import (
    tratar_feminicidio,
    tratar_desaparecidos_idade_sexo,
    tratar_desaparecidos_localizados,
    tratar_desaparecidos_regiao,
    tratar_furto_veiculo,
    tratar_crimes_contra_mulher,
    tratar_homicidio,
    tratar_violencia_idosos,
    tratar_crimes_idosos_ranking,
    crimes_idosos_por_mes,
    tratar_injuria_racial_por_regiao,
    tratar_latrocinio_por_regiao,
    tratar_lesao_corporal_morte_por_regiao,
    tratar_lesao_corporal_morte,
    tratar_racismo,
    tratar_roubo_pedestre,
    tratar_roubo_veiculo,
    roubo_comercio,
    roubo_transporte_coletivo,
)
from database.load_csvs import salvar_tabela
from database.connection import close_engine
from src.core.pipeline_step import PipelineStep
from src.core.executor import executar_pipeline
from validation.esquemas import validador_silver
from util.log import logs
import time

logger = logs()

PASTA_BRONZE_CSV = "./data/bronze/csv"
PASTA_SILVER_OUTPUT = "./data/silver/output"
PASTA_BRONZE_PLANILHA = Path("./data/bronze/planilha")
PASTA_BRONZE_CSV_DIR = Path(PASTA_BRONZE_CSV)
TIMEOUT_TRATAMENTO = 900


def log_tempo_inicio(func_name):
    logger.info("========== ETAPA: %s ==========", func_name)
    start = time.time()
    return start


def log_tempo_fim(func_name, start_time):
    fim = time.time()
    logger.info(
        "========== ETAPA: %s | tempo: %.2f seg ==========", func_name, fim - start_time
    )


# 🔹 definição declarativa dos tratamentos independentes (executados em paralelo)
TRATAMENTOS = [
    PipelineStep(
        "crimes_contra_mulher",
        lambda: tratar_crimes_contra_mulher(
            f"{PASTA_BRONZE_CSV}/crimes-contra-mulher.csv",
            f"{PASTA_SILVER_OUTPUT}/crimes-contra-mulher_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "feminicidio",
        lambda: tratar_feminicidio(
            f"{PASTA_BRONZE_CSV}/feminicidio.csv",
            f"{PASTA_SILVER_OUTPUT}/feminicidio_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "desaparecidos_idade_sexo",
        lambda: tratar_desaparecidos_idade_sexo(
            f"{PASTA_BRONZE_CSV}/desaparecimento-idade-sexo.csv",
            f"{PASTA_SILVER_OUTPUT}/desaparecidos_idade_sexo_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "desaparecidos_localizados",
        lambda: tratar_desaparecidos_localizados(
            f"{PASTA_BRONZE_CSV}/desaparecimento-localizados.csv",
            f"{PASTA_SILVER_OUTPUT}/desaparecimento-localizados_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "desaparecidos_regiao",
        lambda: tratar_desaparecidos_regiao(
            f"{PASTA_BRONZE_CSV}/desaparecimento-regiao.csv",
            f"{PASTA_SILVER_OUTPUT}/desaparecimento-regiao_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "furto_veiculo",
        lambda: tratar_furto_veiculo(
            f"{PASTA_BRONZE_CSV}/furto-em-veiculo.csv",
            f"{PASTA_SILVER_OUTPUT}/furto_em_veiculo_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "homicidio",
        lambda: tratar_homicidio(
            f"{PASTA_BRONZE_CSV}/homicidio.csv",
            f"{PASTA_SILVER_OUTPUT}/homicidio_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "violencia_idosos",
        lambda: tratar_violencia_idosos(
            f"{PASTA_BRONZE_CSV}/idosos_7_anos.csv",
            [
                f"{PASTA_SILVER_OUTPUT}/idosos_tabela4.csv",
                f"{PASTA_SILVER_OUTPUT}/idosos_tabela5.csv",
            ],
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "crimes_idosos_ranking",
        lambda: tratar_crimes_idosos_ranking(
            f"{PASTA_BRONZE_CSV}/idosos_2016.csv",
            f"{PASTA_SILVER_OUTPUT}/idosos_2016_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "crimes_idosos_mensais",
        lambda: crimes_idosos_por_mes(
            f"{PASTA_BRONZE_CSV}/idosos_mensais.csv",
            ["registro", "fato"],
            f"{PASTA_SILVER_OUTPUT}/idosos_mensais_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "injuria_racial",
        lambda: tratar_injuria_racial_por_regiao(
            f"{PASTA_BRONZE_CSV}/injuria-racial.csv",
            f"{PASTA_SILVER_OUTPUT}/injuria_racial_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "latrocinio",
        lambda: tratar_latrocinio_por_regiao(
            f"{PASTA_BRONZE_CSV}/latrocinio.csv",
            f"{PASTA_SILVER_OUTPUT}/latrocinio_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "lesao_corporal_morte_regiao",
        lambda: tratar_lesao_corporal_morte_por_regiao(
            f"{PASTA_BRONZE_CSV}/lesao-corporal-morte.csv",
            f"{PASTA_SILVER_OUTPUT}/lesao_corporal_morte_tratada.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "lesao_corporal_morte_total",
        lambda: tratar_lesao_corporal_morte(
            f"{PASTA_BRONZE_CSV}/lesao-corporal-morte.csv",
            f"{PASTA_SILVER_OUTPUT}/lesao_corporal_morte_total_tratada.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "racismo",
        lambda: tratar_racismo(
            f"{PASTA_BRONZE_CSV}/racismo.csv",
            f"{PASTA_SILVER_OUTPUT}/racismo_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "roubo_pedestre",
        lambda: tratar_roubo_pedestre(
            f"{PASTA_BRONZE_CSV}/roubo-a-transeunte.csv",
            f"{PASTA_SILVER_OUTPUT}/roubo-a-transeunte_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "roubo_veiculo",
        lambda: tratar_roubo_veiculo(
            f"{PASTA_BRONZE_CSV}/roubo-de-veiculo.csv",
            f"{PASTA_SILVER_OUTPUT}/roubo_veiculo_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "roubo_comercio",
        lambda: roubo_comercio(
            f"{PASTA_BRONZE_CSV}/roubo-em-comercio.csv",
            f"{PASTA_SILVER_OUTPUT}/roubo_comercio.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
    PipelineStep(
        "roubo_transporte_coletivo",
        lambda: roubo_transporte_coletivo(
            f"{PASTA_BRONZE_CSV}/roubo-em-transporte-coletivo.csv",
            f"{PASTA_SILVER_OUTPUT}/roubo_transporte_coletivo_tratado.csv",
        ),
        timeout=TIMEOUT_TRATAMENTO,
    ),
]


def _fase_coleta():
    start = log_tempo_inicio("Estágio 1 - Coleta de Dados")
    coletar_dados_()
    arquivos_zip_execucao()
    log_tempo_fim("Coleta de Dados", start)


def _fase_populacao():
    start = log_tempo_inicio("Estágio 2 - Processamento População")
    processar_populacao()
    obter_dados_ra_populacao()
    analisar_populacao()
    tratar_populacao_regiao_administrativa(
        f"{PASTA_BRONZE_CSV}/ra_df_populacao.csv",
        f"{PASTA_SILVER_OUTPUT}/ra_df_populacao_tratado.csv",
    )
    log_tempo_fim("Tratamento População ", start)


def _fase_planilhas():
    start = log_tempo_inicio(
        "Estágio 3 - Processamento Crimes - Transformação de Planilha em CSV"
    )
    PASTA_BRONZE_CSV_DIR.mkdir(parents=True, exist_ok=True)  # garante que a pasta exista
    processar_crimes(PASTA_BRONZE_PLANILHA, PASTA_BRONZE_CSV_DIR)
    log_tempo_fim("Processamento Crimes", start)


def _fase_carga():
    start = log_tempo_inicio("Estágio 8 - Carga de Dados no Banco")
    try:
        salvar_tabela()
    finally:
        close_engine()
    log_tempo_fim("Carga de Dados no Banco", start)


# 🔹 orquestração unificada: todas as fases (inclusive as sequenciais) são
# PipelineStep com dependências, executadas pelo mesmo motor do Gold.
FASES_BASE = [
    PipelineStep("coleta", lambda: (_fase_coleta(), None)[1], retries=0, timeout=1800),
    PipelineStep(
        "populacao", _fase_populacao, dependencias=("coleta",), retries=0, timeout=1800
    ),
    PipelineStep(
        "planilhas", _fase_planilhas, dependencias=("populacao",), retries=0, timeout=1800
    ),
]


def _preparar_tratamentos(steps):
    """Adiciona dependência da fase de planilhas e o hook de schema check."""
    return [
        replace(
            step,
            dependencias=("planilhas",),
            validacao=validador_silver(step.nome),
        )
        for step in steps
    ]


TRATAMENTOS_PREPARADOS = _preparar_tratamentos(TRATAMENTOS)

STEP_CARGA = PipelineStep(
    "carga",
    _fase_carga,
    dependencias=tuple(step.nome for step in TRATAMENTOS_PREPARADOS),
    retries=0,
    timeout=1800,
)

PIPELINE_SILVER = FASES_BASE + TRATAMENTOS_PREPARADOS + [STEP_CARGA]


def busca_transformacao_dados(max_workers: int = 6):
    pipeline_start = log_tempo_inicio("Pipeline Completo")

    try:
        # ⚡ DAG único: coleta → população → planilhas → tratamentos ∥ → carga
        executar_pipeline("silver", PIPELINE_SILVER, max_workers=max_workers)

        logger.info("Pipeline finalizado com sucesso!")

    except Exception as e:
        logger.exception("Erro durante a execução do pipeline: %s", e)
        raise

    log_tempo_fim("Pipeline Completo", pipeline_start)
