import time
import uuid
from ingestion.repository_adapter import Repository
from domain.desaparecimentos import DesaparecimentosService
from domain.violencia_mulher import ViolenciaMulherService
from domain.identificacao_crimes import IdentificacaoCrimesService
from domain.violencia_idosos import ViolenciaIdososService
from domain.crimes_letais import CrimesLetaisService
from domain.crimes_discriminatorios import CrimesDiscriminatoriosService
from domain.crimes_patrimoniais import CrimesPatrimoniaisService
from src.core.pipeline_step import PipelineStep
from src.core.executor import executar_pipeline
from util.log import logs

logger = logs()


# 🔹 definição declarativa
STEPS = [
    PipelineStep(
        "violencia_mulher",
        ViolenciaMulherService.consolidar,
        "violencia_contra_mulher_gold",
    ),
    PipelineStep(
        "identificacao",
        IdentificacaoCrimesService.carregar,
        "identificacao_crimes_contra_mulher_gold",
    ),
    PipelineStep(
        "idosos",
        ViolenciaIdososService.carregar_resumo,
        "violencia_idosos_gold",
    ),
    PipelineStep(
        "idosos_ocorrencias",
        ViolenciaIdososService.carregar_ocorrencias,
        "violencia_idosos_ocorrencias_gold",
    ),
    PipelineStep(
        "idosos_mensais",
        ViolenciaIdososService.carregar_mensal,
        "violencia_idosos_mensais_gold",
    ),
    PipelineStep(
        "idosos_sexo",
        ViolenciaIdososService.carregar_sexo,
        "violencia_idosos_sexo_gold",
    ),
    PipelineStep(
        "crimes_roubo_furto",
        CrimesPatrimoniaisService.consolidar,
        "crimes_roubo_furto_gold",
    ),
    PipelineStep(
        "crimes_letais",
        CrimesLetaisService.consolidar,
        "crimes_letais_gold",
    ),
    PipelineStep(
        "crimes_discriminatorios",
        CrimesDiscriminatoriosService.consolidar,
        "crimes_discriminatorios_gold",
    ),
    PipelineStep(
        "desaparecidos_idade_sexo",
        DesaparecimentosService.carregar_desaparecidos_idade_sexo,
        "desaparecidos_idade_sexo_gold",
    ),
    PipelineStep(
        "desaparecidos_localizados",
        DesaparecimentosService.carregar_desaparecidos_localizados,
        "desaparecidos_localizados_gold",
    ),
    PipelineStep(
        "desaparecidos_regiao",
        DesaparecimentosService.carregar_desaparecidos_regiao,
        "desaparecidos_regiao_gold",
    ),
]


def criar_tabela_gold(max_workers: int = 6):
    run_id = str(uuid.uuid4())[:8]
    start_total = time.time()

    logger.info(f"[{run_id}] 🚀 START PIPELINE GOLD")

    try:
        # ⚡ execução paralela
        resultados = executar_pipeline(run_id, STEPS, max_workers=max_workers)

        # 💾 persistência
        for step in STEPS:
            df = resultados.get(step.nome)

            if df is None:
                logger.warning(f"[{run_id}] ⚠️ Pulando {step.output} (sem dados)")
                continue

            linhas = len(df) if hasattr(df, "__len__") else "N/A"

            logger.info(f"[{run_id}] 💾 Salvando: {step.output} | linhas={linhas}")

            try:
                Repository.save(df, step.output)
            except Exception as e:
                logger.error(
                    f"[{run_id}] ❌ Erro ao salvar {step.output}: {str(e)}",
                    exc_info=True,
                )

        total_time = round(time.time() - start_total, 2)

        logger.info(f"[{run_id}] 🏁 END PIPELINE | tempo_total={total_time}s")

    except Exception as e:
        logger.error(
            f"[{run_id}] 💥 ERRO GERAL NO PIPELINE: {str(e)}",
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    criar_tabela_gold(max_workers=6)
