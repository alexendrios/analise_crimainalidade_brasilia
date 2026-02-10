# src/pipeline.py
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
from pathlib import Path
from util.log import logs
import time

logger = logs()


def log_tempo_inicio(func_name):
    logger.info("========== INÍCIO: %s ==========", func_name)
    start = time.time()
    return start


def log_tempo_fim(func_name, start_time):
    fim = time.time()
    logger.info(
        "========== FIM: %s | tempo: %.2f seg ==========", func_name, fim - start_time
    )


def main():
    pipeline_start = log_tempo_inicio("Pipeline Completo")

    try:
        # Coleta de dados
        start = log_tempo_inicio("Coleta de Dados")
        coletar_dados_()
        arquivos_zip_execucao()
        log_tempo_fim("Coleta de Dados", start)

        # Processamento de população
        start = log_tempo_inicio("Processamento População")
        processar_populacao()
        obter_dados_ra_populacao()
        analisar_populacao()
        log_tempo_fim("Processamento População", start)

        # Região administrativa
        start = log_tempo_inicio("Tratamento População RA")
        tratar_populacao_regiao_administrativa(
            "./data/csv/ra_df_populacao.csv",
            "./data/output/ra_df_populacao_tratado.csv",
        )
        log_tempo_fim("Tratamento População RA", start)
        
        # Processamento de crimes
        start = log_tempo_inicio("Processamento Crimes")
        caminho_planilhas = Path("./data/planilha")
        caminho_saida = Path("./data/csv")
        caminho_saida.mkdir(parents=True, exist_ok=True)  # garante que a pasta exista
        processar_crimes(caminho_planilhas, caminho_saida)
        log_tempo_fim("Processamento Crimes", start)
        
        # Crimes contra mulher
        start = log_tempo_inicio("Crimes contra Mulher")
        tratar_crimes_contra_mulher(
            "./data/csv/crimes-contra-mulher.csv",
            "./data/output/crimes-contra-mulher_tratado.csv",
        )
        log_tempo_fim("Crimes contra Mulher", start)

        # Feminicídio
        start = log_tempo_inicio("Feminicídio")
        tratar_feminicidio(
            "./data/csv/feminicidio.csv", "./data/output/feminicidio.csv"
        )
        log_tempo_fim("Feminicídio", start)

        # Desaparecidos
        for desc in [
            (
                "desaparecimento-idade-sexo.csv",
                "desaparecidos_idade_sexo_tratado.csv",
                tratar_desaparecidos_idade_sexo,
            ),
            (
                "desaparecimento-localizados.csv",
                "desaparecimento-localizados_tratado.csv",
                tratar_desaparecidos_localizados,
            ),
            (
                "desaparecimento-regiao.csv",
                "desaparecimento-regiao_tratado.csv",
                tratar_desaparecidos_regiao,
            ),
        ]:
            start = log_tempo_inicio(f"Tratamento {desc[0]}")
            desc[2](f"./data/csv/{desc[0]}", f"./data/output/{desc[1]}")
            log_tempo_fim(f"Tratamento {desc[0]}", start)

        # Furto e roubo
        for arquivo_entrada, arquivo_saida, func in [
            (
                "furto-em-veiculo.csv",
                "furto_em_veiculo_tratado.csv",
                tratar_furto_veiculo,
            ),
            ("homicidio.csv", "homicidio_tratado.csv", tratar_homicidio),
            # correto:
            (
                "idosos_7_anos.csv",
                [
                    "./data/output/idosos_tabela4.csv",
                    "./data/output/idosos_tabela5.csv",
                ],
                tratar_violencia_idosos,
            ),
            (
                "idosos_2016.csv",
                "idosos_2016_tratado.csv",
                tratar_crimes_idosos_ranking,
            ),
            (
                "idosos_mensais.csv",
                "idosos_mensais_tratado.csv",
                lambda inp, out: crimes_idosos_por_mes(inp, ["registro", "fato"], out),
            ),
            (
                "injuria-racial.csv",
                "injuria_racial_tratado.csv",
                tratar_injuria_racial_por_regiao,
            ),
            ("latrocinio.csv", "latrocinio_tratado.csv", tratar_latrocinio_por_regiao),
            (
                "lesao-corporal-morte.csv",
                "lesao_corporal_morte_tratada.csv",
                tratar_lesao_corporal_morte_por_regiao,
            ),
            (
                "lesao-corporal-morte.csv",
                "lesao_corporal_morte_tratada.csv",
                tratar_lesao_corporal_morte,
            ),
            ("racismo.csv", "racismo_tratado.csv", tratar_racismo),
            (
                "roubo-a-transeunte.csv",
                "roubo-a-transeunte_tratado.csv",
                tratar_roubo_pedestre,
            ),
            ("roubo-de-veiculo.csv", "roubo_veiculo_tratado.csv", tratar_roubo_veiculo),
            ("roubo-em-comercio.csv", "roubo_comercio.csv", roubo_comercio),
            (
                "roubo-em-transporte-coletivo.csv",
                "roubo_transporte_coletivo_tratado.csv",
                roubo_transporte_coletivo,
            ),
        ]:
            start = log_tempo_inicio(f"Tratamento {arquivo_entrada}")
    
            # Se for lista ou tupla, passa direto
            if isinstance(arquivo_saida, (list, tuple)):
                caminhos_saida = arquivo_saida
            else:
                caminhos_saida = f"./data/output/{arquivo_saida}"
            
            func(f"./data/csv/{arquivo_entrada}", caminhos_saida)
            log_tempo_fim(f"Tratamento {arquivo_entrada}", start)

        logger.info("Pipeline finalizado com sucesso!")

    except Exception as e:
        logger.exception("Erro durante a execução do pipeline: %s", e)

    log_tempo_fim("Pipeline Completo", pipeline_start)


if __name__ == "__main__":
    main()
