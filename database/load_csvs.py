# database/load_csvs.py

from database.connection import obter_engine, close_engine
from database.repository.repository import inserir_dados
from util.log import logs
import pandas as pd
import time


logger = logs()

arquivos = {
    "./data/silver/output/crimes-contra-mulher_tratado.csv": "crimes_contra_mulher",
    "./data/silver/output/desaparecidos_idade_sexo_tratado.csv": "desaparecidos_idade_sexo",
    "./data/silver/output/desaparecimento-localizados_tratado.csv": "desaparecimento_localizados",
    "./data/silver/output/desaparecimento-regiao_tratado.csv": "desaparecimento_regiao",
    "./data/silver/output/df_analise_populacao.csv": "populacao_df",
    "./data/silver/output/feminicidio_tratado.csv": "feminicidio",
    "./data/silver/output/furto_em_veiculo_tratado.csv": "furto_em_veiculo",
    "./data/silver/output/homicidio_tratado.csv": "homicidio",
    "./data/silver/output/idosos_2016_tratado.csv": "violencia_idosos",
    "./data/silver/output/idosos_mensais_tratado.csv": "violencia_idosos_mensais",
    "./data/silver/output/idosos_tabela4.csv": "violencia_idosos_ocorrencias",
    "./data/silver/output/idosos_tabela5.csv": "violencia_idosos_sexo",
    "./data/silver/output/injuria_racial_tratado.csv": "injuria_racial",
    "./data/silver/output/latrocinio_tratado.csv": "latrocinio",
    "./data/silver/output/lesao_corporal_morte_tratada.csv": "lesao_corporal_morte",
    "./data/silver/output/ra_df_populacao_tratado.csv": "populacao_regiao_administrativa",
    "./data/silver/output/racismo_tratado.csv": "racismo",
    "./data/silver/output/roubo_comercio.csv": "roubo_comercio",
    "./data/silver/output/roubo_transporte_coletivo_tratado.csv": "roubo_transporte_coletivo",
    "./data/silver/output/roubo_veiculo_tratado.csv": "roubo_veiculo",
    "./data/silver/output/roubo-a-transeunte_tratado.csv": "roubo_pedestre",
}


def salvar_tabela():
    logger.info("🚀 Iniciando processo de carga dos arquivos CSV.")

    for arquivo, tabela in arquivos.items():
        inicio = time.time()

        try:
            logger.info(f"📄 Lendo arquivo: {arquivo}")

            df = pd.read_csv(arquivo, sep=";", encoding="utf-8")

            logger.info(
                f"✅ Arquivo lido com sucesso | "
                f"Linhas: {len(df)} | Colunas: {len(df.columns)}"
            )

            inserir_dados(df, tabela)

            tempo_execucao = round(time.time() - inicio, 2)

            logger.info(
                f"📊 Arquivo processado | Tabela: {tabela} | Tempo: {tempo_execucao}s"
            )

        except pd.errors.ParserError as e:
            logger.error(f"❌ Erro de parsing no arquivo {arquivo}: {e}", exc_info=True)

        except Exception as e:
            logger.error(
                f"❌ Erro inesperado ao processar {arquivo}: {e}", exc_info=True
            )

    logger.info("🏁 Processo de carga finalizado.")

