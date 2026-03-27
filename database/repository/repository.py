# database/repository/repository.py

import time
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from database.connection import obter_engine
from util.log import logs

logger = logs()

_ENGINE: Engine | None = None


def conectar_banco() -> Engine:
    """
    Retorna instância singleton do SQLAlchemy Engine.
    """
    global _ENGINE

    if _ENGINE is None:
        logger.info("♻️ Criando engine de conexão com o banco")
        _ENGINE = obter_engine()

    return _ENGINE

def carregar_tabela(nome_tabela: str) -> pd.DataFrame | None:
    """
    Carrega todos os registros de uma tabela.
    """
    logger.info("📥 Iniciando carga da tabela '%s'", nome_tabela)

    if not nome_tabela.isidentifier():
        logger.error("🚫 Nome de tabela inválido: '%s'", nome_tabela)
        return None

    _ENGINE = conectar_banco()
    inicio = time.perf_counter()

    try:
        with _ENGINE.connect() as conn:
            df = pd.read_sql(
                text(f"SELECT * FROM {nome_tabela}"),
                conn,
            )

        duracao = time.perf_counter() - inicio

        logger.info(
            "✅ Tabela '%s' carregada (%s linhas) em %.2fs",
            nome_tabela,
            len(df),
            duracao,
        )

        return df

    except SQLAlchemyError:
        logger.exception("🔥 Erro SQL ao carregar a tabela '%s'", nome_tabela)
        return None

    except Exception:
        logger.exception("🔥 Erro inesperado ao carregar a tabela '%s'", nome_tabela)
        return None

def inserir_dados(dataframe: pd.DataFrame, tabela: str) -> None:
    """
    Insere dados na tabela com estratégia FULL REFRESH.
    Sempre apaga e recria a tabela ao rodar a aplicação.
    """
    
    logger.info(
        "💾 Iniciando FULL REFRESH da tabela '%s' (%s linhas)",
        tabela,
        len(dataframe),
    )

    if not tabela.isidentifier():
        logger.error("🚫 Nome de tabela inválido: '%s'", tabela)
        return
    _ENGINE = conectar_banco()
    inicio = time.perf_counter()

    try:
        df = dataframe.copy()

        # Controle temporal UTC timezone-aware
        df["inserido_em"] = pd.Timestamp.now(tz="UTC")

        logger.debug("🕒 Coluna 'inserido_em' adicionada com timestamp UTC")

        # Transação segura
        with _ENGINE.begin() as conn:
            df.to_sql(
                tabela,
                conn,
                if_exists="replace",  # 🔥 recria tabela sempre
                index=False,
                method="multi",
                chunksize=5000,
            )

        duracao = time.perf_counter() - inicio

        logger.info(
            "✅ Tabela '%s' recriada com sucesso (%s linhas) em %.2fs",
            tabela,
            len(df),
            duracao,
        )

    except SQLAlchemyError:
        logger.exception(
            "🔥 Erro SQL ao inserir dados na tabela '%s'",
            tabela,
        )
        raise

    except Exception:
        logger.exception(
            "🔥 Erro inesperado ao inserir dados na tabela '%s'",
            tabela,
        )
        raise
    
def listar_tabelas():
    logger.info("Listando tabelas do banco de dados")

    engine = None

    try:
        engine = obter_engine()
        inspector = inspect(engine)

        tabelas = inspector.get_table_names()

        logger.info(f"{len(tabelas)} tabelas encontradas")

        return tabelas

    except Exception:
        logger.exception("Erro ao listar tabelas do banco")
        raise

    finally:
        if engine:
            engine.dispose()
            logger.info("Conexão com banco encerrada")
            
def analisar_tabela(nome_tabela):
    logger.info(f"[START] Análise da tabela: {nome_tabela}")
    inicio = time.time()

    try:
        df = carregar_tabela(nome_tabela)

        linhas, colunas = df.shape
        logger.info(
            f"Tabela carregada: {nome_tabela} | linhas={linhas} | colunas={colunas}"
        )

        # Tipos de dados
        logger.debug(f"Tipos de dados:\n{df.dtypes}")

        # Nulos
        nulos_por_coluna = df.isnull().sum()
        total_nulos = nulos_por_coluna.sum()

        logger.info(f"Total de nulos: {total_nulos}")
        logger.debug(f"Nulos por coluna:\n{nulos_por_coluna}")

        # Estatísticas
        try:
            describe = df.describe(include="all")
            logger.debug(f"Estatísticas descritivas:\n{describe}")
        except Exception:
            logger.warning("Não foi possível gerar describe completo")

        # Amostra de dados
        logger.debug(f"Primeiras linhas:\n{df.head()}")

        # Insights rápidos (nível engenharia de dados)
        colunas_nulas = nulos_por_coluna[nulos_por_coluna > 0].count()
        logger.info(f"Colunas com nulos: {colunas_nulas}")

        colunas_numericas = df.select_dtypes(include="number").columns.tolist()
        logger.info(f"Colunas numéricas: {len(colunas_numericas)}")

        tempo = round(time.time() - inicio, 2)
        logger.info(f"[END] {nome_tabela} analisada em {tempo}s")

        return {
            "tabela": nome_tabela,
            "linhas": linhas,
            "colunas": colunas,
            "nulos_total": int(total_nulos),
            "colunas_com_nulos": int(colunas_nulas),
            "tempo_execucao_s": tempo,
        }

    except Exception:
        logger.exception(f"[ERROR] Falha ao analisar tabela: {nome_tabela}")
        raise

def resumo_tabelas():

    logger.info("Iniciando geração do resumo das tabelas")

    inicio = time.time()
    resultados = []

    try:
        tabelas = listar_tabelas()

        logger.info(f"{len(tabelas)} tabelas encontradas para análise")

        for i, tabela in enumerate(tabelas, start=1):
            logger.info(f"[{i}/{len(tabelas)}] Processando tabela: {tabela}")

            try:
                df = carregar_tabela(tabela)

                linhas = df.shape[0]
                colunas = df.shape[1]
                nulos = df.isnull().sum().sum()

                resultados.append(
                    {
                        "tabela": tabela,
                        "linhas": linhas,
                        "colunas": colunas,
                        "valores_nulos_total": nulos,
                    }
                )

                logger.info(
                    f"Tabela {tabela} analisada | linhas={linhas} colunas={colunas} nulos={nulos}"
                )

            except Exception:
                logger.exception(f"Erro ao processar tabela: {tabela}")

        df_resumo = pd.DataFrame(resultados)

        logger.info("Resumo das tabelas gerado com sucesso")

        return df_resumo

    except Exception:
        logger.exception("Erro na geração do resumo das tabelas")
        raise

    finally:
        tempo = round(time.time() - inicio, 2)
        logger.info(f"Tempo total da análise: {tempo}s")