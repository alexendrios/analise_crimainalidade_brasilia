# database/repository/repository.py

import time
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
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
    

