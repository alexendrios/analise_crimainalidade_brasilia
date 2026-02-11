# database/repository/repository.py
import time
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from database.connection import obter_engine
from util.log import logs

logger = logs()

_ENGINE = None


def conectar_banco():
    """Retorna SQLAlchemy engine (singleton)."""
    global _ENGINE
    if _ENGINE is None:
        logger.info("♻️ Criando engine de conexão com o banco")
        _ENGINE = obter_engine()
    return _ENGINE


def carregar_tabela(nome_tabela: str) -> pd.DataFrame | None:
    logger.info("📥 Iniciando carga da tabela '%s'", nome_tabela)

    if not nome_tabela.isidentifier():
        logger.error("🚫 Nome de tabela inválido: '%s'", nome_tabela)
        return None

    engine = conectar_banco()
    inicio = time.perf_counter()

    try:
        df = pd.read_sql(text(f"SELECT * FROM {nome_tabela}"), engine)

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


def salvar_tabela(df: pd.DataFrame, nome_tabela: str) -> None:
    logger.info(
        "💾 Iniciando gravação da tabela '%s' (%s linhas)",
        nome_tabela,
        len(df),
    )

    if not nome_tabela.isidentifier():
        logger.error("🚫 Nome de tabela inválido: '%s'", nome_tabela)
        return

    engine = conectar_banco()
    inicio = time.perf_counter()

    try:
        df.to_sql(
            nome_tabela,
            engine,
            if_exists="replace",
            index=False,
            method="multi",
        )

        duracao = time.perf_counter() - inicio
        logger.info(
            "✅ Tabela '%s' salva (%s linhas) em %.2fs",
            nome_tabela,
            len(df),
            duracao,
        )

    except SQLAlchemyError:
        logger.exception("🔥 Erro SQL ao salvar a tabela '%s'", nome_tabela)

    except Exception:
        logger.exception("🔥 Erro inesperado ao salvar a tabela '%s'", nome_tabela)
