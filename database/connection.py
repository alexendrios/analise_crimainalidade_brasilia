# database/connection.py
import os
from util.log import logs
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# -------------------------------------------------------------------
# Configuração de logging
# -------------------------------------------------------------------
logger = logs()

# -------------------------------------------------------------------
# Carrega variáveis de ambiente
# -------------------------------------------------------------------
load_dotenv()


def obter_engine():
    """
    Cria e retorna um engine SQLAlchemy configurado para PostgreSQL.
    """
    logger.info("🔌 Iniciando criação do engine de banco de dados")

    DB_USER = os.getenv("POSTGRES_USERNAME")
    DB_PASSWORD_RAW = os.getenv("POSTGRES_PASSWORD")
    DB_HOST = os.getenv("POSTGRES_HOST")
    DB_PORT = os.getenv("POSTGRES_PORT")
    DB_NAME = os.getenv("POSTGRES_DB")
    REQUISITOS_SSL = os.getenv("REQUISITOS_SSL")

    # Validação das variáveis obrigatórias
    if not all([DB_USER, DB_PASSWORD_RAW, DB_HOST, DB_PORT, DB_NAME, REQUISITOS_SSL]):
        logger.error("❌ Variáveis de ambiente do banco estão incompletas")
        raise EnvironmentError(
            "Variáveis de ambiente do banco estão incompletas. Verifique o arquivo .env"
        )

    # Codifica senha (sem logar o valor)
    DB_PASSWORD = quote_plus(DB_PASSWORD_RAW)

    logger.info(
        "📄 Variáveis de ambiente carregadas com sucesso "
        "(usuário=%s, host=%s, porta=%s, database=%s)",
        DB_USER,
        DB_HOST,
        DB_PORT,
        DB_NAME,
    )

    DATABASE_URL = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}{REQUISITOS_SSL}"
    )

    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        logger.info("✅ Engine SQLAlchemy criado com sucesso")
        return engine

    except SQLAlchemyError as exc:
        logger.exception("🔥 Erro ao criar o engine SQLAlchemy")
        raise exc
