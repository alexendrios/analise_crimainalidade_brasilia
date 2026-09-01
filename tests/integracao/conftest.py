# tests/integracao/conftest.py
"""
Fixtures da camada de integração.

Sobe um PostgreSQL real (imagem PostGIS, mesma do docker-compose) via
Testcontainers, aponta as variáveis de ambiente consumidas por
`database.connection.obter_engine()` para o container e garante que os
singletons de engine sejam descartados entre testes.

Se Docker/Testcontainers não estiver disponível, toda a camada é
pulada com uma mensagem explícita (permite rodar só os testes rápidos
sem stack Docker).
"""

import os

import pytest
from sqlalchemy.engine import make_url

IMAGEM_POSTGIS = "postgis/postgis:16-3.4"

_USUARIO = "criminalidade_user"
_SENHA = "criminalidade_pass"
_BANCO = "criminalidade_db"

# Mesmas chaves lidas por database/connection.py
_CHAVES_AMBIENTE = (
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "REQUISITOS_SSL",
)


@pytest.fixture(scope="session")
def postgres_container():
    """Container PostgreSQL/PostGIS real (uma instância por session)."""
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(
        IMAGEM_POSTGIS,
        username=_USUARIO,
        password=_SENHA,
        dbname=_BANCO,
    )

    try:
        container.start()
    except Exception as exc:  # noqa: BLE001 - Docker indisponível no ambiente
        pytest.skip(
            f"Docker/Testcontainers indisponível — camada de integração pulada. "
            f"Detalhe: {exc}"
        )

    yield container
    container.stop()


@pytest.fixture(scope="session")
def banco_env(postgres_container):
    """Configura o ambiente consumido por obter_engine() para o container."""
    url = make_url(postgres_container.get_connection_url())

    valores = {
        "POSTGRES_USER": url.username,
        "POSTGRES_PASSWORD": url.password,
        "POSTGRES_HOST": url.host,
        "POSTGRES_PORT": str(url.port),
        "POSTGRES_DB": url.database,
        "REQUISITOS_SSL": "?sslmode=disable",
    }

    anteriores = {chave: os.environ.get(chave) for chave in _CHAVES_AMBIENTE}
    os.environ.update(valores)

    yield

    for chave in _CHAVES_AMBIENTE:
        if anteriores[chave] is None:
            os.environ.pop(chave, None)
        else:
            os.environ[chave] = anteriores[chave]


@pytest.fixture(scope="function")
def engine(banco_env):
    """
    Engine SQLAlchemy real criado pelo caminho de produção
    (database.connection.obter_engine). Usado pelas funções que recebem
    o engine como parâmetro (ex.: geoespacial.postgis).
    """
    from database.connection import obter_engine

    conexao = obter_engine()
    yield conexao
    conexao.dispose()


@pytest.fixture(autouse=True)
def _resetar_singletons():
    """
    Garante que cada teste comece com engine limpo: descarta os pools
    do módulo database.connection e database.repository.repository.
    """
    from database import connection
    from database.repository import repository

    if connection._engine is not None:
        connection._engine.dispose()
        connection._engine = None
    if repository._ENGINE is not None:
        repository._ENGINE.dispose()
        repository._ENGINE = None

    yield

    if connection._engine is not None:
        connection._engine.dispose()
        connection._engine = None
    if repository._ENGINE is not None:
        repository._ENGINE.dispose()
        repository._ENGINE = None