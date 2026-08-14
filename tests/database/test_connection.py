import pytest
from unittest.mock import MagicMock
from sqlalchemy.exc import SQLAlchemyError

import database.connection as connection

@pytest.fixture
def env_valido(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "postgres")
    monkeypatch.setenv("POSTGRES_PASSWORD", "senha@123")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "test_db")
    monkeypatch.setenv("REQUISITOS_SSL", "?sslmode=disable")


def test_obter_engine_sucesso(env_valido, monkeypatch):
    engine_mock = MagicMock()

    def fake_create_engine(url, pool_pre_ping):
        assert "postgresql+psycopg2://" in url
        assert "senha%40123" in url  # senha codificada
        assert pool_pre_ping is True
        return engine_mock

    monkeypatch.setattr(connection, "create_engine", fake_create_engine)

    engine = connection.obter_engine()

    assert engine == engine_mock


def test_obter_engine_variaveis_incompletas(monkeypatch):
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.delenv("POSTGRES_PORT", raising=False)
    monkeypatch.delenv("POSTGRES_DB", raising=False)
    monkeypatch.delenv("REQUISITOS_SSL", raising=False)

    with pytest.raises(EnvironmentError) as exc:
        connection.obter_engine()

    assert "Variáveis de ambiente do banco estão incompletas" in str(exc.value)

def test_obter_engine_erro_sqlalchemy(env_valido, monkeypatch):
    def fake_create_engine(*args, **kwargs):
        raise SQLAlchemyError("Falha ao criar engine")

    monkeypatch.setattr(connection, "create_engine", fake_create_engine)

    with pytest.raises(SQLAlchemyError):
        connection.obter_engine()

def test_logs_de_criacao_engine(env_valido, monkeypatch, caplog):
    engine_mock = MagicMock()

    monkeypatch.setattr(
        connection,
        "create_engine",
        lambda *args, **kwargs: engine_mock,
    )

    with caplog.at_level("INFO"):
        connection.obter_engine()

    mensagens = [r.message for r in caplog.records]

    assert any("Iniciando criação do engine" in m for m in mensagens)
    assert any("Engine SQLAlchemy criado com sucesso" in m for m in mensagens)


def test_close_engine_com_engine_ativo_faz_dispose(monkeypatch):
    engine_mock = MagicMock()
    monkeypatch.setattr(connection, "_engine", engine_mock)

    connection.close_engine()

    engine_mock.dispose.assert_called_once()
    assert connection._engine is None


def test_close_engine_sem_engine_loga_warning(monkeypatch, caplog):
    monkeypatch.setattr(connection, "_engine", None)

    with caplog.at_level("WARNING"):
        connection.close_engine()

    mensagens = [r.message for r in caplog.records]
    assert any("já está encerrado ou não foi criado" in m for m in mensagens)
