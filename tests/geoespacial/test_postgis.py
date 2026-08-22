import pandas as pd
import pytest

from geoespacial.malha import gerar_malha
from geoespacial.postgis import (
    DDL_EXTENSAO,
    criar_tabela_malha,
    ocorrencias_por_celula_sql,
    postgis_disponivel,
)


class _ResultadoFake:
    def __init__(self, linhas):
        self._linhas = linhas

    def fetchall(self):
        return self._linhas


class _ConexaoFake:
    """Context manager de conexão: responde pg_available_extensions e captura DDL."""

    def __init__(self, captura, postgis_ok):
        self._captura = captura
        self._postgis_ok = postgis_ok

    def execute(self, sql):
        comando = str(sql)
        if "pg_available_extensions" in comando:
            linhas = [("postgis",)] if self._postgis_ok else []
            return _ResultadoFake(linhas)
        self._captura.append(comando)
        return None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _EngineFake:
    def __init__(self, postgis_ok=True):
        self._postgis_ok = postgis_ok
        self.captura = []

    def connect(self):
        return _ConexaoFake(self.captura, self._postgis_ok)

    def begin(self):
        return _ConexaoFake(self.captura, self._postgis_ok)


# ============================================================
# postgis_disponivel
# ============================================================
def test_postgis_disponivel_true_quando_extensao_existe():
    assert postgis_disponivel(_EngineFake(postgis_ok=True)) is True


def test_postgis_disponivel_false_quando_extensao_ausente():
    assert postgis_disponivel(_EngineFake(postgis_ok=False)) is False


# ============================================================
# criar_tabela_malha
# ============================================================
def test_criar_tabela_malha_sem_postgis_levanta_runtime_error(monkeypatch):
    monkeypatch.setattr("geoespacial.postgis.postgis_disponivel", lambda engine: False)
    malha = gerar_malha(tamanho_celula_km=5.0)

    with pytest.raises(RuntimeError, match="PostGIS não está disponível"):
        criar_tabela_malha(_EngineFake(), malha)


def test_criar_tabela_malha_executa_ddl_com_st_makeenvelope(monkeypatch):
    monkeypatch.setattr("geoespacial.postgis.postgis_disponivel", lambda engine: True)
    malha = gerar_malha(tamanho_celula_km=11.0, bbox=(-48.0, -16.0, -47.8, -15.8))

    engine = _EngineFake()
    criar_tabela_malha(engine, malha, tabela="malha_teste")

    sql_executado = "\n".join(engine.captura)
    assert "CREATE TABLE malha_teste" in sql_executado
    assert "geometry(Polygon, 4326)" in sql_executado
    assert "ST_MakeEnvelope" in sql_executado
    assert sql_executado.count("INSERT INTO malha_teste") == len(malha)
    assert "USING GIST" in sql_executado


def test_criar_tabela_malha_sem_colunas_levanta_valueerror(monkeypatch):
    monkeypatch.setattr("geoespacial.postgis.postgis_disponivel", lambda engine: True)
    with pytest.raises(ValueError, match="colunas obrigatórias"):
        criar_tabela_malha(_EngineFake(), pd.DataFrame({"x": [1]}))


def test_ocorrencias_por_celula_monta_join_espacial():
    query = ocorrencias_por_celula_sql("fatos", "malha_x")
    assert "FROM malha_x" in query
    assert "LEFT JOIN fatos" in query
    assert "ST_Contains(m.geom, f.geom)" in query


def test_ddl_extensao_e_idempotente():
    assert DDL_EXTENSAO == "CREATE EXTENSION IF NOT EXISTS postgis;"
