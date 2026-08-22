# geoespacial/postgis.py
"""
Integração opcional com PostGIS.

A camada degrada graciosamente (`postgis_disponivel`) e só executa
DDL/consultas espaciais quando a extensão existe. Toda a malha pode ser
espelhada no banco como `geometry(Polygon, 4326)` para consultas
ST_Intersects/ST_Contains.

O `docker-compose.yaml` já usa a imagem `postgis/postgis:16-3.4`, que traz
a extensão embutida — basta chamar `habilitar_postgis(engine)` uma vez
(requer superusuário) e materializar a malha com `criar_tabela_malha`.
"""

from sqlalchemy import text
from sqlalchemy.engine import Engine

from util.log import logs

logger = logs()

TABELA_MALHA_PADRAO = "malha_grid"

DDL_EXTENSAO = "CREATE EXTENSION IF NOT EXISTS postgis;"


def postgis_disponivel(engine: Engine) -> bool:
    """Verifica se a extensão PostGIS está disponível no servidor."""
    with engine.connect() as conn:
        registros = conn.execute(
            text("SELECT name FROM pg_available_extensions WHERE name = 'postgis'")
        ).fetchall()
    disponivel = bool(registros)
    logger.info("PostGIS disponível?", extra={"disponivel": disponivel})
    return disponivel


def habilitar_postgis(engine: Engine) -> None:
    """Cria a extensão PostGIS no banco atual (requer superusuário)."""
    with engine.begin() as conn:
        conn.execute(text(DDL_EXTENSAO))
    logger.info("Extensão PostGIS habilitada")


def criar_tabela_malha(
    engine: Engine,
    df_malha,
    tabela: str = TABELA_MALHA_PADRAO,
    recriar: bool = True,
) -> str:
    """
    Materializa a malha como tabela PostGIS (uma linha por célula).

    Gera um único bloco SQL transacional com DROP opcional, CREATE TABLE e
    INSERTs via ST_MakeEnvelope(lon_min, lat_min, lon_max, lat_max, 4326).
    """
    if not postgis_disponivel(engine):
        raise RuntimeError(
            "PostGIS não está disponível neste servidor. Instale a extensão "
            "(ex.: imagem postgis/postgis) antes de materializar a malha."
        )

    colunas_numericas = ("lon_min", "lat_min", "lon_max", "lat_max")
    faltando = [c for c in colunas_numericas if c not in df_malha.columns]
    if faltando:
        raise ValueError(f"Malha sem as colunas obrigatórias: {faltando}")

    ddl_tabela = (
        f"{'DROP TABLE IF EXISTS ' + tabela + ';' if recriar else ''} "
        f"CREATE TABLE {tabela} ("
        "celula_id TEXT PRIMARY KEY, "
        "linha INTEGER, coluna INTEGER, "
        "geom geometry(Polygon, 4326));"
    )
    inserts = "; ".join(
        (
            f"INSERT INTO {tabela} (celula_id, linha, coluna, geom) VALUES ("
            f"'{row.celula_id}', {int(row.linha)}, {int(row.coluna)}, "
            f"ST_MakeEnvelope({row.lon_min}, {row.lat_min}, "
            f"{row.lon_max}, {row.lat_max}, 4326))"
        )
        for row in df_malha.itertuples(index=False)
    )
    indice = (
        f"CREATE INDEX IF NOT EXISTS idx_{tabela}_geom ON {tabela} USING GIST (geom);"
    )
    sql_completo = f"{ddl_tabela} {inserts}; {indice}"

    with engine.begin() as conn:
        conn.execute(text(sql_completo))

    logger.info(
        "Malha materializada no PostGIS",
        extra={"tabela": tabela, "celulas": len(df_malha)},
    )
    return sql_completo


def ocorrencias_por_celula_sql(tabela_fatos: str, tabela_malha: str = TABELA_MALHA_PADRAO) -> str:
    """
    Query pronta para agregar pontos de ocorrência por célula da malha
    (join espacial ST_Contains). `tabela_fatos` deve ter geometry(lat, lon).
    """
    return (
        f"SELECT m.celula_id, COUNT(f.*) AS ocorrencias "
        f"FROM {tabela_malha} m "
        f"LEFT JOIN {tabela_fatos} f ON ST_Contains(m.geom, f.geom) "
        f"GROUP BY m.celula_id ORDER BY ocorrencias DESC;"
    )
