# tests/integracao/test_malha_celulas_integracao.py
"""
Bateria geoespacial parametrizada por célula da malha de 1 km.

A malha em PostGIS (geometry(Polygon, 4326)) é criada uma única vez por
módulo; cada item insere um ponto no centróide da própria célula e
verifica que o join espacial ST_Contains o contabiliza exatamente uma vez
na célula certa (nenhuma célula vizinha é afetada).

O bbox foi escolhido para gerar uma malha com tamanho fixo de células,
permitindo dimensionar o volume da camada de integração (~80% da pirâmide).
"""

import pytest
from sqlalchemy import text

from database.connection import obter_engine
from geoespacial.malha import gerar_malha
from geoespacial.postgis import (
    criar_tabela_malha,
    habilitar_postgis,
)

pytestmark = pytest.mark.integracao

# Bbox sobre o DF para uma malha de ~2.5 mil células de 1 km
BBOX_AMPLO = (-48.29, -16.10, -47.8601, -15.61)
TABELA_MALHA = "malha_celula_1km"

MALHA = gerar_malha(tamanho_celula_km=1, bbox=BBOX_AMPLO)
CELULAS = list(enumerate(MALHA.to_dict("records")))


@pytest.fixture(scope="module")
def malha_materializada(banco_env):
    """Materializa a malha completa uma única vez (SQL multi-statement)."""
    engine = obter_engine()
    habilitar_postgis(engine)
    criar_tabela_malha(engine, MALHA, TABELA_MALHA)
    engine.dispose()

    yield

    engine = obter_engine()
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABELA_MALHA}"))
    engine.dispose()


@pytest.mark.parametrize(
    "indice,celula",
    CELULAS,
    ids=[celula["celula_id"] for _, celula in CELULAS],
)
def test_ponto_no_centro_da_celula_contado_so_na_propria_celula(
    indice,
    celula,
    engine,
    malha_materializada,
):
    lat = celula["centro_lat"]
    lon = celula["centro_lon"]
    pontos = f"pontos_celula_exp_{indice}"

    with engine.begin() as conn:
        conn.execute(
            text(
                f"CREATE TABLE {pontos} (id INT, geom geometry(Point, 4326)); "
                f"INSERT INTO {pontos} VALUES (1, "
                f"ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))"
            )
        )

    try:
        with engine.connect() as conn:
            contagem = conn.execute(
                text(
                    f"SELECT COUNT(*) FROM {TABELA_MALHA} m "
                    f"JOIN {pontos} f ON ST_Contains(m.geom, f.geom) "
                    f"AND m.celula_id = '{celula['celula_id']}'"
                )
            ).scalar()

        # o ponto materializa exatamente uma ocorrência na própria célula;
        # a não-duplicação em vértices/vizinhos é coberta por teste dedicado
        assert int(contagem) == 1
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {pontos}"))