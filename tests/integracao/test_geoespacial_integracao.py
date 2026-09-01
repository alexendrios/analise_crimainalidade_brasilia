# tests/integracao/test_geoespacial_integracao.py
"""
Integração da camada geoespacial (geoespacial.postgis) com PostGIS real:
disponibilidade da extensão, materialização da malha como tabela espacial
(geometry POLYGON/4326) e agregação de ocorrências por célula via
ST_Contains.
"""

import pandas as pd
import pytest
from sqlalchemy import text

from geoespacial.malha import gerar_malha
from geoespacial.postgis import (
    criar_tabela_malha,
    habilitar_postgis,
    ocorrencias_por_celula_sql,
    postgis_disponivel,
)

pytestmark = pytest.mark.integracao

# Bounding box pequeno para manter a malha com poucas células
BBOX_TESTE = (-48.0, -15.9, -47.9, -15.8)

TABELA_MALHA = "malha_grid_test"
TABELA_PONTOS = "pontos_ocorrencia_test"


def _malha_pequena():
    return gerar_malha(tamanho_celula_km=5, bbox=BBOX_TESTE)


def _criar_tabela_pontos(engine, pontos):
    """Cria a tabela de fatos espaciais com uma linha por (lat, lon)."""
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABELA_PONTOS}"))
        conn.execute(text(f"CREATE TABLE {TABELA_PONTOS} (id INT, geom geometry(Point, 4326))"))
        for i, (lat, lon) in enumerate(pontos):
            conn.execute(
                text(
                    f"INSERT INTO {TABELA_PONTOS} VALUES ({i}, "
                    f"ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))"
                )
            )


def _contagem_por_celula(engine):
    sql = ocorrencias_por_celula_sql(TABELA_PONTOS, TABELA_MALHA)
    with engine.connect() as conn:
        linhas = conn.execute(text(sql)).fetchall()
    return {celula: int(total) for celula, total in linhas}


def test_postgis_disponivel_eh_verdadeiro(engine):
    assert postgis_disponivel(engine) is True


def test_habilitar_postgis_cria_extensao(engine):
    habilitar_postgis(engine)

    with engine.connect() as conn:
        existe = conn.execute(
            text("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
        ).scalar()

    assert existe == 1


def test_criar_tabela_malha_materializa_geometrias(engine):
    malha = _malha_pequena()

    criada = criar_tabela_malha(engine, malha, TABELA_MALHA)

    assert "CREATE TABLE" in criada
    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM {TABELA_MALHA}")).scalar()
        srid = conn.execute(
            text(
                f"SELECT ST_SRID(geom) FROM {TABELA_MALHA} "
                "ORDER BY linha, coluna LIMIT 1"
            )
        ).scalar()
        tipo = conn.execute(
            text(f"SELECT GeometryType(geom) FROM {TABELA_MALHA} LIMIT 1")
        ).scalar()

    assert total == len(malha)
    assert srid == 4326
    assert tipo.upper() == "POLYGON"


def test_criar_tabela_malha_recriar_substitui_dados(engine):
    malha_1 = _malha_pequena()
    malha_2 = malha_1.copy().iloc[:-1]  # uma célula a menos

    criar_tabela_malha(engine, malha_1, TABELA_MALHA)
    criar_tabela_malha(engine, malha_2, TABELA_MALHA)

    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM {TABELA_MALHA}")).scalar()

    assert total == len(malha_2)


def test_criar_tabela_malha_valida_colunas_obrigatorias(engine):
    malha_sem_lon_min = _malha_pequena().drop(columns=["lon_min"])

    with pytest.raises(ValueError, match="colunas obrigatórias"):
        criar_tabela_malha(engine, malha_sem_lon_min, TABELA_MALHA)


def test_ocorrencias_por_celula_join_espacial_real(engine):
    malha = _malha_pequena()
    criar_tabela_malha(engine, malha, TABELA_MALHA)

    centros = malha.set_index("celula_id")
    pontos = [
        (centros.loc["R001C001", "centro_lat"], centros.loc["R001C001", "centro_lon"]),
        (centros.loc["R001C001", "centro_lat"], centros.loc["R001C001", "centro_lon"]),
        (centros.loc["R002C002", "centro_lat"], centros.loc["R002C002", "centro_lon"]),
    ]
    _criar_tabela_pontos(engine, pontos)

    ocorrencias = _contagem_por_celula(engine)

    assert sum(ocorrencias.values()) == len(pontos)
    assert ocorrencias["R001C001"] == 2
    assert ocorrencias["R002C002"] == 1


def test_habilitar_postgis_idempotente(engine):
    habilitar_postgis(engine)
    habilitar_postgis(engine)

    with engine.connect() as conn:
        existe = conn.execute(
            text("SELECT COUNT(*) FROM pg_extension WHERE extname = 'postgis'")
        ).scalar()

    assert existe == 1


def test_criar_tabela_malha_celula_ids_unicos(engine):
    malha = _malha_pequena()
    criar_tabela_malha(engine, malha, TABELA_MALHA)

    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM {TABELA_MALHA}")).scalar()
        distintos = conn.execute(
            text(f"SELECT COUNT(DISTINCT celula_id) FROM {TABELA_MALHA}")
        ).scalar()

    assert total == len(malha) == distintos


def test_criar_tabela_malha_geometrias_validas_e_nao_vazias(engine):
    criar_tabela_malha(engine, _malha_pequena(), TABELA_MALHA)

    with engine.connect() as conn:
        invalidas = conn.execute(
            text(
                f"SELECT COUNT(*) FROM {TABELA_MALHA} "
                "WHERE NOT ST_IsValid(geom) OR ST_IsEmpty(geom)"
            )
        ).scalar()

    assert invalidas == 0


def test_criar_tabela_malha_envelope_dentro_do_bbox(engine):
    lon_min, lat_min, lon_max, lat_max = BBOX_TESTE
    criar_tabela_malha(engine, _malha_pequena(), TABELA_MALHA)

    with engine.connect() as conn:
        fora = conn.execute(
            text(
                f"SELECT COUNT(*) FROM {TABELA_MALHA} "
                f"WHERE ST_XMin(geom) < {lon_min} - 1e-6 "
                f"OR ST_XMax(geom) > {lon_max} + 1e-6 "
                f"OR ST_YMin(geom) < {lat_min} - 1e-6 "
                f"OR ST_YMax(geom) > {lat_max} + 1e-6"
            )
        ).scalar()

    assert fora == 0


def test_criar_tabela_malha_preserva_precisao_dos_limites(engine):
    malha = _malha_pequena()
    criar_tabela_malha(engine, malha, TABELA_MALHA)
    celula = malha.sort_values(["linha", "coluna"]).iloc[0]
    celula_id = celula["celula_id"]

    with engine.connect() as conn:
        x_min = conn.execute(
            text(f"SELECT ST_XMin(geom) FROM {TABELA_MALHA} WHERE celula_id = '{celula_id}'")
        ).scalar()

    assert round(x_min, 6) == celula["lon_min"]


def test_criar_tabela_malha_recriar_false_nao_destroi_existente(engine):
    criar_tabela_malha(engine, _malha_pequena(), TABELA_MALHA)

    import sqlalchemy.exc  # noqa: PLC0415

    with pytest.raises(sqlalchemy.exc.ProgrammingError):
        criar_tabela_malha(engine, _malha_pequena(), TABELA_MALHA, recriar=False)

    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM {TABELA_MALHA}")).scalar()

    assert total == len(_malha_pequena())


def test_ocorrencias_por_celula_varios_pontos_na_mesma_celula(engine):
    malha = _malha_pequena()
    criar_tabela_malha(engine, malha, TABELA_MALHA)
    centros = malha.set_index("celula_id")
    celula = centros.loc["R000C000"]
    pontos = [(celula["centro_lat"], celula["centro_lon"])] * 3
    _criar_tabela_pontos(engine, pontos)

    ocorrencias = _contagem_por_celula(engine)

    assert ocorrencias["R000C000"] == 3
    assert sum(ocorrencias.values()) == 3


def test_ocorrencias_por_celula_ponto_no_vertice_nao_eh_duplo_contado(engine):
    malha = _malha_pequena()
    criar_tabela_malha(engine, malha, TABELA_MALHA)
    celula = malha.sort_values(["linha", "coluna"]).iloc[1]  # vértice compartilhado
    _criar_tabela_pontos(engine, [(celula["lat_min"], celula["lon_min"])])

    ocorrencias = _contagem_por_celula(engine)

    assert sum(ocorrencias.values()) == 0


def test_ocorrencias_por_celula_pontos_fora_do_bbox_nao_contam(engine):
    malha = _malha_pequena()
    criar_tabela_malha(engine, malha, TABELA_MALHA)
    lon_min, lat_min, lon_max, lat_max = BBOX_TESTE
    fora = [(lat_min - 0.5, lon_min - 0.5), (lat_max + 0.5, lon_max + 0.5)]
    _criar_tabela_pontos(engine, fora)

    ocorrencias = _contagem_por_celula(engine)

    assert sum(ocorrencias.values()) == 0
    assert set(ocorrencias) == set(malha["celula_id"])  # todas as células retornadas


def test_ocorrencias_por_celula_tabela_de_fatos_vazia(engine):
    criar_tabela_malha(engine, _malha_pequena(), TABELA_MALHA)
    _criar_tabela_pontos(engine, [])

    ocorrencias = _contagem_por_celula(engine)

    assert set(ocorrencias) == set(_malha_pequena()["celula_id"])
    assert all(total == 0 for total in ocorrencias.values())