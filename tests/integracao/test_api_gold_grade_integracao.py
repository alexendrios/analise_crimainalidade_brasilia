# tests/integracao/test_api_gold_grade_integracao.py
"""
Matriz de integração da API gold com o PostgreSQL real, parametrizada
pelas 12 tabelas gold do catálogo (api.config.TABELAS_GOLD), além da
série temporal (coluna ano) e do filtro por Região Administrativa.

Cada item exercita o caminho TestClient(app) -> routers.gold ->
gold_service -> Repository -> Postgres real, sem mocks.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from api.config import COLUNA_ANO_POR_TABELA, TABELAS_GOLD
from api.main import app
from database.repository import repository as repo
from ingestion.repository_adapter import Repository
from tests.integracao.dados_gold import df_da_gold

pytestmark = pytest.mark.integracao

TABELAS = list(TABELAS_GOLD)
TABELAS_COM_ANO = sorted(COLUNA_ANO_POR_TABELA)
TABELAS_COM_RA = sorted(
    tabela
    for tabela in TABELAS
    if "regiao_administrativa" in df_da_gold(tabela).columns
)

client = TestClient(app)


@pytest.fixture
def tabela_materializada(engine, request):
    """Limpa e recalcula uma tabela gold no banco real a partir dos dados fiéis."""
    tabela = request.param
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tabela}"))
    Repository.save(df_da_gold(tabela), tabela)
    return tabela


@pytest.mark.parametrize("tabela_materializada", TABELAS, indirect=True)
def test_api_resumo_por_tabela(engine, tabela_materializada):
    resposta = client.get(f"/gold/{tabela_materializada}/resumo")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["tabela"] == tabela_materializada
    assert corpo["linhas"] == len(df_da_gold(tabela_materializada))
    assert corpo["nulos_total"] == 0
    assert corpo["colunas_com_nulos"] == 0


@pytest.mark.parametrize("tabela_materializada", TABELAS, indirect=True)
def test_api_dados_por_tabela(engine, tabela_materializada):
    resposta = client.get(f"/gold/{tabela_materializada}/dados")

    assert resposta.status_code == 200
    corpo = resposta.json()
    esperado = df_da_gold(tabela_materializada)
    assert corpo["tabela"] == tabela_materializada
    assert corpo["total_linhas"] == len(esperado)
    assert set(corpo["registros"][0]) >= set(esperado.columns)
    assert len(corpo["registros"]) == len(esperado)


@pytest.mark.parametrize("tabela_materializada", TABELAS, indirect=True)
def test_api_dados_defaults_de_paginacao_por_tabela(engine, tabela_materializada):
    resposta = client.get(f"/gold/{tabela_materializada}/dados")

    corpo = resposta.json()
    assert corpo["pagina"] == 1
    assert corpo["tamanho_pagina"] == 50
    assert corpo["total_paginas"] == 1
    assert len(corpo["registros"]) == len(df_da_gold(tabela_materializada))


@pytest.mark.parametrize("tabela_materializada", TABELAS, indirect=True)
def test_api_dados_ultima_pagina_por_tabela(engine, tabela_materializada):
    resposta = client.get(
        f"/gold/{tabela_materializada}/dados",
        params={"tamanho_pagina": 1, "pagina": 2},
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    total = len(df_da_gold(tabela_materializada))
    assert corpo["tamanho_pagina"] == 1
    assert corpo["total_paginas"] == total
    assert corpo["pagina"] == min(2, total)
    assert len(corpo["registros"]) == (1 if total >= 2 else 1)


@pytest.mark.parametrize("tabela_materializada", TABELAS, indirect=True)
def test_api_dados_pagina_acima_do_limite_por_tabela(engine, tabela_materializada):
    resposta = client.get(
        f"/gold/{tabela_materializada}/dados",
        params={"tamanho_pagina": 1, "pagina": 999},
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["pagina"] == corpo["total_paginas"]


@pytest.mark.parametrize("tabela", TABELAS)
def test_api_resumo_tabela_nao_materializada_503(engine, tabela):
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tabela}"))

    resposta = client.get(f"/gold/{tabela}/resumo")

    assert resposta.status_code == 503


@pytest.mark.parametrize("tabela", TABELAS)
def test_api_dados_tabela_nao_materializada_503(engine, tabela):
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tabela}"))

    resposta = client.get(f"/gold/{tabela}/dados")

    assert resposta.status_code == 503


@pytest.mark.parametrize("tabela_materializada", TABELAS, indirect=True)
def test_api_resumo_consistente_com_repositorio(engine, tabela_materializada):
    resposta_api = client.get(f"/gold/{tabela_materializada}/resumo").json()
    resumo_repo = repo.analisar_tabela(tabela_materializada)

    assert resposta_api["linhas"] == resumo_repo["linhas"]
    assert resposta_api["colunas"] == resumo_repo["colunas"]
    assert resposta_api["nulos_total"] == resumo_repo["nulos_total"]


@pytest.mark.parametrize("tabela", TABELAS_COM_ANO)
def test_api_dados_filtro_de_ano_por_tabela(engine, tabela):
    df = df_da_gold(tabela)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tabela}"))
    Repository.save(df, tabela)
    ano = int(df["ano"].iloc[0])

    resposta = client.get(f"/gold/{tabela}/dados", params={"ano_min": ano, "ano_max": ano})

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["total_linhas"] == int((df["ano"] == ano).sum())
    assert all(registro["ano"] == ano for registro in corpo["registros"])


@pytest.mark.parametrize("tabela", TABELAS_COM_RA)
def test_api_dados_filtro_regiao_case_insensitive_por_tabela(engine, tabela):
    df = df_da_gold(tabela)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tabela}"))
    Repository.save(df, tabela)
    ra = df["regiao_administrativa"].iloc[0]

    resposta = client.get(
        f"/gold/{tabela}/dados",
        params={"regiao_administrativa": ra.lower()},
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["total_linhas"] == int((df["regiao_administrativa"].str.upper() == ra.upper()).sum())
    assert all(registro["regiao_administrativa"].upper() == ra.upper() for registro in corpo["registros"])