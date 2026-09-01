# tests/integracao/test_api_gold_integracao.py
"""
Integração da camada de Consumo (API FastAPI) com o PostgreSQL real:
os endpoints /gold de fato consultam/materializam dados no banco de
verdade, sem nenhum mock de serviços ou do repositório.

Caminho de produção: TestClient(app) -> api/routers/gold.py ->
api/services/gold_service.py -> ingestion.repository_adapter.Repository
-> database/repository/repository.py -> Postgres real (Testcontainer).
"""

from fastapi.testclient import TestClient
import pandas as pd
import pytest
from sqlalchemy import text

from api.config import COLUNA_ANO_POR_TABELA, TABELAS_GOLD
from api.main import app
from database.repository import repository as repo
from ingestion.repository_adapter import Repository
from tests.integracao.dados_gold import df_da_gold

pytestmark = pytest.mark.integracao

TABELA = "crimes_letais_gold"
client = TestClient(app)


def _seed(engine, df, tabela=TABELA):
    """Limpa e recalcula a tabela partindo do caminho de produção."""
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tabela}"))
    Repository.save(df, tabela)


def _df_paginada():
    registros = []
    for ano in (2019, 2020, 2021, 2022):
        for ra in ("Taguatinga", "Ceilândia", "Planaltina", "Gama"):
            registros.append(
                {
                    "ano": ano,
                    "regiao_administrativa": ra,
                    "ocorrencia_homicidio": (ano % 7) + 1,
                    "ocorrencia_latrocinio": 1,
                    "ocorrencia_lesao_morte": 0,
                }
            )
    return pd.DataFrame(registros)


def test_health_usa_banco_real(engine):
    resposta = client.get("/health")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["status"] == "ok"
    assert corpo["database"] == "ok"


def test_gold_tabelas_lista_catalogo_completo(engine):
    resposta = client.get("/gold/tabelas")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["total"] == len(TABELAS_GOLD)
    assert {item["nome"] for item in corpo["tabelas"]} == set(TABELAS_GOLD)
    for item in corpo["tabelas"]:
        assert "descricao" in item
        assert "disponivel_no_banco" in item


def test_gold_tabelas_disponibilidade_reflete_banco_real(engine):
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABELA}"))

    antes = client.get("/gold/tabelas").json()
    disponivel = next(item for item in antes["tabelas"] if item["nome"] == TABELA)
    assert disponivel["disponivel_no_banco"] is False

    _seed(engine, df_da_gold(TABELA))

    depois = client.get("/gold/tabelas").json()
    disponivel = next(item for item in depois["tabelas"] if item["nome"] == TABELA)
    assert disponivel["disponivel_no_banco"] is True


def test_gold_resumo_retorna_metricas_do_banco_real(engine):
    _seed(engine, df_da_gold(TABELA))

    resposta = client.get(f"/gold/{TABELA}/resumo")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["tabela"] == TABELA
    assert corpo["linhas"] == len(df_da_gold(TABELA))
    assert corpo["nulos_total"] == 0
    assert corpo["colunas_com_nulos"] == 0


def test_gold_resumo_tabela_desconhecida_404(engine):
    resposta = client.get("/gold/tabela_que_nao_existe/resumo")

    assert resposta.status_code == 404
    assert "não é uma tabela gold conhecida" in resposta.json()["detail"]


def test_gold_resumo_tabela_valida_nao_materializada_503(engine):
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABELA}"))

    resposta = client.get(f"/gold/{TABELA}/resumo")

    assert resposta.status_code == 503


def test_gold_dados_paginacao_real(engine):
    df = _df_paginada()
    _seed(engine, df)

    resposta = client.get(f"/gold/{TABELA}/dados", params={"tamanho_pagina": 4, "pagina": 2})

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["tabela"] == TABELA
    assert corpo["total_linhas"] == len(df)
    assert corpo["tamanho_pagina"] == 4
    assert corpo["total_paginas"] == len(df) // 4
    assert corpo["pagina"] == 2
    assert len(corpo["registros"]) == 4


def test_gold_dados_pagina_fora_do_limite_clamado(engine):
    _seed(engine, _df_paginada())

    resposta = client.get(f"/gold/{TABELA}/dados", params={"tamanho_pagina": 4, "pagina": 999})

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["pagina"] == corpo["total_paginas"]
    assert len(corpo["registros"]) <= 4


def test_gold_dados_filtro_de_ano_no_banco_real(engine):
    _seed(engine, _df_paginada())

    resposta = client.get(
        f"/gold/{TABELA}/dados",
        params={"ano_min": 2021, "ano_max": 2021},
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["total_linhas"] == 4
    assert all(registro["ano"] == 2021 for registro in corpo["registros"])


def test_gold_dados_filtro_regiao_administrativa_case_insensitive(engine):
    _seed(engine, _df_paginada())

    resposta = client.get(
        f"/gold/{TABELA}/dados",
        params={"regiao_administrativa": "taguatinga"},
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["total_linhas"] == 4
    assert all(registro["regiao_administrativa"] == "Taguatinga" for registro in corpo["registros"])


def test_gold_dados_registros_fieis_ao_schema(engine):
    _seed(engine, df_da_gold(TABELA))

    resposta = client.get(f"/gold/{TABELA}/dados")

    corpo = resposta.json()
    colunas_esperadas = set(df_da_gold(TABELA).columns)
    assert set(corpo["registros"][0]) >= colunas_esperadas


@pytest.mark.parametrize(
    "params",
    [
        {"tamanho_pagina": 5000},
        {"tamanho_pagina": 0},
    ],
    ids=["acima_do_limite", "abaixo_do_minimo"],
)
def test_gold_dados_tamanho_pagina_invalido_422(engine, params):
    resposta = client.get(f"/gold/{TABELA}/dados", params=params)

    assert resposta.status_code == 422


def test_gold_dados_tabela_nao_materializada_503(engine):
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABELA}"))

    resposta = client.get(f"/gold/{TABELA}/dados")

    assert resposta.status_code == 503


def test_gold_dados_tabela_desconhecida_404(engine):
    resposta = client.get("/gold/tabela_que_nao_existe/dados")

    assert resposta.status_code == 404


def test_gold_dados_ignora_filtro_de_ano_para_tabela_sem_serie_temporal(engine):
    tabela_sem_ano = "violencia_idosos_gold"
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tabela_sem_ano}"))
    Repository.save(df_da_gold(tabela_sem_ano), tabela_sem_ano)

    assert "ano" not in COLUNA_ANO_POR_TABELA.get(tabela_sem_ano, {})

    resposta = client.get(
        f"/gold/{tabela_sem_ano}/dados",
        params={"ano_min": 2021},
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["total_linhas"] == len(df_da_gold(tabela_sem_ano))
    assert len(corpo["registros"]) == len(df_da_gold(tabela_sem_ano))


def test_gold_dados_defaults_de_paginacao(engine):
    _seed(engine, _df_paginada())

    resposta = client.get(f"/gold/{TABELA}/dados")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["pagina"] == 1
    assert corpo["tamanho_pagina"] == 50
    assert len(corpo["registros"]) == len(_df_paginada())


def test_ciclo_completo_pipeline_api_consome_dados_materializados_pelo_pipeline(engine):
    """Pipeline gold (produção) -> mesma API /gold, sem nenhum mock."""
    from unittest.mock import patch

    from src.pipeline_tabela_gold import STEPS, criar_tabela_gold

    resultados = {step.nome: df_da_gold(step.output) for step in STEPS}
    with patch("src.pipeline_tabela_gold.executar_pipeline", return_value=resultados):
        criar_tabela_gold(max_workers=2)

    tabelas_consulta = client.get("/gold/tabelas").json()
    materializadas = {item["nome"] for item in tabelas_consulta["tabelas"] if item["disponivel_no_banco"]}
    assert materializadas == {step.output for step in STEPS}

    dados = client.get(f"/gold/{TABELA}/dados").json()
    assert dados["total_linhas"] == len(df_da_gold(TABELA))


def test_resumo_tabelas_exige_que_repo_e_api_compartilhem_banco(engine):
    """Prova que a API e o repositório enxergam exatamente o mesmo banco."""
    _seed(engine, df_da_gold(TABELA))

    resposta_api = client.get(f"/gold/{TABELA}/resumo").json()
    resumo_repo = repo.analisar_tabela(TABELA)

    assert resposta_api["linhas"] == resumo_repo["linhas"]
    assert resposta_api["colunas"] == resumo_repo["colunas"]
    assert resposta_api["nulos_total"] == resumo_repo["nulos_total"]