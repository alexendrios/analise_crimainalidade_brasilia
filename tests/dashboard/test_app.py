import os
from contextlib import ExitStack
from unittest.mock import patch

from streamlit.testing.v1 import AppTest

from dashboard.api_client import ApiError

APP_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "dashboard", "app.py")
)

TABELA = {"nome": "crimes_letais_gold", "disponivel_no_banco": True}

DADOS = {
    "tabela": "crimes_letais_gold",
    "total_linhas": 2,
    "total_paginas": 1,
    "registros": [
        {"ano": 2020, "regiao_administrativa": "Taguatinga", "crimes": 10},
        {"ano": 2021, "regiao_administrativa": "Ceilândia", "crimes": 20},
    ],
}

RESUMO = {"tabela": "crimes_letais_gold", "linhas": 2, "colunas": 3, "nulos_total": 0}

TABELA_IDADES = {"nome": "identificacao_crimes_contra_mulher_gold", "disponivel_no_banco": True}

DADOS_IDADES = {
    "tabela": "identificacao_crimes_contra_mulher_gold",
    "total_linhas": 2,
    "total_paginas": 1,
    "registros": [
        {"ano": 2020, "regiao_administrativa": "Taguatinga", "idade_vitima": 30, "idade_autor": 35, "crimes": 10},
        {"ano": 2021, "regiao_administrativa": "Ceilândia", "idade_vitima": 28, "idade_autor": 40, "crimes": 20},
    ],
}

PREVISAO = {
    "tabela_origem": "violencia_contra_mulher_gold",
    "coluna_alvo": "crimes_contra_mulher",
    "horizonte_anos": 2,
    "fonte_modelo": "artefato",
    "modelo_arquivo": "bundle.pkl",
    "metricas_residual": {"mae": 0.1, "rmse": 0.2},
    "previsao": [
        {"ano": 2027, "valor_previsto": 100.0, "componente_prophet": 95.0, "residual_log_aplicado": 0.05},
        {"ano": 2028, "valor_previsto": 105.0, "componente_prophet": 98.0, "residual_log_aplicado": 0.07},
    ],
}


def _pads():
    return [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.obter_previsao", return_value=PREVISAO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
        patch("dashboard.api_client.health", return_value={"status": "ok", "database": "ok"}),
    ]


def _entrar(pads):
    stack = ExitStack()
    for pad in pads:
        stack.enter_context(pad)
    return stack


def _rodar():
    return AppTest.from_file(APP_PATH, default_timeout=30).run()


def test_app_renderiza_sem_erros():
    with _entrar(_pads()):
        at = _rodar()

    assert not at.exception
    assert not at.error
    assert at.title[0].value == "Criminalidade Brasília/DF — Dashboard"


def test_app_exibe_previsao_com_metricas_e_grafico():
    with _entrar(_pads()):
        at = _rodar()

    valores = [m.value for m in at.metric]
    assert "artefato" in valores
    assert "0.1" in valores
    assert len(at.get("plotly_chart")) >= 2  # série temporal + previsão


def test_app_sem_tabelas_avisa_usuario():
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[]),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert len(at.warning) >= 1


def test_app_erro_de_api_exibe_error():
    pads = [patch("dashboard.api_client.listar_tabelas", side_effect=ApiError("API fora do ar"))]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("API fora do ar" in e.value for e in at.error)


def test_app_health_ok_ao_clicar_botao():
    with _entrar(_pads()):
        at = _rodar()
        at.sidebar.button[0].click().run()

    assert not at.exception
    assert any("API OK" in s.value for s in at.sidebar.success)


def test_app_serie_com_media_movel_renderiza():
    with _entrar(_pads()):
        at = _rodar()
        at.slider(key="serie_media_movel").set_value(3).run()

    assert not at.exception
    assert len(at.get("plotly_chart")) >= 1


def test_app_health_falha_exibe_error():
    pads = [
        patch("dashboard.api_client.health", side_effect=ApiError("API lenta")),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()
        at.sidebar.button[0].click().run()

    assert not at.exception
    assert any("API lenta" in e.value for e in at.sidebar.error)


def test_app_tabela_vazia_avisa_usuario():
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch(
            "dashboard.api_client.obter_dados",
            return_value={"tabela": "crimes_letais_gold", "total_linhas": 0, "total_paginas": 1, "registros": []},
        ),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("ainda não foi materializada" in i.value for i in at.info)


def test_app_tabela_sem_colunas_numericas_avisa_usuario():
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch(
            "dashboard.api_client.obter_dados",
            return_value={
                "tabela": "crimes_letais_gold",
                "total_linhas": 1,
                "total_paginas": 1,
                "registros": [{"ano": 2020, "regiao_administrativa": "Taguatinga"}],
            },
        ),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("não possui colunas numéricas" in i.value for i in at.info)


def test_app_paginacao_concatena_todas_as_paginas():
    pag1 = {"tabela": "crimes_letais_gold", "total_linhas": 2, "total_paginas": 2,
            "registros": [{"ano": 2020, "regiao_administrativa": "Taguatinga", "crimes": 10}]}
    pag2 = {"tabela": "crimes_letais_gold", "total_linhas": 2, "total_paginas": 2,
            "registros": [{"ano": 2021, "regiao_administrativa": "Ceilândia", "crimes": 20}]}

    def fake_dados(tabela, pagina=1, tamanho_pagina=1000, base_url=""):
        return pag1 if pagina == 1 else pag2

    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", side_effect=fake_dados),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert not any("ainda não foi materializada" in i.value for i in at.info)


def test_app_previsao_sem_pontos_informa_usuario():
    previsao_vazia = dict(PREVISAO, previsao=[])
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.obter_previsao", return_value=previsao_vazia),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("não contém pontos" in i.value for i in at.info)


def test_app_previsao_falha_exibe_error():
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.obter_previsao", side_effect=ApiError("previsão indisponível")),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("previsão indisponível" in e.value for e in at.error)


def test_app_erro_ao_listar_modelos_exibe_error():
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.obter_previsao", return_value=PREVISAO),
        patch("dashboard.api_client.listar_modelos", side_effect=ApiError("modelos indisponíveis")),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("modelos indisponíveis" in e.value for e in at.error)


def test_app_erro_ao_obter_resumo_exibe_error():
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
        patch("dashboard.api_client.obter_resumo", side_effect=ApiError("resumo indisponível")),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("resumo indisponível" in e.value for e in at.error)


def test_app_sem_ano_nem_ra_mostra_aviso_sem_grafico():
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch(
            "dashboard.api_client.obter_dados",
            return_value={
                "tabela": "crimes_letais_gold",
                "total_linhas": 1,
                "total_paginas": 1,
                "registros": [{"crimes": 10}],
            },
        ),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("exige as colunas" in w.value for w in at.warning)


def test_app_tabelas_filtra_por_ra():
    with _entrar(_pads()):
        at = _rodar()
        at.selectbox(key="tab_ra").select("Taguatinga").run()

    assert not at.exception
    assert any(df.value.shape[0] == 1 for df in at.dataframe)


def test_app_previsao_sem_modelo_arquivo_renderiza():
    previsao_sem_arquivo = {k: v for k, v in PREVISAO.items() if k != "modelo_arquivo"}
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.obter_previsao", return_value=previsao_sem_arquivo),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception


def test_app_modelos_persistidos_exibe_tabela():
    modelo = {"arquivo": "bundle.pkl", "criado_em": "2026-01-01T00:00:00",
              "tipo_modelo": "bundle", "formato_artefato": "bundle", "metricas": {"mae": 0.1, "rmse": 0.2}}
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.obter_previsao", return_value=PREVISAO),
        patch("dashboard.api_client.listar_modelos", return_value=[modelo]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("bundle.pkl" in str(df.value.values) for df in at.dataframe)


def test_app_serie_exclui_colunas_de_idade():
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA_IDADES]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS_IDADES),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    opcoes = at.selectbox(key="serie_coluna").options
    assert "Idade da vítima" not in opcoes
    assert "Idade do autor (suspeito)" not in opcoes
    assert "Crimes" in opcoes


def test_app_aba_idades_renderiza_histograma_e_resumo():
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA_IDADES]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS_IDADES),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    aba_idades = at.tabs[2]
    assert len(aba_idades.get("plotly_chart")) >= 1
    assert any("Idade da vítima" in str(df.value.values) for df in aba_idades.dataframe)


def test_app_aba_idades_sem_colunas_de_idade_avisa_usuario():
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("não possui as colunas de idade" in i.value for i in at.tabs[2].info)


def test_app_aba_idades_sem_idades_validas_avisa_usuario():
    dados_idades_zero = dict(DADOS_IDADES)
    dados_idades_zero["registros"] = [
        {"ano": 2020, "regiao_administrativa": "Taguatinga", "idade_vitima": 0, "idade_autor": 0, "crimes": 10},
        {"ano": 2021, "regiao_administrativa": "Ceilândia", "idade_vitima": 0, "idade_autor": 0, "crimes": 20},
    ]
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA_IDADES]),
        patch("dashboard.api_client.obter_dados", return_value=dados_idades_zero),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("idades válidas" in w.value for w in at.tabs[2].warning)
    assert any("Registros válidos" in str(df.value.columns) for df in at.tabs[2].dataframe)


def test_app_serie_categorica_oferece_meio_utilizado_e_motivacao():
    dados = {
        "tabela": "identificacao_crimes_contra_mulher_gold",
        "total_linhas": 2,
        "total_paginas": 1,
        "registros": [
            {"ano": 2020, "regiao_administrativa": "Taguatinga", "meio_utilizado": "ARMA DE FOGO", "motivacao": "CIUME", "crimes": 10},
            {"ano": 2021, "regiao_administrativa": "Ceilândia", "meio_utilizado": "FISICA", "motivacao": "DISCUSSAO", "crimes": 20},
        ],
    }
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA_IDADES]),
        patch("dashboard.api_client.obter_dados", return_value=dados),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()
        at.selectbox(key="serie_modo").select("Contagem por categoria").run()

    assert not at.exception
    opcoes = at.selectbox(key="serie_coluna").options
    assert "Meio utilizado" in opcoes
    assert "Motivação" in opcoes


def test_app_serie_categorica_renderiza_grafico():
    dados = {
        "tabela": "identificacao_crimes_contra_mulher_gold",
        "total_linhas": 2,
        "total_paginas": 1,
        "registros": [
            {"ano": 2020, "regiao_administrativa": "Taguatinga", "meio_utilizado": "ARMA DE FOGO", "motivacao": "CIUME", "crimes": 10},
            {"ano": 2021, "regiao_administrativa": "Ceilândia", "meio_utilizado": "FISICA", "motivacao": "DISCUSSAO", "crimes": 20},
        ],
    }
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA_IDADES]),
        patch("dashboard.api_client.obter_dados", return_value=dados),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()
        at.selectbox(key="serie_modo").select("Contagem por categoria").run()
        at.selectbox(key="serie_coluna").select("Motivação").run()

    assert not at.exception
    assert len(at.get("plotly_chart")) >= 1
