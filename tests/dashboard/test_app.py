import os
from contextlib import ExitStack
from unittest.mock import patch

import pytest
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

CLASSIFICACAO = {
    "tabelas_origem": ["crimes_letais_gold", "populacao_regiao_administrativa"],
    "total_registros": 4,
    "total_ras": 2,
    "periodo": [2023, 2024],
    "limiar_taxa_mediana": 10.66,
    "distribuicao_real": {"alta": 2, "baixa": 2},
    "metricas": {
        "cv_roc_auc_media": 0.99,
        "cv_roc_auc_std": 0.01,
        "holdout_accuracy": 1.0,
        "holdout_precision": 1.0,
        "holdout_recall": 1.0,
        "holdout_f1": 1.0,
        "holdout_roc_auc": 1.0,
    },
    "odds_ratios": {"taxa_homicidio": 199.4, "ano_num": 0.88},
    "matriz_confusao": [[2, 0], [0, 2]],
    "fonte_modelo": "artefato",
    "modelo_arquivo": "logreg_criminalidade_letal.pkl",
    "classificacoes": [
        {"regiao_administrativa": "Taguatinga", "ano": 2024,
         "classe_prevista": 1, "rotulo_previsto": "alta", "probabilidade_alta": 0.92},
        {"regiao_administrativa": "Ceilândia", "ano": 2024,
         "classe_prevista": 0, "rotulo_previsto": "baixa", "probabilidade_alta": 0.21},
        {"regiao_administrativa": "Taguatinga", "ano": 2023,
         "classe_prevista": 1, "rotulo_previsto": "alta", "probabilidade_alta": 0.88},
        {"regiao_administrativa": "Ceilândia", "ano": 2023,
         "classe_prevista": 0, "rotulo_previsto": "baixa", "probabilidade_alta": 0.35},
    ],
}


def _pads():
    return [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.obter_previsao", return_value=PREVISAO),
        patch("dashboard.api_client.obter_classificacao", return_value=CLASSIFICACAO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
        patch("dashboard.api_client.health", return_value={"status": "ok", "database": "ok"}),
    ]


@pytest.fixture(autouse=True)
def _cache_da_tabela_limpo():
    """O AppTest executa app.py como módulo próprio, então a limpeza precisa
    ser global: st.cache_data.clear() alcança o decorator usado pelo script."""
    import streamlit as st

    st.cache_data.clear()
    yield
    st.cache_data.clear()


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


def test_app_visao_geral_exclui_tabelas_nao_sumarizaveis():
    tabelas = [
        TABELA,
        TABELA_IDADES,
        {"nome": "desaparecidos_idade_sexo_gold", "disponivel_no_banco": True},
        {"nome": "desaparecidos_localizados_gold", "disponivel_no_banco": True},
    ]
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=tabelas)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    opcoes = at.tabs[0].selectbox[0].options
    assert "Identificação crimes contra mulher" not in opcoes
    assert "Desaparecidos — por idade e sexo" not in opcoes
    assert "Desaparecidos — localizados" not in opcoes
    assert "Crimes letais" in opcoes


def test_app_series_exclui_tabelas_nao_serie_temporal():
    tabelas = [
        TABELA,
        {"nome": "violencia_idosos_gold", "disponivel_no_banco": True},
        {"nome": "violencia_idosos_mensais_gold", "disponivel_no_banco": True},
        {"nome": "violencia_idosos_sexo_gold", "disponivel_no_banco": True},
        {"nome": "desaparecidos_regiao_gold", "disponivel_no_banco": True},
        {"nome": "desaparecidos_idade_sexo_gold", "disponivel_no_banco": True},
        {"nome": "desaparecidos_localizados_gold", "disponivel_no_banco": True},
    ]
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=tabelas)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    opcoes = at.tabs[1].selectbox[0].options
    assert "Violência contra idosos" not in opcoes
    assert "Violência contra idosos — série mensal" not in opcoes
    assert "Violência contra idosos — por sexo" not in opcoes
    assert "Desaparecidos — por RA" not in opcoes
    assert "Desaparecidos — por idade e sexo" not in opcoes
    assert "Desaparecidos — localizados" not in opcoes
    assert "Crimes letais" in opcoes


def test_app_mapa_exclui_tabelas_sem_regiao():
    tabelas = [
        TABELA,
        TABELA_IDADES,
        {"nome": "violencia_idosos_gold", "disponivel_no_banco": True},
        {"nome": "violencia_idosos_mensais_gold", "disponivel_no_banco": True},
        {"nome": "violencia_idosos_sexo_gold", "disponivel_no_banco": True},
        {"nome": "desaparecidos_regiao_gold", "disponivel_no_banco": True},
        {"nome": "desaparecidos_idade_sexo_gold", "disponivel_no_banco": True},
        {"nome": "desaparecidos_localizados_gold", "disponivel_no_banco": True},
    ]
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=tabelas)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    opcoes = at.tabs[2].selectbox[0].options
    assert "Violência contra idosos" not in opcoes
    assert "Violência contra idosos — série mensal" not in opcoes
    assert "Violência contra idosos — por sexo" not in opcoes
    assert "Desaparecidos — por RA" not in opcoes
    assert "Desaparecidos — por idade e sexo" not in opcoes
    assert "Desaparecidos — localizados" not in opcoes
    assert "Identificação crimes contra mulher" not in opcoes
    assert "Crimes letais" in opcoes


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
    aba_idades = at.tabs[3]
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
    assert any("não possui as colunas de idade" in i.value for i in at.tabs[3].info)


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
    assert any("idades válidas" in w.value for w in at.tabs[3].warning)
    assert any("Registros válidos" in str(df.value.columns) for df in at.tabs[3].dataframe)


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


def test_app_serie_categorica_sem_colunas_categoricas_avisa_usuario():
    """Tabela só com colunas numéricas + RA: modo categórico não tem o que mostrar."""
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
    ]
    with _entrar(pads):
        at = _rodar()
        at.selectbox(key="serie_modo").select("Contagem por categoria").run()

    assert not at.exception
    assert any(
        "colunas categóricas" in info.value for info in at.info
    )


def test_app_mapa_ranking_sem_dados_avisa_usuario():
    from dashboard.visualizacoes import SemDadosParaGraficoError

    with _entrar(_pads()):
        at = _rodar()
        with patch(
            "dashboard.visualizacoes.figura_ranking_ra",
            side_effect=SemDadosParaGraficoError("sem dados para o ranking"),
        ):
            at.selectbox(key="mapa_coluna").select("crimes").run()

    assert not at.exception
    assert any("sem dados para o ranking" in w.value for w in at.warning)


def test_app_importado_fora_do_windows_nao_aplica_politica_e_nao_executa_main(monkeypatch):
    """Cobre os ramos de módulo: sys.platform != 'win32' (política não aplicada)
    e __name__ != '__main__' (main() não executada). Recarrega e restaura."""
    import importlib
    import sys

    import dashboard.app as modulo_app

    monkeypatch.setattr(sys, "platform", "linux")
    try:
        recarregado = importlib.reload(modulo_app)
        assert callable(recarregado.main)
    finally:
        monkeypatch.undo()
        importlib.reload(modulo_app)


def test_app_aba_classificacao_renderiza_graficos_metricas_e_tabela():
    with _entrar(_pads()):
        at = _rodar()

    assert not at.exception
    aba = at.tabs[5]
    valores = [m.value for m in aba.metric]
    assert "artefato" in valores  # fonte do modelo
    assert any("10.66" == str(v) for v in valores)  # limiar da mediana
    assert len(aba.get("plotly_chart")) == 2  # ranking + heatmap
    assert any("Taguatinga" in str(df.value.values) for df in aba.dataframe)
    assert any("Taxa de homicídio" in str(df.value.values) for df in aba.dataframe)


def test_app_aba_classificacao_ranking_respeita_ano_selecionado():
    with _entrar(_pads()):
        at = _rodar()
        # anos ordenados desc: [2024, 2023]; seleciona o ano 2023
        at.selectbox(key="classe_ano").select(2023).run()

    assert not at.exception


def test_app_aba_classificacao_sem_classificacoes_informa_usuario():
    payload_vazio = dict(CLASSIFICACAO, classificacoes=[])
    pads = _pads()
    pads[4] = patch("dashboard.api_client.obter_classificacao", return_value=payload_vazio)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("não contém classificações" in i.value for i in at.tabs[5].info)


def test_app_aba_classificacao_falha_exibe_error():
    pads = _pads()
    pads[4] = patch(
        "dashboard.api_client.obter_classificacao",
        side_effect=ApiError("classificação indisponível"),
    )
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("classificação indisponível" in e.value for e in at.tabs[5].error)


def test_app_aba_classificacao_sem_metricas_holdout_renderiza():
    metricas_parciais = {
        k: v for k, v in CLASSIFICACAO["metricas"].items()
        if k not in ("holdout_roc_auc", "holdout_f1", "cv_roc_auc_std")
    }
    payload = dict(CLASSIFICACAO, metricas=metricas_parciais)
    pads = _pads()
    pads[4] = patch("dashboard.api_client.obter_classificacao", return_value=payload)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
