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

DADOS_DESAPARECIDOS_IDADE_SEXO = {
    "tabela": "desaparecidos_idade_sexo_gold",
    "total_linhas": 4,
    "total_paginas": 1,
    "registros": [
        {"ano": 2020, "faixa_etaria": "0 A 17 ANOS", "sexo": "MASCULINO", "quantidade": 6},
        {"ano": 2020, "faixa_etaria": "0 A 17 ANOS", "sexo": "FEMININO", "quantidade": 4},
        {"ano": 2020, "faixa_etaria": "18 A 29 ANOS", "sexo": "MASCULINO", "quantidade": 10},
        {"ano": 2020, "faixa_etaria": "18 A 29 ANOS", "sexo": "FEMININO", "quantidade": 8},
    ],
}

DADOS_DESAPARECIDOS_LOCALIZADOS = {
    "tabela": "desaparecidos_localizados_gold",
    "total_linhas": 2,
    "total_paginas": 1,
    "registros": [
        {"ano": 2021, "faixa_etaria": "0 A 17 ANOS", "status": "LOCALIZADOS", "quantidade": 30},
        {"ano": 2021, "faixa_etaria": "0 A 17 ANOS", "status": "AINDA DESAPARECIDOS", "quantidade": 12},
    ],
}

DADOS_DESAPARECIDOS_REGIAO = {
    "tabela": "desaparecidos_regiao_gold",
    "total_linhas": 2,
    "total_paginas": 1,
    "registros": [
        {"regiao_administrativa": "Ceilândia", "ocorrencias_2020": 100, "ocorrencias_2021": 120},
        {"regiao_administrativa": "Taguatinga", "ocorrencias_2020": 50, "ocorrencias_2021": 60},
    ],
}

DADOS_IDOSOS_RESUMO = {
    "tabela": "violencia_idosos_gold",
    "total_linhas": 2,
    "total_paginas": 1,
    "registros": [
        {"ranking": 1, "regiao_administrativa": "GAMA", "jan_ago_2016": 88, "jan_ago_2017": 51},
        {"ranking": 2, "regiao_administrativa": "BRASILIA", "jan_ago_2016": 21, "jan_ago_2017": 43},
    ],
}

DADOS_IDOSOS_OCORRENCIAS = {
    "tabela": "violencia_idosos_ocorrencias_gold",
    "total_linhas": 2,
    "total_paginas": 1,
    "registros": [
        {"ano": 2010, "ocorrencias": 55, "violencia_dentro_de_casa": 22},
        {"ano": 2011, "ocorrencias": 63, "violencia_dentro_de_casa": 25},
    ],
}

DADOS_IDOSOS_MENSAIS = {
    "tabela": "violencia_idosos_mensais_gold",
    "total_linhas": 2,
    "total_paginas": 1,
    "registros": [
        {"ano": 2016, "mes": "JAN", "mes_num": 1, "fato": 54, "registro": 59},
        {"ano": 2016, "mes": "FEV", "mes_num": 2, "fato": 26, "registro": 31},
    ],
}

DADOS_IDOSOS_SEXO = {
    "tabela": "violencia_idosos_sexo_gold",
    "total_linhas": 2,
    "total_paginas": 1,
    "registros": [
        {"ano": 2010, "masculino": 36, "feminino": 34},
        {"ano": 2011, "masculino": 32, "feminino": 49},
    ],
}


def _obter_dados_por_tabela(tabela, *args, **kwargs):
    mapeamento = {
        "desaparecidos_idade_sexo_gold": DADOS_DESAPARECIDOS_IDADE_SEXO,
        "desaparecidos_localizados_gold": DADOS_DESAPARECIDOS_LOCALIZADOS,
        "desaparecidos_regiao_gold": DADOS_DESAPARECIDOS_REGIAO,
        "violencia_idosos_gold": DADOS_IDOSOS_RESUMO,
        "violencia_idosos_ocorrencias_gold": DADOS_IDOSOS_OCORRENCIAS,
        "violencia_idosos_mensais_gold": DADOS_IDOSOS_MENSAIS,
        "violencia_idosos_sexo_gold": DADOS_IDOSOS_SEXO,
    }
    return mapeamento.get(tabela, DADOS)

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

CORRELACOES = {
    "metodo": "pearson",
    "periodo": [2016, 2024],
    "indicadores": ["roubo_pedestre", "homicidio"],
    "matriz_correlacao": {
        "roubo_pedestre": {"roubo_pedestre": 1.0, "homicidio": 0.8},
        "homicidio": {"roubo_pedestre": 0.8, "homicidio": 1.0},
    },
    "serie_historica": [
        {"ano": 2016, "roubo_pedestre": 100, "homicidio": 10},
        {"ano": 2024, "roubo_pedestre": 200, "homicidio": 20},
    ],
    "pares_destaque": [
        {"indicador_a": "roubo_pedestre", "indicador_b": "homicidio", "correlacao": 0.8},
    ],
    "insights": [
        "'Roubo a pedestre' e 'Homicídio' têm correlação positiva forte (+0.80)."
    ],
}

GRANGER = {
    "max_lag": 1,
    "alpha": 0.05,
    "total_pares": 1,
    "total_significantes": 1,
    "pares": [
        {"origem": "roubo_pedestre", "destino": "homicidio",
         "melhor_lag": 1, "p_valor": 0.01, "significante": True},
    ],
}

ANOMALIAS = {
    "total_painel": 1,
    "total_mensal": 1,
    "painel": [
        {"regiao_administrativa": "Ceilândia", "ano": 2021,
         "ocorrencia_roubo_pedestre": 900, "lag_1": 500.0,
         "diff_1": 400.0, "media_movel_3": 480.0},
    ],
    "mensal": [
        {"ano": 2017, "mes": "DEZ", "mes_num": 12, "fato": 120,
         "registro": 130, "lag_1": 40.0, "diff_1": 80.0, "media_movel_3": 45.0},
    ],
}

ZONAS_QUENTES = {
    "ano_referencia": 2024,
    "tamanho_celula_km": 1.5,
    "celulas_com_ocorrencias": 2,
    "zonas": [
        {"celula_id": "R010C005", "ocorrencia_roubo_pedestre": 120},
        {"celula_id": "R002C001", "ocorrencia_roubo_pedestre": 60},
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
        patch("dashboard.api_client.obter_correlacoes", return_value=CORRELACOES),
        patch("dashboard.api_client.obter_granger", return_value=GRANGER),
        patch("dashboard.api_client.obter_anomalias", return_value=ANOMALIAS),
        patch("dashboard.api_client.obter_zonas_quentes", return_value=ZONAS_QUENTES),
        patch("dashboard.ia_client.listar_modelos_ollama", return_value=["modelo-local"]),
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
    # 60s: com a aba de violência contra idosos o app consulta mais tabelas,
    # e alguns testes deixam chamadas reais passarem quando a API está no ar.
    return AppTest.from_file(APP_PATH, default_timeout=60).run()


def test_app_renderiza_sem_erros():
    with _entrar(_pads()):
        at = _rodar()

    assert not at.exception
    assert not at.error
    assert at.title[0].value == "Criminalidade em Brasília/DF — Dashboard Analítico"


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


def test_app_visao_geral_exibe_estatisticas_e_boxplot():
    with _entrar(_pads()):
        at = _rodar()

    assert not at.exception
    aba = at.tabs[0]
    rotulos = [m.label for m in aba.metric]
    for esperado in ("Média", "Mediana", "Mínimo", "Máximo", "Desvio padrão"):
        assert esperado in rotulos
    # por RA: Taguatinga 10 e Ceilândia 20 → média 15 e mediana 15
    valores = {m.label: m.value for m in aba.metric}
    assert valores["Média"] == "15"
    assert valores["Mediana"] == "15"
    assert len(aba.get("plotly_chart")) == 1
    assert any("outliers" in i.value.lower() for i in aba.info)


def test_app_visao_geral_avisa_sobre_ra_outlier():
    dados_outlier = {
        "tabela": "crimes_letais_gold",
        "total_linhas": 8,
        "total_paginas": 1,
        "registros": [
            {"ano": 2020, "regiao_administrativa": "Taguatinga", "crimes": 10},
            {"ano": 2021, "regiao_administrativa": "Taguatinga", "crimes": 10},
            {"ano": 2020, "regiao_administrativa": "Ceilândia", "crimes": 20},
            {"ano": 2021, "regiao_administrativa": "Ceilândia", "crimes": 20},
            {"ano": 2020, "regiao_administrativa": "Gama", "crimes": 30},
            {"ano": 2021, "regiao_administrativa": "Gama", "crimes": 30},
            {"ano": 2020, "regiao_administrativa": "Brasília", "crimes": 150},
            {"ano": 2021, "regiao_administrativa": "Brasília", "crimes": 150},
        ],
    }
    pads = list(_pads())
    pads[1] = patch("dashboard.api_client.obter_dados", return_value=dados_outlier)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    avisos = " ".join(w.value for w in at.tabs[0].warning)
    assert "Brasília" in avisos
    assert "outlier" in avisos.lower()


def test_app_aba_resumo_geral_renderiza_controles():
    with _entrar(_pads()):
        at = _rodar()

    assert not at.exception
    aba = at.tabs[14]
    assert aba.subheader[0].value == "Resumo Geral (IA)"
    assert aba.text_input[0].label == "URL do Ollama"
    assert aba.text_input[0].value == "http://localhost:11434"
    assert "modelo-local" in aba.selectbox[0].options
    assert any(b.label == "Gerar resumo com IA" for b in aba.button)
    assert any("Clique" in i.value for i in aba.info)


def test_app_aba_resumo_geral_gera_resumo_com_sucesso():
    pads = list(_pads()) + [
        patch(
            "dashboard.ia_client.gerar_resumo_ia",
            return_value="# Panorama\n- Dados OK",
        ),
        patch(
            "dashboard.contexto_ia.montar_contexto_ia",
            return_value="DADOS SINTETIZADOS",
        ),
    ]
    with _entrar(pads):
        at = _rodar()
        at.tabs[14].button[0].click().run()

    aba = at.tabs[14]
    assert not at.exception
    assert any("# Panorama" in md.value for md in aba.markdown)


def test_app_aba_resumo_geral_falha_exibe_error():
    from dashboard.ia_client import OllamaError

    pads = list(_pads()) + [
        patch(
            "dashboard.ia_client.gerar_resumo_ia",
            side_effect=OllamaError("Ollama fora do ar"),
        ),
        patch(
            "dashboard.contexto_ia.montar_contexto_ia",
            return_value="CONTEXTO",
        ),
    ]
    with _entrar(pads):
        at = _rodar()
        at.tabs[14].button[0].click().run()

    assert not at.exception
    assert any("Ollama fora do ar" in e.value for e in at.tabs[14].error)


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


def test_app_aba_mancha_criminal_renderiza_mapa_e_ranking():
    with _entrar(_pads()):
        at = _rodar()

    assert not at.exception
    aba = at.tabs[3]
    assert len(aba.get("plotly_chart")) == 1
    # recorte padrão é o ano mais recente (2021): RA mais crítica é Ceilândia
    metricas = {m.label: m.value for m in aba.metric}
    assert "Ceilândia" in metricas
    assert "20" in list(metricas.values())


def test_app_aba_mancha_criminal_avisa_quando_nenhuma_ra_tem_centroide():
    dados_sem_ra = {
        "tabela": "crimes_letais_gold",
        "total_linhas": 1,
        "total_paginas": 1,
        "registros": [
            {"ano": 2020, "regiao_administrativa": "ATLANTIDA", "crimes": 10},
        ],
    }
    pads = list(_pads())
    pads[1] = patch("dashboard.api_client.obter_dados", return_value=dados_sem_ra)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    aba = at.tabs[3]
    assert any("centróide cadastrado" in w.value for w in aba.warning)
    assert len(aba.get("plotly_chart")) == 0


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
        patch("dashboard.api_client.listar_tabelas", return_value=[TABELA]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
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
    aba_idades = at.tabs[4]
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
    assert any("não possui as colunas de idade" in i.value for i in at.tabs[4].info)


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
    assert any("idades válidas" in w.value for w in at.tabs[4].warning)
    assert any("Registros válidos" in str(df.value.columns) for df in at.tabs[4].dataframe)


def test_app_identificacao_crimes_nao_oferece_seletor_de_tabela():
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=[TABELA])
    pads[1] = patch("dashboard.api_client.obter_dados", return_value=DADOS_IDADES)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    aba = at.tabs[4]
    assert not any("Tabela gold" == s.label for s in aba.selectbox)
    assert len(aba.get("plotly_chart")) >= 1


def test_app_aba_desaparecidos_renderiza_quatro_graficos():
    pads = list(_pads())
    pads[1] = patch("dashboard.api_client.obter_dados", side_effect=_obter_dados_por_tabela)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    aba = at.tabs[5]
    assert len(aba.get("plotly_chart")) == 4
    assert not aba.warning


def test_app_aba_desaparecidos_sem_tabelas_informa_usuario():
    pads = list(_pads())
    pads[1] = patch(
        "dashboard.api_client.obter_dados",
        side_effect=lambda tabela, *args, **kwargs: {
            "tabela": tabela,
            "total_linhas": 0,
            "total_paginas": 1,
            "registros": [],
        },
    )
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any(
        "Nenhuma tabela de desaparecidos foi materializada" in i.value
        for i in at.tabs[5].info
    )


def test_app_aba_violencia_idosos_renderiza_quatro_graficos():
    pads = list(_pads())
    pads[1] = patch("dashboard.api_client.obter_dados", side_effect=_obter_dados_por_tabela)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    aba = at.tabs[6]
    assert len(aba.get("plotly_chart")) == 4
    assert not aba.warning


def test_app_aba_violencia_idosos_sem_tabelas_informa_usuario():
    pads = list(_pads())
    pads[1] = patch(
        "dashboard.api_client.obter_dados",
        side_effect=lambda tabela, *args, **kwargs: {
            "tabela": tabela,
            "total_linhas": 0,
            "total_paginas": 1,
            "registros": [],
        },
    )
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any(
        "Nenhuma tabela de violência contra idosos foi materializada" in i.value
        for i in at.tabs[6].info
    )


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
    aba = at.tabs[8]
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
    assert any("não contém classificações" in i.value for i in at.tabs[8].info)


def test_app_aba_classificacao_falha_exibe_error():
    pads = _pads()
    pads[4] = patch(
        "dashboard.api_client.obter_classificacao",
        side_effect=ApiError("classificação indisponível"),
    )
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any("classificação indisponível" in e.value for e in at.tabs[8].error)


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


def test_app_aba_analises_renderiza_quatro_secoes():
    with _entrar(_pads()):
        at = _rodar()

    assert not at.exception
    aba = at.tabs[9]
    assert len(aba.get("plotly_chart")) == 6  # heatmap + pares + granger + 2 anomalias + zonas
    valores = [m.value for m in aba.metric]
    assert "2016–2024" in valores  # período consolidado das correlações
    assert any("Ceilândia" in str(df.value.values) for df in aba.dataframe)


def test_app_aba_analises_exibe_insights_das_correlacoes():
    with _entrar(_pads()):
        at = _rodar()

    aba = at.tabs[9]
    assert any(
        "correlação positiva forte" in md.value
        for md in aba.markdown
    )


def test_app_aba_analises_correlacoes_falha_exibe_error():
    pads = list(_pads())
    pads[7] = patch(
        "dashboard.api_client.obter_correlacoes",
        side_effect=ApiError("correlações indisponíveis"),
    )
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any(
        "correlações indisponíveis" in e.value for e in at.tabs[9].error
    )


def test_app_aba_analises_granger_vazio_avisa_e_informa():
    granger_vazio = dict(GRANGER, pares=[], total_significantes=0)
    pads = list(_pads())
    pads[8] = patch("dashboard.api_client.obter_granger", return_value=granger_vazio)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    aba = at.tabs[9]
    assert any("Nenhum par retornado" in i.value for i in aba.info)
    assert any("não contém pares avaliáveis" in w.value for w in aba.warning)


def test_app_aba_analises_anomalias_sem_serie_mensal_avisa_usuario():
    sem_mensal = dict(ANOMALIAS, mensal=[], total_mensal=0)
    pads = list(_pads())
    pads[9] = patch("dashboard.api_client.obter_anomalias", return_value=sem_mensal)
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any(
        "Não há anomalias na série mensal." in w.value for w in at.tabs[9].warning
    )


def test_app_aba_analises_zonas_quentes_falha_exibe_error():
    pads = list(_pads())
    pads[10] = patch(
        "dashboard.api_client.obter_zonas_quentes",
        side_effect=ApiError("zonas quentes indisponíveis"),
    )
    with _entrar(pads):
        at = _rodar()

    assert not at.exception
    assert any(
        "zonas quentes indisponíveis" in e.value for e in at.tabs[9].error
    )
