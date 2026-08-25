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
    return AppTest.from_file(APP_PATH, default_timeout=15).run()
