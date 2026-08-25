from contextlib import ExitStack, contextmanager
from unittest.mock import patch

import pytest

from dashboard.api_client import ApiError
from dashboard.contexto_ia import montar_contexto_ia

TABELAS = [
    {"nome": "crimes_letais_gold"},
    {"nome": "identificacao_crimes_contra_mulher_gold"},
]

RESUMO = {"linhas": 10, "colunas": 3, "nulos_total": 1}

CORRELACOES = {
    "metodo": "pearson",
    "periodo": [2016, 2024],
    "pares_destaque": [
        {"indicador_a": "roubo_pedestre", "indicador_b": "homicidio", "correlacao": 0.8},
    ],
    "insights": ["'Roubo a pedestre' e 'Homicídio' têm correlação positiva forte."],
}

GRANGER = {
    "alpha": 0.05,
    "total_pares": 2,
    "total_significantes": 1,
    "pares": [
        {"origem": "roubo_pedestre", "destino": "homicidio",
         "melhor_lag": 1, "p_valor": 0.01, "significante": True},
        {"origem": "a", "destino": "b", "melhor_lag": 1, "p_valor": 0.9,
         "significante": False},
    ],
}

ANOMALIAS = {
    "total_painel": 1,
    "total_mensal": 2,
    "painel": [
        {"regiao_administrativa": "Ceilândia", "ano": 2021,
         "ocorrencia_roubo_pedestre": 900},
    ],
}

ZONAS_QUENTES = {
    "ano_referencia": 2024,
    "tamanho_celula_km": 1.5,
    "zonas": [{"celula_id": "R010C005", "ocorrencia_roubo_pedestre": 120}],
}

CLASSIFICACAO = {
    "metricas": {"cv_roc_auc_media": 0.99, "holdout_f1": 1.0},
    "distribuicao_real": {"alta": 2, "baixa": 2},
    "classificacoes": [
        {"regiao_administrativa": "Taguatinga", "ano": 2024,
         "rotulo_previsto": "alta", "probabilidade_alta": 0.92},
    ],
}

PREVISAO = {
    "fonte_modelo": "artefato",
    "previsao": [{"ano": 2027, "valor_previsto": 100.0}],
}


DADOS_CRIMES = {
    "registros": [
        {"regiao_administrativa": "Ceilândia", "homicidio": 10, "roubo_pedestre": 20},
        {"regiao_administrativa": "Taguatinga", "homicidio": 5, "roubo_pedestre": 15},
    ],
}


def _pads_contexto():
    return [
        patch("dashboard.contexto_ia.listar_tabelas", return_value=TABELAS),
        patch("dashboard.contexto_ia.obter_resumo", return_value=RESUMO),
        patch("dashboard.contexto_ia.obter_dados", return_value=DADOS_CRIMES),
        patch("dashboard.contexto_ia.obter_correlacoes", return_value=CORRELACOES),
        patch("dashboard.contexto_ia.obter_granger", return_value=GRANGER),
        patch("dashboard.contexto_ia.obter_anomalias", return_value=ANOMALIAS),
        patch("dashboard.contexto_ia.obter_zonas_quentes", return_value=ZONAS_QUENTES),
        patch("dashboard.contexto_ia.obter_classificacao", return_value=CLASSIFICACAO),
        patch("dashboard.contexto_ia.obter_previsao", return_value=PREVISAO),
    ]


def test_montar_contexto_reune_as_oito_secoes():
    with _entrar(_pads_contexto()):
        contexto = montar_contexto_ia("http://api-teste")

    for titulo in (
        "## Tabelas gold disponíveis",
        "## Top 5 RAs com mais ocorrências de crimes",
        "## Correlações entre indicadores",
        "## Causalidade de Granger (indicadores anuais)",
        "## Anomalias (Isolation Forest)",
        "## Zonas quentes (roubo a pedestre por célula)",
        "## Classificação de criminalidade letal por RA",
        "## Previsão (crimes contra a mulher)",
    ):
        assert titulo in contexto

    assert "- crimes_letais_gold: 10 linhas, 3 colunas, 1 nulos" in contexto
    assert "identificacao_crimes_contra_mulher_gold" not in contexto
    assert "r = +0.80" in contexto
    assert "roubo_pedestre → homicidio (lag 1, p = 0.01)" in contexto
    assert "Ceilândia em 2021" in contexto
    assert "R010C005: 120 ocorrências" in contexto
    assert "Taguatinga em 2024" in contexto
    assert "2027: valor previsto 100.0" in contexto
    # top 5 RAs
    assert "1º Ceilândia: 30 registros" in contexto
    assert "2º Taguatinga: 20 registros" in contexto


def test_montar_contexto_api_fora_do_ar_marca_as_secoes_como_indisponiveis():
    pads = [
        patch(target, side_effect=ApiError("fora do ar"))
        for target in (
            "dashboard.contexto_ia.listar_tabelas",
            "dashboard.contexto_ia.obter_resumo",
            "dashboard.contexto_ia.obter_dados",
            "dashboard.contexto_ia.obter_correlacoes",
            "dashboard.contexto_ia.obter_granger",
            "dashboard.contexto_ia.obter_anomalias",
            "dashboard.contexto_ia.obter_zonas_quentes",
            "dashboard.contexto_ia.obter_classificacao",
            "dashboard.contexto_ia.obter_previsao",
        )
    ]
    with _entrar(pads):
        contexto = montar_contexto_ia("http://api-teste")

    assert contexto.count("Indisponível no momento.") == 8
    for titulo in ("## Tabelas gold disponíveis", "## Previsão (crimes contra a mulher)"):
        assert titulo in contexto


@contextmanager
def _entrar(pads):
    with ExitStack() as stack:
        for pad in pads:
            stack.enter_context(pad)
        yield
