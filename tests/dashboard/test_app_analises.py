from unittest.mock import patch

from dashboard.api_client import ApiError
from tests.dashboard.conftest import (
    CLASSIFICACAO,
    _entrar,
    _obter_dados_por_tabela,
    _pads,
    _rodar,
)


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
        side_effect=lambda tabela, *args, **kwargs: {"tabela": tabela, "total_linhas": 0, "total_paginas": 1, "registros": []},
    )
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("Nenhuma tabela de desaparecidos foi materializada" in i.value for i in at.tabs[5].info)


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
        side_effect=lambda tabela, *args, **kwargs: {"tabela": tabela, "total_linhas": 0, "total_paginas": 1, "registros": []},
    )
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("Nenhuma tabela de violência contra idosos foi materializada" in i.value for i in at.tabs[6].info)


def test_app_aba_classificacao_renderiza_graficos_metricas_e_tabela():
    with _entrar(_pads()):
        at = _rodar()
    assert not at.exception
    aba = at.tabs[8]
    valores = [m.value for m in aba.metric]
    assert "artefato" in valores
    assert any("10.66" == str(v) for v in valores)
    assert len(aba.get("plotly_chart")) == 2
    assert any("Taguatinga" in str(df.value.values) for df in aba.dataframe)
    assert any("Taxa de homicídio" in str(df.value.values) for df in aba.dataframe)


def test_app_aba_classificacao_ranking_respeita_ano_selecionado():
    with _entrar(_pads()):
        at = _rodar()
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
    pads[4] = patch("dashboard.api_client.obter_classificacao", side_effect=ApiError("classificação indisponível"))
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("classificação indisponível" in e.value for e in at.tabs[8].error)


def test_app_aba_classificacao_sem_metricas_holdout_renderiza():
    metricas_parciais = {k: v for k, v in CLASSIFICACAO["metricas"].items()
                         if k not in ("holdout_roc_auc", "holdout_f1", "cv_roc_auc_std")}
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
    assert len(aba.get("plotly_chart")) == 6
    valores = [m.value for m in aba.metric]
    assert "2016–2024" in valores
    assert any("Ceilândia" in str(df.value.values) for df in aba.dataframe)


def test_app_aba_analises_exibe_insights_das_correlacoes():
    with _entrar(_pads()):
        at = _rodar()
    aba = at.tabs[9]
    assert any("correlação positiva forte" in md.value for md in aba.markdown)


def test_app_aba_analises_correlacoes_falha_exibe_error():
    pads = list(_pads())
    pads[7] = patch("dashboard.api_client.obter_correlacoes", side_effect=ApiError("correlações indisponíveis"))
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("correlações indisponíveis" in e.value for e in at.tabs[9].error)


def test_app_aba_analises_granger_vazio_avisa_e_informa():
    from tests.dashboard.conftest import GRANGER as _GRANGER

    granger_vazio = dict(_GRANGER, pares=[], total_significantes=0)
    pads = list(_pads())
    pads[8] = patch("dashboard.api_client.obter_granger", return_value=granger_vazio)
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    aba = at.tabs[9]
    assert any("Nenhum par retornado" in i.value for i in aba.info)
    assert any("não contém pares avaliáveis" in w.value for w in aba.warning)


def test_app_aba_analises_anomalias_sem_serie_mensal_avisa_usuario():
    from tests.dashboard.conftest import ANOMALIAS as _ANOMALIAS

    sem_mensal = dict(_ANOMALIAS, mensal=[], total_mensal=0)
    pads = list(_pads())
    pads[9] = patch("dashboard.api_client.obter_anomalias", return_value=sem_mensal)
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("Não há anomalias na série mensal." in w.value for w in at.tabs[9].warning)


def test_app_aba_analises_zonas_quentes_falha_exibe_error():
    pads = list(_pads())
    pads[10] = patch("dashboard.api_client.obter_zonas_quentes", side_effect=ApiError("zonas quentes indisponíveis"))
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("zonas quentes indisponíveis" in e.value for e in at.tabs[9].error)
