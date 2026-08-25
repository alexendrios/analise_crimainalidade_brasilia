import importlib
import sys
from unittest.mock import patch

from dashboard.api_client import ApiError
from tests.dashboard.conftest import (
    APP_PATH,
    DADOS,
    RESUMO,
    TABELA,
    _entrar,
    _pads,
    _rodar,
)

OK = {"status": "ok", "database": "ok"}


def test_app_renderiza_sem_erros():
    with _entrar(_pads()):
        at = _rodar()
    assert not at.exception
    assert not at.error
    assert at.title[0].value == "Criminalidade em Brasília/DF — Dashboard Analítico"


def test_app_visao_geral_exclui_tabelas_nao_sumarizaveis():
    tabelas = [
        TABELA,
        {"nome": "identificacao_crimes_contra_mulher_gold", "disponivel_no_banco": True},
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
        patch("dashboard.ia_client.gerar_resumo_ia", return_value="# Panorama\n- Dados OK"),
        patch("dashboard.contexto_ia.montar_contexto_ia", return_value="DADOS SINTETIZADOS"),
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
        patch("dashboard.ia_client.gerar_resumo_ia", side_effect=OllamaError("Ollama fora do ar")),
        patch("dashboard.contexto_ia.montar_contexto_ia", return_value="CONTEXTO"),
    ]
    with _entrar(pads):
        at = _rodar()
        at.tabs[14].button[0].click().run()
    assert not at.exception
    assert any("Ollama fora do ar" in e.value for e in at.tabs[14].error)


def test_app_sem_tabelas_avisa_usuario():
    pads = [
        patch("dashboard.api_client.listar_tabelas", return_value=[]),
        patch("dashboard.api_client.obter_dados", return_value=DADOS),
        patch("dashboard.api_client.obter_resumo", return_value=RESUMO),
        patch("dashboard.api_client.obter_previsao", return_value={"previsao": []}),
        patch("dashboard.api_client.obter_classificacao", return_value={"classificacoes": []}),
        patch("dashboard.api_client.listar_modelos", return_value=[]),
        patch("dashboard.api_client.health", return_value=OK),
        patch("dashboard.api_client.obter_correlacoes", return_value={"pares_destaque": [], "insights": []}),
        patch("dashboard.api_client.obter_granger", return_value={"pares": [], "total_significantes": 0}),
        patch("dashboard.api_client.obter_anomalias", return_value={"painel": [], "mensal": []}),
        patch("dashboard.api_client.obter_zonas_quentes", return_value={"zonas": []}),
        patch("dashboard.ia_client.listar_modelos_ollama", return_value=[]),
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


def test_app_health_falha_exibe_error():
    pads = list(_pads())
    pads[6] = patch("dashboard.api_client.health", side_effect=ApiError("API lenta"))
    with _entrar(pads):
        at = _rodar()
        at.sidebar.button[0].click().run()
    assert not at.exception
    assert any("API lenta" in e.value for e in at.sidebar.error)


def test_app_erro_ao_listar_modelos_exibe_error():
    pads = list(_pads())
    pads[5] = patch("dashboard.api_client.listar_modelos", side_effect=ApiError("modelos indisponíveis"))
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("modelos indisponíveis" in e.value for e in at.error)


def test_app_erro_ao_obter_resumo_exibe_error():
    pads = list(_pads())
    pads[2] = patch("dashboard.api_client.obter_resumo", side_effect=ApiError("resumo indisponível"))
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("resumo indisponível" in e.value for e in at.error)


def test_app_importado_fora_do_windows_nao_aplica_politica_e_nao_executa_main(monkeypatch):
    import dashboard.app as modulo_app

    monkeypatch.setattr(sys, "platform", "linux")
    try:
        recarregado = importlib.reload(modulo_app)
        assert callable(recarregado.main)
    finally:
        monkeypatch.undo()
        importlib.reload(modulo_app)
