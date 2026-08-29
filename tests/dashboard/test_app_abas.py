from unittest.mock import patch

from tests.dashboard.conftest import (
    DADOS,
    DADOS_IDADES,
    RESUMO,
    TABELA,
    TABELA_IDADES,
    _entrar,
    _obter_dados_por_tabela,
    _pads,
    _rodar,
)


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


def test_app_aba_mancha_exclui_tabela_ocorrencias_idosos():
    tabelas = [
        TABELA,
        {"nome": "violencia_idosos_ocorrencias_gold", "disponivel_no_banco": True},
    ]
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=tabelas)
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    opcoes = at.tabs[3].selectbox[0].options
    assert "Violência contra idosos — ocorrências" not in opcoes
    assert "Crimes letais" in opcoes


def test_app_serie_com_media_movel_renderiza():
    with _entrar(_pads()):
        at = _rodar()
        at.slider(key="serie_media_movel").set_value(3).run()
    assert not at.exception
    assert len(at.get("plotly_chart")) >= 1


def test_app_serie_exclui_colunas_de_idade():
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=[TABELA_IDADES])
    pads[1] = patch("dashboard.api_client.obter_dados", return_value=DADOS_IDADES)
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    opcoes = at.selectbox(key="serie_coluna").options
    assert "Idade da vítima" not in opcoes
    assert "Idade do autor (suspeito)" not in opcoes
    assert "Crimes" in opcoes


def test_app_aba_idades_renderiza_histograma_e_resumo():
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=[TABELA_IDADES])
    pads[1] = patch("dashboard.api_client.obter_dados", return_value=DADOS_IDADES)
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    aba_idades = at.tabs[4]
    assert len(aba_idades.get("plotly_chart")) >= 1
    assert any("Idade da vítima" in str(df.value.values) for df in aba_idades.dataframe)


def test_app_aba_idades_sem_colunas_de_idade_avisa_usuario():
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=[TABELA])
    pads[1] = patch("dashboard.api_client.obter_dados", return_value=DADOS)
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
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=[TABELA_IDADES])
    pads[1] = patch("dashboard.api_client.obter_dados", return_value=dados_idades_zero)
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
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=[TABELA_IDADES])
    pads[1] = patch("dashboard.api_client.obter_dados", return_value=dados)
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
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=[TABELA_IDADES])
    pads[1] = patch("dashboard.api_client.obter_dados", return_value=dados)
    with _entrar(pads):
        at = _rodar()
        at.selectbox(key="serie_modo").select("Contagem por categoria").run()
        at.selectbox(key="serie_coluna").select("Motivação").run()
    assert not at.exception
    assert len(at.get("plotly_chart")) >= 1


def test_app_serie_categorica_sem_colunas_categoricas_avisa_usuario():
    pads = list(_pads())
    pads[0] = patch("dashboard.api_client.listar_tabelas", return_value=[TABELA])
    pads[1] = patch("dashboard.api_client.obter_dados", return_value=DADOS)
    with _entrar(pads):
        at = _rodar()
        at.selectbox(key="serie_modo").select("Contagem por categoria").run()
    assert not at.exception
    assert any("colunas categóricas" in info.value for info in at.info)


def test_app_mapa_ranking_sem_dados_avisa_usuario():
    from dashboard.visualizacoes import SemDadosParaGraficoError

    with _entrar(_pads()):
        at = _rodar()
        with patch("dashboard.visualizacoes.figura_ranking_ra", side_effect=SemDadosParaGraficoError("sem dados para o ranking")):
            at.selectbox(key="mapa_coluna").select("crimes").run()
    assert not at.exception
    assert any("sem dados para o ranking" in w.value for w in at.warning)
