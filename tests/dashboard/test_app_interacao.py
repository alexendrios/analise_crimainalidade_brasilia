from unittest.mock import patch

from dashboard.api_client import ApiError
from tests.dashboard.conftest import (
    DADOS,
    PREVISAO,
    TABELA,
    _entrar,
    _pads,
    _rodar,
)


def test_app_exibe_previsao_com_metricas_e_grafico():
    with _entrar(_pads()):
        at = _rodar()
    valores = [m.value for m in at.metric]
    assert "artefato" in valores
    assert "0.1" in valores
    assert len(at.get("plotly_chart")) >= 2


def test_app_tabela_vazia_avisa_usuario():
    pads = list(_pads())
    pads[1] = patch("dashboard.api_client.obter_dados", return_value={"tabela": "crimes_letais_gold", "total_linhas": 0, "total_paginas": 1, "registros": []})
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("ainda não foi materializada" in i.value for i in at.info)


def test_app_tabela_sem_colunas_numericas_avisa_usuario():
    pads = list(_pads())
    pads[1] = patch("dashboard.api_client.obter_dados", return_value={"tabela": "crimes_letais_gold", "total_linhas": 1, "total_paginas": 1, "registros": [{"ano": 2020, "regiao_administrativa": "Taguatinga"}]})
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

    pads = list(_pads())
    pads[1] = patch("dashboard.api_client.obter_dados", side_effect=fake_dados)
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert not any("ainda não foi materializada" in i.value for i in at.info)


def test_app_previsao_sem_pontos_informa_usuario():
    previsao_vazia = dict(PREVISAO, previsao=[])
    pads = list(_pads())
    pads[3] = patch("dashboard.api_client.obter_previsao", return_value=previsao_vazia)
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("não contém pontos" in i.value for i in at.info)


def test_app_previsao_falha_exibe_error():
    pads = list(_pads())
    pads[3] = patch("dashboard.api_client.obter_previsao", side_effect=ApiError("previsão indisponível"))
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("previsão indisponível" in e.value for e in at.error)


def test_app_previsao_sem_modelo_arquivo_renderiza():
    previsao_sem_arquivo = {k: v for k, v in PREVISAO.items() if k != "modelo_arquivo"}
    pads = list(_pads())
    pads[3] = patch("dashboard.api_client.obter_previsao", return_value=previsao_sem_arquivo)
    with _entrar(pads):
        at = _rodar()
    assert not at.exception


def test_app_modelos_persistidos_exibe_tabela():
    modelo = {"arquivo": "bundle.pkl", "criado_em": "2026-01-01T00:00:00",
              "tipo_modelo": "bundle", "formato_artefato": "bundle", "metricas": {"mae": 0.1, "rmse": 0.2}}
    pads = list(_pads())
    pads[5] = patch("dashboard.api_client.listar_modelos", return_value=[modelo])
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    assert any("bundle.pkl" in str(df.value.values) for df in at.dataframe)


def test_app_sem_ano_nem_ra_mostra_aviso_sem_grafico():
    pads = list(_pads())
    pads[1] = patch("dashboard.api_client.obter_dados", return_value={"tabela": "crimes_letais_gold", "total_linhas": 1, "total_paginas": 1, "registros": [{"crimes": 10}]})
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
