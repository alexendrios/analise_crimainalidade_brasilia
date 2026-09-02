from unittest.mock import patch

from dashboard.api_client import ApiError
from tests.dashboard.conftest import _entrar, _pads, _rodar


def _aba_qualidade(at):
    return at.tabs[-1]


def test_app_aba_qualidade_renderiza_metricas_grafico_e_tabela():
    with _entrar(_pads()):
        at = _rodar()
    assert not at.exception
    aba = _aba_qualidade(at)

    valores = [m.value for m in aba.metric]
    assert "92/100" in valores
    assert "1 de 2" in valores
    assert "1" in valores

    assert len(aba.get("plotly_chart")) == 1

    corpo = str(aba.dataframe[0].value.values)
    assert "crimes_letais_gold" in corpo
    assert "desaparecidos_regiao_gold" in corpo
    assert "92.0" in corpo

    assert any("Data Quality Score do catálogo gold" in s.value for s in aba.subheader)


def test_app_aba_qualidade_expande_detalhe_por_tabela():
    with _entrar(_pads()):
        at = _rodar()
    assert not at.exception
    aba = _aba_qualidade(at)

    detalhe_letais = next(
        e for e in aba.expander if "crimes_letais_gold" in e.label
    )
    assert any("cobre 7 de 10 anos esperados" in c.value for c in detalhe_letais.caption)
    assert not detalhe_letais.error

    detalhe_desap = next(
        e for e in aba.expander if "desaparecidos_regiao_gold" in e.label
    )
    assert any("Tabela não materializada" in w.value for w in detalhe_desap.warning)


def test_app_aba_qualidade_falha_exibe_error():
    pads = list(_pads())
    pads[11] = patch(
        "dashboard.api_client.obter_qualidade", side_effect=ApiError("qualidade indisponível")
    )
    with _entrar(pads):
        at = _rodar()
    assert not at.exception
    aba = _aba_qualidade(at)
    assert any("qualidade indisponível" in e.value for e in aba.error)