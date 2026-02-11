import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pathlib import Path
import src.scraping as mod

FIXTURES = Path(__file__).parent / "fixtures"

# =========================================
# FIXTURE HTML BASE
# =========================================

HTML_LISTA_OK = """
<table class="wikitable">
<tr><th>Região Administrativa</th><th>População</th></tr>
<tr><td>RA 1</td><td>10.000</td></tr>
</table>
"""

HTML_RA_OK = """
<h1 id="firstHeading">RA 1 (Distrito Federal)</h1>
<table>
<tr><td>População</td></tr>
<tr><td>- Total 15.000 habitantes</td></tr>
</table>
"""

HTML_RA_SEM_TOTAL = """
<h1 id="firstHeading">RA X</h1>
<table>
<tr><td>População</td></tr>
<tr><td>Valor estranho</td></tr>
</table>
"""

HTML_RA_SEM_NUMERO = """
<h1 id="firstHeading">RA Y</h1>
<table>
<tr><td>População</td></tr>
<tr><td>- Total abc</td></tr>
</table>
"""

# =========================================
# criar_sessao
# =========================================


def test_criar_sessao():
    session = mod.criar_sessao()
    assert "User-Agent" in session.headers


# =========================================
# obter_tabela_ra_populacao
# =========================================


@patch("src.scraping.SESSION.get")
def test_obter_tabela_sucesso(mock_get):
    mock_resp = MagicMock()
    mock_resp.text = HTML_LISTA_OK
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    df = mod.obter_tabela_ra_populacao("fake_url")

    assert not df.empty
    assert "região administrativa" in df.columns
    assert "população" in df.columns


@patch("src.scraping.SESSION.get")
def test_obter_tabela_sem_tabela(mock_get):
    mock_resp = MagicMock()
    mock_resp.text = "<html></html>"
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    with pytest.raises(RuntimeError):
        mod.obter_tabela_ra_populacao("fake_url")


# =========================================
# obter_populacao_ra_individual
# =========================================


@patch("src.scraping.SESSION.get")
def test_obter_ra_sucesso(mock_get):
    mock_resp = MagicMock()
    mock_resp.text = HTML_RA_OK
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    df = mod.obter_populacao_ra_individual("fake")

    assert df.iloc[0][mod.COL_RA] == "RA 1"
    assert df.iloc[0][mod.COL_POP] == "15000"


@patch("src.scraping.SESSION.get")
def test_obter_ra_sem_label(mock_get):
    mock_resp = MagicMock()
    mock_resp.text = "<html></html>"
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    with pytest.raises(RuntimeError):
        mod.obter_populacao_ra_individual("fake")


@patch("src.scraping.SESSION.get")
def test_obter_ra_sem_total_warning(mock_get):
    mock_resp = MagicMock()
    mock_resp.text = HTML_RA_SEM_TOTAL
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    df = mod.obter_populacao_ra_individual("fake")
    assert df.iloc[0][mod.COL_POP] is None


@patch("src.scraping.SESSION.get")
def test_obter_ra_sem_numero(mock_get):
    mock_resp = MagicMock()
    mock_resp.text = HTML_RA_SEM_NUMERO
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    df = mod.obter_populacao_ra_individual("fake")
    assert df.iloc[0][mod.COL_POP] is None


# =========================================
# normalizar_df
# =========================================


def test_normalizar_sucesso():
    df = pd.DataFrame({"Região Administrativa": [" RA 1 "], "População": ["10.000"]})

    df_norm = mod.normalizar_df(df)

    assert df_norm[mod.COL_RA].iloc[0] == "RA 1"
    assert df_norm[mod.COL_POP].iloc[0] == 10000


def test_normalizar_coluna_invalida():
    df = pd.DataFrame({"x": [1]})

    with pytest.raises(ValueError):
        mod.normalizar_df(df)


# =========================================
# PIPELINE PRINCIPAL
# =========================================


@patch("src.scraping.obter_tabela_ra_populacao")
@patch("src.scraping.obter_populacao_ra_individual")
@patch("src.scraping.normalizar_df")
@patch("src.scraping.pd.DataFrame.to_csv")
def test_pipeline_sucesso(
    mock_to_csv,
    mock_normalizar,
    mock_individual,
    mock_lista,
):
    df_lista = pd.DataFrame({mod.COL_RA: ["RA 1"], mod.COL_POP: [10000]})

    df_ind = pd.DataFrame({mod.COL_RA: ["RA 2"], mod.COL_POP: [20000]})

    mock_lista.return_value = df_lista
    mock_normalizar.side_effect = lambda x: x
    mock_individual.return_value = df_ind

    with patch.object(mod, "URLS_RA_INDIVIDUAIS", ["url1"]):
        mod.obter_dados_ra_populacao()

    assert mock_to_csv.called


@patch("src.scraping.obter_tabela_ra_populacao")
@patch("src.scraping.obter_populacao_ra_individual", side_effect=Exception("erro"))
@patch("src.scraping.normalizar_df")
@patch("src.scraping.pd.DataFrame.to_csv")
def test_pipeline_excecao_individual(
    mock_to_csv,
    mock_normalizar,
    mock_individual,
    mock_lista,
):
    df_lista = pd.DataFrame({mod.COL_RA: ["RA 1"], mod.COL_POP: [10000]})

    mock_lista.return_value = df_lista
    mock_normalizar.side_effect = lambda x: x

    with patch.object(mod, "URLS_RA_INDIVIDUAIS", ["url1"]):
        mod.obter_dados_ra_populacao()

    assert mock_to_csv.called


@patch("src.scraping.obter_tabela_ra_populacao")
@patch("src.scraping.normalizar_df")
@patch("src.scraping.pd.DataFrame.to_csv")
def test_pipeline_sem_individuais(
    mock_to_csv,
    mock_normalizar,
    mock_lista,
):
    df_lista = pd.DataFrame({mod.COL_RA: ["RA 1"], mod.COL_POP: [10000]})

    mock_lista.return_value = df_lista
    mock_normalizar.side_effect = lambda x: x

    with patch.object(mod, "URLS_RA_INDIVIDUAIS", []):
        mod.obter_dados_ra_populacao()

    assert mock_to_csv.called

@patch("src.scraping.SESSION.get")
def test_ra_populacao_valida(mock_get):
    from pathlib import Path
    import src.scraping as mod

    html = Path("tests/scrapings/fixtures/ra_populacao_valida.html").read_text(
        encoding="utf-8"
    )

    mock_resp = MagicMock()
    mock_resp.text = html
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    df = mod.obter_populacao_ra_individual("fake_url")

    assert df.iloc[0][mod.COL_RA] == "RA Teste"
    assert df.iloc[0][mod.COL_POP] == "123456"


def carregar_html(nome_arquivo):
    return (FIXTURES / nome_arquivo).read_text(encoding="utf-8")


@patch("src.scraping.SESSION.get")
def test_ra_sem_linha_rotulo(mock_get):
    html = carregar_html("ra_sem_linha_rotulo.html")

    mock_resp = MagicMock()
    mock_resp.text = html
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    with pytest.raises(RuntimeError, match="Linha do rótulo"):
        mod.obter_populacao_ra_individual("fake_url")


@patch("src.scraping.SESSION.get")
def test_ra_sem_linha_valor(mock_get):
    html = carregar_html("ra_sem_linha_valor.html")

    mock_resp = MagicMock()
    mock_resp.text = html
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    with pytest.raises(RuntimeError, match="Linha do valor"):
        mod.obter_populacao_ra_individual("fake_url")
