import pytest
from src.scraping import obter_populacao_ra_individual
from unittest.mock import patch


def test_ra_individual_sem_populacao(mock_response):
    html = "<h1 id='firstHeading'>Teste</h1>"

    with patch("src.scraping.SESSION.get") as mock_get:
        mock_get.return_value = mock_response(html)

        with pytest.raises(RuntimeError):
            obter_populacao_ra_individual("http://fake-url")

def test_scraping_sem_infobox_mas_com_tabela(mock_response):
    html = """
    <h1 id="firstHeading">Teste</h1>
    <table class="infobox_v2">
        <tr><th>População</th><td>10.000</td></tr>
    </table>
    """

    with patch("src.scraping.SESSION.get") as mock_get:
        mock_get.return_value = mock_response(html)

        df = obter_populacao_ra_individual("http://fake")

    assert df.iloc[0]["população"] == "10000"

def test_scraping_sem_h1_usa_titulo_fallback(mock_response):
    html = """
    <table class="infobox">
        <tr><th>População</th><td>8.000</td></tr>
    </table>
    """

    with patch("src.scraping.SESSION.get") as mock_get:
        mock_get.return_value = mock_response(html)

        df = obter_populacao_ra_individual("http://fake")

    assert df.iloc[0]["região administrativa"] == "Desconhecida"

def test_scraping_populacao_formato_invalido(mock_response):
    html = """
    <h1 id="firstHeading">Teste</h1>
    <table class="infobox">
        <tr><th>População</th><td>não disponível</td></tr>
    </table>
    """

    with patch("src.scraping.SESSION.get") as mock_get:
        mock_get.return_value = mock_response(html)

        df = obter_populacao_ra_individual("http://fake")

    assert df.iloc[0]["população"] is None

def test_scraping_exception_durante_parsing(mock_response):
    with patch("src.scraping.BeautifulSoup", side_effect=Exception("erro")):
        with patch("src.scraping.SESSION.get") as mock_get:
            mock_get.return_value = mock_response("<html></html>")

            with pytest.raises(Exception):
                obter_populacao_ra_individual("http://fake")
