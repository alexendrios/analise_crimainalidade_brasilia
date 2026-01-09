from src.scraping import obter_tabela_ra_populacao
from unittest.mock import patch
from pathlib import Path


def test_scraping_lista_ra(mock_response):
    html = Path("tests/fixtures/lista_ra.html").read_text(encoding="utf-8")

    with patch("src.scraping.SESSION.get") as mock_get:
        mock_get.return_value = mock_response(html)

        df = obter_tabela_ra_populacao("http://fake-url")

    assert len(df) == 2
    assert "região administrativa" in df.columns
    assert "população" in df.columns
    assert df.iloc[0]["região administrativa"] == "Águas Claras"
