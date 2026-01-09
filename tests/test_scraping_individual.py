import pandas as pd
from unittest.mock import Mock
from src.scraping import obter_populacao_ra_individual, SESSION


def test_scraping_ra_individual(monkeypatch):
    html = open("tests/fixtures/ra_individual.html", encoding="utf-8").read()

    mock_response = Mock()
    mock_response.text = html
    mock_response.raise_for_status = Mock()

    monkeypatch.setattr(SESSION, "get", Mock(return_value=mock_response))

    df = obter_populacao_ra_individual("http://fake-url")

    assert isinstance(df, pd.DataFrame)
    assert df.loc[0, "região administrativa"] == "Plano Piloto"
    assert df.loc[0, "população"] == "35000"
