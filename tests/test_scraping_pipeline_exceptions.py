from unittest.mock import patch
from src.scraping import obter_dados_ra_populacao
import runpy

def test_pipeline_erro_em_ra_individual_nao_quebra(monkeypatch):
    monkeypatch.setattr("src.scraping.URLS_RA_INDIVIDUAIS", ["http://fake-url"])

    with patch("src.scraping.obter_populacao_ra_individual") as mock_ra:
        mock_ra.side_effect = RuntimeError("Erro simulado")

        # não deve levantar exceção
        obter_dados_ra_populacao()

def test_pipeline_sem_urls_individuais(monkeypatch):
    monkeypatch.setattr("src.scraping.URLS_RA_INDIVIDUAIS", [])
    obter_dados_ra_populacao()
    
def test_execucao_direta_scraping():
    runpy.run_module("src.scraping", run_name="__main__")
    
def test_pipeline_grava_csv(tmp_path, monkeypatch):
    output = tmp_path / "saida.csv"

    monkeypatch.setattr("src.scraping.OUTPUT_PATH", str(output))

    obter_dados_ra_populacao()

    assert output.exists()
