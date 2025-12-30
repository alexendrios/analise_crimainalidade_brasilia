import pytest
from util.rotas import montar_url, gerar_urls_rotas


# ============================================================
# TESTES montar_url
# ============================================================


def test_montar_url_basico():
    url = montar_url(
        url="http://base", dataset="dataset", resource="resource", arquivo="arquivo.csv"
    )

    assert url == "http://base/dataset/resource/arquivo.csv"


def test_montar_url_com_barra_no_final():
    url = montar_url(url="http://base/", dataset="d1", resource="r1", arquivo="a1.csv")

    # comportamento atual (sem normalização)
    assert url == "http://base//d1/r1/a1.csv"


# ============================================================
# TESTES gerar_urls_rotas
# ============================================================


def test_gerar_urls_rotas_com_url_direta():
    rotas = {"rota1": {"url": "http://direta/arquivo.csv", "arquivo": "arquivo.csv"}}

    resultado = gerar_urls_rotas("http://base", rotas)

    assert resultado == [("http://direta/arquivo.csv", "arquivo.csv")]


def test_gerar_urls_rotas_montada():
    rotas = {"rota1": {"dataset": "ds", "resource": "res", "arquivo": "arq.csv"}}

    resultado = gerar_urls_rotas("http://base", rotas)

    assert resultado == [("http://base/ds/res/arq.csv", "arq.csv")]


def test_gerar_urls_rotas_mista():
    rotas = {
        "rota1": {"url": "http://direta/a.csv", "arquivo": "a.csv"},
        "rota2": {"dataset": "ds", "resource": "res", "arquivo": "b.csv"},
    }

    resultado = gerar_urls_rotas("http://base", rotas)

    assert resultado == [
        ("http://direta/a.csv", "a.csv"),
        ("http://base/ds/res/b.csv", "b.csv"),
    ]


def test_gerar_urls_rotas_vazio():
    resultado = gerar_urls_rotas("http://base", {})

    assert resultado == []


def test_gerar_urls_rotas_com_rotas_dict_explicito():
    rotas = {"rota1": {"dataset": "ds", "resource": "res", "arquivo": "padrao.csv"}}

    resultado = gerar_urls_rotas("http://base", rotas)

    assert resultado == [("http://base/ds/res/padrao.csv", "padrao.csv")]

def test_gerar_urls_rotas_sem_rotas_dict_retorna_vazio():
    resultado = gerar_urls_rotas("http://base")
    assert resultado == []
