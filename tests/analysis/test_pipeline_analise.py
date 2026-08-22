from analysis.pipeline_analise import executar_analise


def test_executar_analise_produz_todos_os_artefatos(dados_gold, tmp_path):
    import analysis.pipeline_analise as modulo

    modulo._carregar_tabelas = lambda: dados_gold

    resumo = executar_analise(pasta_saida=tmp_path)

    assert resumo["markdown"].exists() and resumo["markdown"].suffix == ".md"
    assert resumo["html"].exists() and resumo["html"].suffix == ".html"
    assert resumo["caminho_mapa"].exists() and resumo["caminho_mapa"].suffix == ".html"

    assert resumo["indicadores"] == 12
    assert resumo["anos"] == "2015-2024"
    assert isinstance(resumo["anomalias_painel"], int)
    assert len(resumo["top_zonas_quentes"]) == 3

    markdown = resumo["markdown"].read_text(encoding="utf-8")
    assert "Criminalidade Brasília/DF" in markdown
