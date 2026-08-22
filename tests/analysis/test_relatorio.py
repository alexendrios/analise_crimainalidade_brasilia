import pandas as pd
import pytest

from analysis.relatorio import (
    _renderizar_markdown,
    exportar_relatorio,
    gerar_html,
    gerar_markdown,
    montar_relatorio,
    salvar_markdown,
    _tabela_markdown,
)


@pytest.fixture
def resultados_completos(dados_gold, tmp_path):
    from analysis.anomalias import detectar_anomalias, resumo_anomalias
    from analysis.correlacoes import (
        construir_matriz_indicadores,
        correlacao_idosos_patrimoniais,
        causalidade_granger,
        insights_correlacao,
        matriz_correlacao,
        pares_mais_correlacionados,
    )

    matriz = construir_matriz_indicadores(dados_gold)
    correlacao = matriz_correlacao(matriz)
    granger = causalidade_granger(matriz)

    return {
        "pares_correlacao": pares_mais_correlacionados(correlacao),
        "insights": insights_correlacao(correlacao, granger),
        "granger": granger,
        "correlacao_idosos_patrimonial": correlacao_idosos_patrimoniais(
            dados_gold["violencia_idosos_gold"], dados_gold["crimes_roubo_furto_gold"]
        ),
        "anomalias_painel": pd.DataFrame(
            {"regiao_administrativa": ["Gama"], "ano": [2021], "valor": [999], "score": [-0.5]}
        ),
        "anomalias_mensal": pd.DataFrame({"ano": [2017], "mes_num": [6], "fato": [90], "score": [-0.6]}),
        "zonas_quentes": pd.DataFrame(
            {"celula_id": ["R100C100", "R101C100", "R102C100"],
             "ocorrencia_roubo_pedestre": [300.0, 250.0, 200.0]}
        ),
        "caminho_mapa": tmp_path / "mapa.html",
    }


def test_montar_relatorio_produz_todas_as_secoes(resultados_completos):
    secoes = montar_relatorio(resultados_completos)
    titulos = [titulo for titulo, _ in secoes]

    assert "Correlação multivariada entre tipos de crime" in titulos
    assert any("Granger" in titulo for titulo in titulos)
    assert any("idosos" in titulo for titulo in titulos)
    assert any("Anomalias detectadas no painel" in titulo for titulo in titulos)
    assert any("série mensal" in titulo for titulo in titulos)
    assert any("Zonas quentes" in titulo for titulo in titulos)
    assert any("Mapa interativo" in titulo for titulo in titulos)


def test_secao_granger_omitida_quando_vazio(resultados_completos):
    resultados = dict(resultados_completos)
    resultados["granger"] = None

    titulos = [t for t, _ in montar_relatorio(resultados)]

    assert not any("Granger" in t for t in titulos)


def test_tabela_markdown_sem_dados_tem_placeholder():
    corpo = _tabela_markdown(None)

    assert "Sem dados" in corpo


def test_tabela_markdown_renderiza_linhas():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    corpo = _tabela_markdown(df)

    assert "| a | b |" in corpo
    assert corpo.strip().splitlines()[0].count("|") == 3


def test_gerar_markdown_estrutura_documento():
    documento = gerar_markdown("Título Principal", [("Seção A", "conteúdo A\n"), ("Seção B", "- item\n")])

    linhas = documento.splitlines()
    assert linhas[0] == "# Título Principal"
    assert "## Seção A" in linhas
    assert documento.rstrip("\n").endswith("- item")


def test_exportar_relatorio_cria_md_e_html_validos(tmp_path):
    artefatos = exportar_relatorio(
        "Relatório de Teste",
        [("Resumo", "Insight com acentuação: violência, região.\n")],
        tmp_path / "saida",
    )

    markdown = artefatos["markdown"].read_text(encoding="utf-8")
    assert "# Relatório de Teste" in markdown
    assert "violência" in markdown

    html = artefatos["html"].read_text(encoding="utf-8")
    assert html.startswith("<!DOCTYPE html>")
    assert 'lang="pt-BR"' in html
    assert "Relatório de Teste" in html
    assert "Gerado em" in html


def test_gerar_html_renderiza_tabela_lista_e_inline(tmp_path):
    secoes = [
        (
            "Seção Completa",
            "| a | b |\n| --- | --- |\n| 1 | **destaque** |\n\n- item um\n- item dois\n\n_nota final_\n",
        )
    ]

    caminho = gerar_html("Doc", secoes, tmp_path / "rel.html")
    html = caminho.read_text(encoding="utf-8")

    assert "<table>" in html and "<th>a</th>" in html
    assert "<strong>destaque</strong>" in html
    assert "<ul><li>item um</li><li>item dois</li></ul>" in html
    assert "<em>nota final</em>" in html
    assert "<style>" in html  # CSS embutido (autocontido)


def test_renderizar_markdown_escapa_html_perigoso():
    renderizado = _renderizar_markdown("<script>alert('x')</script>\n")

    assert "<script>" not in renderizado
    assert "&lt;script&gt;" in renderizado


def test_renderizar_markdown_codigo_inline():
    renderizado = _renderizar_markdown("Mapa em: `data/analises/mapa.html`\n")

    assert "<code>data/analises/mapa.html</code>" in renderizado


def test_tabela_html_com_celulas_vazias():
    renderizado = _renderizar_markdown("| a | b |\n| --- | --- |\n| x |  |\n")

    assert "<td></td>" in renderizado


def test_salvar_markdown_escreve_utf8(tmp_path):
    caminho = salvar_markdown("# Acentuação: ação\n", tmp_path / "rel.md")

    assert caminho.read_text(encoding="utf-8").endswith("ação\n")
