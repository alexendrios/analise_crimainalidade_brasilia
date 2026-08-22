# analysis/relatorio.py
"""
Exportação de relatório executivo (Markdown + HTML) com os insights das
análises de correlação, anomalias e zonas quentes.

O relatório é montado a partir de um dicionário de resultados produzido pelo
pipeline de análise (`analysis.pipeline_analise`), mantendo este módulo
independente da fonte dos dados e fácil de testar.

O HTML é autocontido (CSS embutido, sem dependências externas) e pronto para
compartilhamento ou impressão em PDF pelo navegador.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from util.log import logs

logger = logs()

_CSS_RELATORIO = """
:root { --borda: #d8dee6; --destaque: #1a5276; --cinza: #5d6d7e; }
* { box-sizing: border-box; }
body { font-family: "Segoe UI", Roboto, Arial, sans-serif; color: #212f3d; margin: 0; background: #f4f6f8; }
main { max-width: 960px; margin: 0 auto; padding: 24px 32px 48px; background: #ffffff; }
header { background: var(--destaque); color: #ffffff; padding: 28px 32px; }
header h1 { margin: 0 0 6px; font-size: 1.6em; }
header p { margin: 0; opacity: 0.85; font-size: 0.9em; }
h2 { color: var(--destaque); border-bottom: 2px solid var(--borda); padding-bottom: 6px; margin-top: 36px; font-size: 1.15em; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 0.92em; }
th, td { border: 1px solid var(--borda); padding: 7px 10px; text-align: left; }
th { background: #eaf1f8; color: var(--destaque); }
tr:nth-child(even) td { background: #fafbfc; }
li { margin: 4px 0; }
p, li { line-height: 1.55; }
em { color: var(--cinza); }
code { background: #eef1f4; padding: 1px 5px; border-radius: 3px; }
@media print { body { background: #ffffff; } main { max-width: none; } header { -webkit-print-color-adjust: exact; print-color-adjust: exact; } }
"""


def _html_escape(texto: str) -> str:
    return (
        texto.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _inline_html(texto: str) -> str:
    """Escapa HTML e aplica código inline, negrito e itálico."""
    import re

    escapado = _html_escape(texto)
    escapado = re.sub(r"`([^`]+)`", r"<code>\1</code>", escapado)
    escapado = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escapado)
    escapado = re.sub(r"(?<![\w])_([^_]+)_(?![\w])", r"<em>\1</em>", escapado)
    return escapado


def _tabela_html(linhas: list[str]) -> str:
    """Renderiza um bloco de linhas Markdown de tabela como <table>."""
    cabecalho = [coluna.strip() for coluna in linhas[0].strip().strip("|").split("|")]
    corpo = [
        [celula.strip() for celula in linha.strip().strip("|").split("|")]
        for linha in linhas[2:]
    ]
    ths = "".join(f"<th>{_inline_html(coluna)}</th>" for coluna in cabecalho)
    trs = "".join(
        "<tr>" + "".join(f"<td>{_inline_html(celula)}</td>" for celula in registro) + "</tr>"
        for registro in corpo
    )
    return f"<table><thead><tr>{ths}</tr></thead><tbody>{trs}</tbody></table>"


def _e_separador(linha: str) -> bool:
    sem_marcacao = linha.replace("|", "").replace("-", "").replace(" ", "").replace(":", "")
    return linha.strip().startswith("|") and not sem_marcacao


def _renderizar_markdown(corpo: str) -> str:
    """
    Converte o subconjunto Markdown gerado por este módulo em HTML:
    tabelas, listas com hífen e parágrafos (com inline de **/_/`).
    """
    linhas = corpo.splitlines()
    blocos: list[str] = []
    indice = 0

    while indice < len(linhas):
        linha = linhas[indice]

        if (
            linha.strip().startswith("|")
            and indice + 1 < len(linhas)
            and _e_separador(linhas[indice + 1])
        ):
            fim = indice + 2
            while fim < len(linhas) and linhas[fim].strip().startswith("|"):
                fim += 1
            blocos.append(_tabela_html(linhas[indice:fim]))
            indice = fim
            continue

        if linha.strip().startswith("- "):
            itens = []
            while indice < len(linhas) and linhas[indice].strip().startswith("- "):
                itens.append(f"<li>{_inline_html(linhas[indice].strip()[2:])}</li>")
                indice += 1
            blocos.append("<ul>" + "".join(itens) + "</ul>")
            continue

        if linha.strip():
            blocos.append(f"<p>{_inline_html(linha.strip())}</p>")
        indice += 1

    return "\n".join(blocos)


def _tabela_markdown(df: pd.DataFrame, colunas: list[str] | None = None) -> str:
    """Renderiza um DataFrame como tabela Markdown simples."""
    if df is None or df.empty:
        return "_Sem dados disponíveis._\n"
    selecionadas = colunas or list(df.columns)
    cabecalho = "| " + " | ".join(selecionadas) + " |"
    separador = "|" + "|".join([" --- " for _ in selecionadas]) + "|"
    linhas = []
    for _, registro in df.iterrows():
        celulas = [str(registro[coluna]) for coluna in selecionadas]
        linhas.append("| " + " | ".join(celulas) + " |")
    return "\n".join([cabecalho, separador] + linhas) + "\n"


def montar_relatorio(resultados: dict) -> list[tuple[str, str]]:
    """
    Traduz os resultados do pipeline em seções `(titulo, corpo_markdown)`.

    Chaves reconhecidas em `resultados`:
        pares_correlacao, insights, granger, correlacao_idosos_patrimonial,
        anomalias_painel, anomalias_mensal, zonas_quentes, caminho_mapa
    """
    secoes: list[tuple[str, str]] = []

    pares = resultados.get("pares_correlacao")
    if pares is not None and not pares.empty:
        pares = pares.assign(correlacao=pares["correlacao"].round(2))
    corpo = _tabela_markdown(pares, ["indicador_a", "indicador_b", "correlacao"])
    for insight in resultados.get("insights", []):
        corpo += f"- {insight}\n"
    secoes.append(("Correlação multivariada entre tipos de crime", corpo))

    granger = resultados.get("granger")
    if granger is not None and not granger.empty:
        significante = granger.query("significante")
        corpo = (
            _tabela_markdown(
                significante.head(5), ["origem", "destino", "melhor_lag", "p_valor"]
            )
            + "\n_Séries anuais curtas (~10 observações): leitura exploratória._\n"
        )
        secoes.append(("Causalidade de Granger (pares significantes)", corpo))

    cruzamento = resultados.get("correlacao_idosos_patrimonial")
    if cruzamento:
        corpo = (
            f"Correlação espacial por RA no ano {cruzamento.get('ano_referencia')} "
            f"(n={cruzamento.get('n_ra')} regiões):\n\n"
            f"- Pearson: **{cruzamento.get('pearson', float('nan')):+.2f}** "
            f"(p={cruzamento.get('p_valor_pearson', float('nan')):.3f})\n"
            f"- Spearman: **{cruzamento.get('spearman', float('nan')):+.2f}** "
            f"(p={cruzamento.get('p_valor_spearman', float('nan')):.3f})\n"
            "\n_A sobreposição temporal entre as tabelas é curta; a comparação usa "
            "cross-section por Região Administrativa._\n"
        )
        secoes.append(("Violência contra idosos x crimes patrimoniais", corpo))

    for chave, titulo in (
        ("anomalias_painel", "Anomalias detectadas no painel RA x ano"),
        ("anomalias_mensal", "Anomalias na série mensal de violência contra idosos"),
    ):
        anomalias = resultados.get(chave)
        if anomalias is not None and not anomalias.empty:
            corpo = _tabela_markdown(anomalias.head(12))
            if len(anomalias) > 12:
                corpo += f"\n_Exibindo as 12 anomalias mais extremas de {len(anomalias)}._\n"
            secoes.append((titulo, corpo))

    zonas = resultados.get("zonas_quentes")
    if zonas is not None and not zonas.empty:
        corpo = (
            _tabela_markdown(zonas)
            + "\n_Valores distribuídos para a célula do centróide de cada RA._\n"
        )
        secoes.append(("Zonas quentes (malha de células)", corpo))

    mapa = resultados.get("caminho_mapa")
    if mapa:
        secoes.append(("Mapa interativo", f"Mapa de calor disponível em: `{mapa}`\n"))

    return secoes


def gerar_markdown(titulo: str, secoes: list[tuple[str, str]]) -> str:
    """Concatena seções em um documento Markdown completo."""
    partes = [f"# {titulo}", ""]
    for titulo_secao, corpo in secoes:
        partes += [f"## {titulo_secao}", "", corpo, ""]
    return "\n".join(partes)


def salvar_markdown(conteudo: str, caminho_saida: str | Path) -> Path:
    """Grava o relatório Markdown."""
    caminho = Path(caminho_saida)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text(conteudo, encoding="utf-8")
    logger.info("Relatório Markdown salvo", extra={"caminho": str(caminho)})
    return caminho


def gerar_html(titulo: str, secoes: list[tuple[str, str]], caminho_saida: str | Path) -> Path:
    """
    Gera o relatório em HTML autocontido (CSS embutido, sem dependências
    externas), pronto para compartilhamento ou impressão pelo navegador.
    """
    secoes_html = "".join(
        f"<section><h2>{_html_escape(titulo_secao)}</h2>\n{_renderizar_markdown(corpo)}</section>"
        for titulo_secao, corpo in secoes
    )
    documento = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_html_escape(titulo)}</title>
<style>{_CSS_RELATORIO}</style>
</head>
<body>
<header>
<h1>{_html_escape(titulo)}</h1>
<p>Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
</header>
<main>
{secoes_html}
</main>
</body>
</html>
"""

    caminho = Path(caminho_saida)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text(documento, encoding="utf-8")
    logger.info("Relatório HTML salvo", extra={"caminho": str(caminho)})
    return caminho


def exportar_relatorio(
    titulo: str,
    secoes: list[tuple[str, str]],
    pasta_saida: str | Path,
    nome_base: str = "relatorio_executivo",
) -> dict[str, Path]:
    """Escreve `.md` e `.html` na pasta informada e devolve os caminhos."""
    pasta = Path(pasta_saida)
    pasta.mkdir(parents=True, exist_ok=True)

    markdown = gerar_markdown(titulo, secoes)
    caminho_md = salvar_markdown(markdown, pasta / f"{nome_base}.md")
    caminho_html = gerar_html(titulo, secoes, pasta / f"{nome_base}.html")

    return {"markdown": caminho_md, "html": caminho_html}
