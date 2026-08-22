# analysis/relatorio.py
"""
Exportação de relatório executivo (Markdown + PDF) com os insights das
análises de correlação, anomalias e zonas quentes.

O relatório é montado a partir de um dicionário de resultados produzido pelo
pipeline de análise (`analysis.pipeline_analise`), mantendo este módulo
independente da fonte dos dados e fácil de testar.
"""

from pathlib import Path

import pandas as pd
from fpdf import FPDF
from fpdf.enums import XPos, YPos

from util.log import logs

logger = logs()


def localizar_fonte_unicode() -> Path | None:
    """
    Procura uma fonte TTF com acentuação completa. Prefere a DejaVuSans
    distribuída com o matplotlib do venv; aceita fallback do sistema Windows.
    """
    candidatos = []
    try:
        import matplotlib

        candidatos.append(
            Path(matplotlib.__file__).parent / "mpl-data" / "fonts" / "ttf" / "DejaVuSans.ttf"
        )
    except ImportError:
        pass
    candidatos.append(Path("C:/Windows/Fonts/arial.ttf"))

    for candidato in candidatos:
        if candidato.exists():
            return candidato
    return None


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


def _celula(pdf: FPDF, altura: float, texto: str):
    """multi_cell que sempre retorna o cursor para a margem esquerda."""
    pdf.multi_cell(0, altura, texto, new_x=XPos.LMARGIN, new_y=YPos.NEXT)


def gerar_pdf(titulo: str, secoes: list[tuple[str, str]], caminho_saida: str | Path) -> Path:
    """
    Gera o PDF do relatório executivo.

    Usa fonte TTF (DejaVuSans/Arial) para acentuação correta quando
    disponível; caso contrário cai para a core Helvetica.
    """
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)

    fonte = localizar_fonte_unicode()
    nome_fonte = "Helvetica"
    if fonte is not None:
        pdf.add_font("Relatorio", "", str(fonte))
        nome_fonte = "Relatorio"

    pdf.add_page()

    pdf.set_font(nome_fonte, size=17)
    _celula(pdf, 10, titulo)
    pdf.ln(2)

    for titulo_secao, corpo in secoes:
        pdf.set_font(nome_fonte, size=13)
        _celula(pdf, 8, titulo_secao)
        pdf.set_font(nome_fonte, size=10)
        for paragrafo in corpo.splitlines():
            _celula(pdf, 5, paragrafo if paragrafo.strip() else " ")
        pdf.ln(3)

    caminho = Path(caminho_saida)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(caminho))
    logger.info("Relatório PDF salvo", extra={"caminho": str(caminho)})
    return caminho


def exportar_relatorio(
    titulo: str,
    secoes: list[tuple[str, str]],
    pasta_saida: str | Path,
    nome_base: str = "relatorio_executivo",
) -> dict[str, Path]:
    """Escreve `.md` e `.pdf` na pasta informada e devolve os caminhos."""
    pasta = Path(pasta_saida)
    pasta.mkdir(parents=True, exist_ok=True)

    markdown = gerar_markdown(titulo, secoes)
    caminho_md = salvar_markdown(markdown, pasta / f"{nome_base}.md")
    caminho_pdf = gerar_pdf(titulo, secoes, pasta / f"{nome_base}.pdf")

    return {"markdown": caminho_md, "pdf": caminho_pdf}
