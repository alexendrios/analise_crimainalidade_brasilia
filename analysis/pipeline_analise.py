# analysis/pipeline_analise.py
"""
Pipeline de modelagem e análise executiva.

Orquestra os quatro produtos do roadmap de análise:

1. Correlação multivariada entre tipos de crime (+ causalidade de Granger);
2. Correlação espacial entre tabelas gold (violência contra idosos x
   crimes patrimoniais, cross-section por RA);
3. Detecção de anomalias com Isolation Forest (painel RA x ano e série
   mensal de idosos);
4. Mapa de calor geoespacial (Folium) + relatório executivo em Markdown/PDF.

Execução: `python -m analysis.pipeline_analise`
"""

from pathlib import Path

PASTA_PADRAO = Path("./data/analises")

MODULO = "analysis.pipeline_analise"


def _carregar_tabelas():
    """Carrega as tabelas gold necessárias via Repository (import tardio para testes)."""
    from ingestion.repository_adapter import Repository

    nomes = (
        "violencia_contra_mulher_gold",
        "crimes_roubo_furto_gold",
        "crimes_letais_gold",
        "crimes_discriminatorios_gold",
        "violencia_idosos_gold",
        "violencia_idosos_mensais_gold",
    )
    return {nome: Repository.load(nome) for nome in nomes}


def executar_analise(pasta_saida=PASTA_PADRAO) -> dict:
    """
    Executa as análises completas e exporta mapa + relatório.

    :return: dicionário com caminhos dos artefatos e resumos das análises.
    """
    from analysis.anomalias import detectar_anomalias, detectar_anomalias_painel, resumo_anomalias
    from analysis.correlacoes import (
        construir_matriz_indicadores,
        correlacao_idosos_patrimoniais,
        causalidade_granger,
        insights_correlacao,
        matriz_correlacao,
        pares_mais_correlacionados,
    )
    from analysis.mapa import gerar_agregado_celulas, gerar_mapa_calor, salvar_mapa
    from analysis.relatorio import exportar_relatorio, montar_relatorio
    from util.log import logs

    logger = logs()
    pasta = Path(pasta_saida)
    pasta.mkdir(parents=True, exist_ok=True)

    logger.info("Pipeline de análise iniciado")
    dados = _carregar_tabelas()

    # 1. Correlações temporais multivariadas + Granger
    matriz = construir_matriz_indicadores(dados)
    correlacao = matriz_correlacao(matriz)
    granger = causalidade_granger(matriz)
    pares = pares_mais_correlacionados(correlacao)

    # 2. Idosos x patrimoniais (cross-section por RA)
    cruzamento = correlacao_idosos_patrimoniais(
        dados["violencia_idosos_gold"], dados["crimes_roubo_furto_gold"]
    )

    # 3. Anomalias (painel RA x ano + série mensal de idosos)
    painel = dados["crimes_roubo_furto_gold"].drop(columns=["inserido_em"], errors="ignore")
    painel_marcado = detectar_anomalias_painel(painel, "ocorrencia_roubo_pedestre")
    anomalias_painel = resumo_anomalias(painel_marcado, ("regiao_administrativa",))

    mensais_marcado = detectar_anomalias(dados["violencia_idosos_mensais_gold"], "fato")
    anomalias_mensal = resumo_anomalias(mensais_marcado)

    # 4. Mapa de calor + zonas quentes
    agregado = gerar_agregado_celulas(
        dados["crimes_roubo_furto_gold"], "ocorrencia_roubo_pedestre"
    )
    mapa = gerar_mapa_calor(
        agregado,
        "ocorrencia_roubo_pedestre",
        "Roubo a pedestre por célula - último ano",
    )
    caminho_mapa = salvar_mapa(mapa, pasta / "mapa_calor_roubo_pedestre.html")

    zonas_quentes = agregado.sort_values("ocorrencia_roubo_pedestre", ascending=False).head(10)

    # 5. Relatório executivo
    resultados = {
        "pares_correlacao": pares,
        "insights": insights_correlacao(correlacao, granger),
        "granger": granger,
        "correlacao_idosos_patrimonial": cruzamento,
        "anomalias_painel": anomalias_painel,
        "anomalias_mensal": anomalias_mensal,
        "zonas_quentes": zonas_quentes,
        "caminho_mapa": caminho_mapa,
    }
    secoes = montar_relatorio(resultados)
    artefatos = exportar_relatorio(
        "Relatório Executivo - Criminalidade Brasília/DF", secoes, pasta
    )

    resumo = {
        **artefatos,
        "caminho_mapa": caminho_mapa,
        "indicadores": int(matriz.shape[1]),
        "anos": f"{matriz.index.min():.0f}-{matriz.index.max():.0f}",
        "anomalias_painel": int(len(anomalias_painel)),
        "anomalias_mensal": int(len(anomalias_mensal)),
        "top_zonas_quentes": [
            str(celula) for celula in zonas_quentes["celula_id"].head(3)
        ],
    }
    logger.info("Pipeline de análise finalizado", extra={"artefatos": str(artefatos)})
    return resumo


if __name__ == "__main__":
    _saida = executar_analise()
    for chave, valor in _saida.items():
        print(f"{chave}: {valor}")
