# src/pepiline.py
from src.busca import coletar_dados_
from src.scraping import obter_dados_ra_populacao
from util.extrator_zip import arquivos_zip_execucao
from util.leitor_excel import processar_populacao, processar_crimes
from util.log import logs
from pathlib import Path

logger = logs()


def main():
    logger.info("Iniciando pipeline...")

    # coleta de dados
    coletar_dados_()
    arquivos_zip_execucao()

    # população
    processar_populacao()
    obter_dados_ra_populacao()

    # crimes
    caminho_planilhas = Path("./data/planilha")
    caminho_saida = Path("./data/csv")
    caminho_saida.mkdir(parents=True, exist_ok=True)  # garante que a pasta exista
    processar_crimes(caminho_planilhas, caminho_saida)

    logger.info("Pipeline finalizado com sucesso!")
    logger.info("===== FIM DO PROCESSO =====")


if __name__ == "__main__":  # pragma: no cover
    main()
