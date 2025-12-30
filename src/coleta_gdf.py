from datetime import datetime
from util.arquivos import download_arquivo
from util.log import logs
from util.rotas import gerar_urls_rotas

logger = logs()


def coleta_dados(url_, lista_nomes, rotas):
    """Função principal para coletar dados das URLs definidas."""

    start = datetime.now()
    logger.info(f"Processo iniciado em {start.strftime('%Y-%m-%d %H:%M:%S')}")

    urls_rotas = gerar_urls_rotas(url_, rotas)

    planilhas = lista_nomes

    # Segurança contra índice fora da lista
    if len(urls_rotas) != len(planilhas):
        logger.warning(
            f"ATENÇÃO: {len(urls_rotas)} URLs mas {len(planilhas)} nomes de arquivos!"
        )

    for i, (url, _) in enumerate(urls_rotas):
        file_name = planilhas[i] if i < len(planilhas) else f"arquivo_{i+1}"
        logger.info(f"({i+1}/{len(urls_rotas)}) Baixando: {file_name}")

        download_arquivo(url, nome_arquivo=file_name)

    end = datetime.now()
    logger.info(f"Processo finalizado em {end.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Duração total: {end - start}")



