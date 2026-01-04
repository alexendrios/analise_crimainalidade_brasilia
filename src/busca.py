from util.arquivos import limpar_diretorios
from src.coleta_gdf import coleta_dados
from name_arquivos.names import (
    nome_arquivo_busca_dados_abertos as nomes_dados_abertos,
    nome_arquivo_busca_ibge as nomes_ibge,
)
from util.config_loader import get_config
from util.log import logs
from datetime import datetime


config = get_config()
logger = logs()

def coletar_dados_():
    # Limpa diretórios e registra no log
    limpar_diretorios()

    start = datetime.now()
    
    logger.info("*****************************************************")   
    logger.info(f"Processo geral iniciado em {start.strftime('%Y-%m-%d %H:%M:%S')}")
    # Rotas e URLs
    rotas_abertos = config["rotas"]
    url_abertos = config["coleta"]["fontes"]["dados_abertos"]["url"]

    rotas_ibge = config["rotas_ibge"]
    url_ibge = config["coleta"]["fontes"]["dados_ibge"]["url"]

    # Execução das coletas
    logger.info(f"Iniciando a busca de Dados: {url_abertos}\n")
    coleta_dados(url_abertos, nomes_dados_abertos, rotas_abertos)
    logger.info(f"Iniciando a busca de Dados: {url_ibge}\n...")
    coleta_dados(url_ibge, nomes_ibge, rotas_ibge)
    end = datetime.now()
    
    logger.info("*****************************************************")
    logger.info("Todas as coletas de dados concluídas.")
    logger.info("Resumo dos processos:")
    logger.info(f"Início geral:  {start.strftime('%H:%M:%S')}")
    logger.info(f"finalizado em: {end.strftime('%H:%M:%S')}")
    logger.info(f"Duração total: {end - start}")
    logger.info("*****************************************************")


