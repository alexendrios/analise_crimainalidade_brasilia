# src/pepiline.py
from src.busca import coletar_dados_
from src.scraping import obter_dados_ra_populacao
from util.extrator_zip import arquivos_zip_execucao
from util.leitor_excel import processar_populacao, processar_crimes
from util.log import logs
from pathlib import Path
from src.tratamento_populacao_df_csv import analisar_populacao
from src.tratamento_crimes_csv import (
    tratar_feminicidio, 
    tratar_desaparecidos_idade_sexo, 
    tratar_desaparecidos_localizados, 
    tratar_desaparecidos_regiao,
    tratar_furto_veiculo,
    tratar_crimes_contra_mulher,
    tratar_homicidio,
    tratar_violencia_idosos,
    tratar_crimes_idosos_ranking,
    crimes_idosos_por_mes
    
    )

logger = logs()


def main():
    logger.info("Iniciando pipeline...")

    # coleta de dados
    # coletar_dados_()
    # arquivos_zip_execucao()

    #  Obtento dados da polpulaão população
    # processar_populacao()
    # obter_dados_ra_populacao()

    # crimes
    # caminho_planilhas = Path("./data/planilha")
    # caminho_saida = Path("./data/csv")
    # caminho_saida.mkdir(parents=True, exist_ok=True)  # garante que a pasta exista
    # processar_crimes(caminho_planilhas, caminho_saida)
    
    # logger.info("Criando CSV da Popolação do DF e algumas análises...")
    
    # Gerando  artefatos -> Onde o mesno será utilizado no DB
    # analisar_populacao()
    # logger.info("Ciando dados para CSV de população por RA...")
    
    # arquivo_entr_reg_adm_df = Path("./data/csv/ra_df_populacao.csv")
    #arquivo_saida_reg_adm_df = Path("./data/output/ra_df_populacao_salvo.csv")
    
    # crimes contra mulher
    logger.info("Ciando dados para CSV de crimes contra mulher por RA...")
    tratar_crimes_contra_mulher(
        "./data/csv/crimes-contra-mulher.csv",
        "./data/output/crimes-contra-mulher_tratado.csv",
    )
 

    # feminicidio
    logger.info("Ciando dados para CSV de feminicídio por RA...")

    arquivo_entr_feminicidio = Path("./data/csv/feminicidio.csv")
    arquivo_saida_feminicidio = Path("./data/output/feminicidio.csv")    
 
    tratar_feminicidio(
        arquivo_entr_feminicidio, arquivo_saida_feminicidio
    )
    
    ARQUIVO_ENTRADA = Path("./data/csv/desaparecimento-idade-sexo.csv")
    ARQUIVO_SAIDA = Path("./data/output/desaparecidos_idade_sexo_tratado.csv")
    tratar_desaparecidos_idade_sexo(ARQUIVO_ENTRADA, ARQUIVO_SAIDA)
    

    ARQUIVO_ENTRADA = Path("./data/csv/desaparecimento-localizados.csv")
    ARQUIVO_SAIDA = Path("./data/output/desaparecimento-localizados_tratado.csv")
    tratar_desaparecidos_localizados(ARQUIVO_ENTRADA, ARQUIVO_SAIDA)
    
    
    
    ARQUIVO_ENTRADA = Path("./data/csv/desaparecimento-regiao.csv")
    ARQUIVO_SAIDA = Path("./data/output/desaparecimento-regiao_tratado.csv")
    
    tratar_desaparecidos_regiao(ARQUIVO_ENTRADA, ARQUIVO_SAIDA)
    
    
    ARQUIVO_ENTRADA = Path("./data/csv/furto-em-veiculo.csv")
    ARQUIVO_SAIDA = Path("./data/output/furto_em_veiculo_tratado.csv")
    tratar_furto_veiculo(ARQUIVO_ENTRADA, ARQUIVO_SAIDA)
    

    
    tratar_homicidio("./data/csv/homicidio.csv", "./data/output/homicidio_tratado.csv")
    
    tratar_violencia_idosos(
        "./data/csv/idosos_7_anos.csv",
        ["./data/output/idosos_tabela4.csv", "./data/output/idosos_tabela5.csv"],
    )


    tratar_crimes_idosos_ranking(
        "./data/csv/idosos_2016.csv", "./data/output/idosos_2016_tratado.csv"
    )
    
    crimes_idosos_por_mes("./data/csv/idosos_mensais.csv", ['registro', 'fato'], "./data/output/idosos_mensais_tratado.csv")
    logger.info("Pipeline finalizado com sucesso!")
    logger.info("===== FIM DO PROCESSO =====")
    
if __name__ == "__main__":  # pragma: no cover
    main()
