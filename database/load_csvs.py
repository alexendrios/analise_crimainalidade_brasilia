import pandas as pd
from database.repository.repository import salvar_tabela, logger

arquivos = {
    "./data/output/crimes-contra-mulher_tratado.csv": "crimes_contra_mulher",
    "./data/output/desaparecidos_idade_sexo_tratado.csv": "desaparecidos_idade_sexo",
    "./data/output/desaparecimento-localizados_tratado.csv": "desaparecimento_localizados",
    "./data/output/desaparecimento-regiao_tratado.csv": "desaparecimento_regiao",
    "./data/output/df_analise_populacao.csv": "analise_populacao",
    "./data/output/feminicidio.csv": "feminicidio",
    "./data/output/furto_em_veiculo_tratado.csv": "furto_em_veiculo",
    "./data/output/homicidio_tratado.csv": "homicidio",
    "./data/output/idosos_2016_tratado.csv": "idosos",
    "./data/output/idosos_mensais_tratado.csv": "idosos_mensais",
    "./data/output/idosos_tabela4.csv": "idosos_ocorrencias",
    "./data/output/idosos_tabela5.csv": "idosos_sexo",
    "./data/output/injuria_racial_tratado.csv": "injuria_racial",
    "./data/output/latrocinio_tratado.csv": "latrocinio",
    "./data/output/lesao_corporal_morte_tratada.csv": "lesao_corporal_morte",
    "./data/output/ra_df_populacao_tratado.csv": "populacao"
}


def carregar_csv_para_banco():
    for arquivo in arquivos:
        try:
            df = pd.read_csv(arquivo,sep=';', encoding='utf-8')
            logger.info(f"Arquivo {arquivo} lido com sucesso.")
            print(df.head(10))
        except Exception as e:
            logger.error(f"Erro ao carregar o arquivo {arquivo}: {e}")
     

if __name__ == "__main__":
    carregar_csv_para_banco()
