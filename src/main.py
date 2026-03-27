from src.pipeline_busca_transformacao import busca_transformacao_dados
from src.pipeline_tabela_gold import criar_tabela_gold


if __name__ == "__main__":
    
    busca_transformacao_dados()
    criar_tabela_gold(max_workers=6)