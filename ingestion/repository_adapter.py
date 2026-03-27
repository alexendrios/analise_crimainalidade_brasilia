from database.repository.repository import carregar_tabela, inserir_dados

class Repository:
    @staticmethod
    def load(nome):
        return carregar_tabela(nome)

    @staticmethod
    def save(df, nome):
        inserir_dados(df, nome)