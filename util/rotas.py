from util.config_loader import get_config

config = get_config()

def montar_url(dataset: str, resource: str, arquivo: str):
    """
    Monta uma URL completa no padrão CKAN / Dados Abertos.
    Exemplo:
      base_url/dataset/<dataset>/resource/<resource>/download/<arquivo>
    """

    base_url = config["coleta"]["fontes"]["dados_abertos"]["url"]
    
    return f"{base_url}/dataset/{dataset}/resource/{resource}/download/{arquivo}"
