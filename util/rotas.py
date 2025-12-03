from util.config_loader import get_config

config = get_config()
rotas = config["rotas"]

def montar_url(dataset: str, resource: str, arquivo: str):
    """
    Monta uma URL completa no padrão CKAN / Dados Abertos.
    Exemplo:
      base_url/dataset/<dataset>/resource/<resource>/download/<arquivo>
    """

    base_url = config["coleta"]["fontes"]["dados_abertos"]["url"]
    
    return f"{base_url}/dataset/{dataset}/resource/{resource}/download/{arquivo}"


def gerar_urls_rotas(rotas_dict=None):
    """Gera a lista de URLs com seus respectivos nomes de arquivo."""
    urls = []
    rotas_dict = rotas_dict or rotas  # usa rotas padrão se não passar nada

    for nome, rota in rotas_dict.items():
        if "url" in rota:
            urls.append((rota["url"], rota["arquivo"]))
        else:
            url = montar_url(
                rota["dataset"],
                rota["resource"],
                rota["arquivo"]
            )
            urls.append((url, rota["arquivo"]))
    return urls
