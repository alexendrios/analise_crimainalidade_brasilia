def montar_url(url: str, dataset: str, resource: str, arquivo: str):
    """
    Monta uma URL completa no padrão CKAN / Dados Abertos.
    Exemplo:
      base_url/dataset/<dataset>/resource/<resource>/download/<arquivo>
    """

    base_url = url
    
    return f"{base_url}/{dataset}/{resource}/{arquivo}"


def gerar_urls_rotas(url_: str,rotas_dict=None, rotas={}):
    """Gera a lista de URLs com seus respectivos nomes de arquivo."""
    urls = []
    rotas_dict = rotas_dict or rotas  # usa rotas padrão se não passar nada

    for nome, rota in rotas_dict.items():
        if "url" in rota:
            urls.append((rota["url"], rota["arquivo"]))
        else:
            url = montar_url(
                url_,
                rota["dataset"],
                rota["resource"],
                rota["arquivo"]
            )
            urls.append((url, rota["arquivo"]))
    return urls
