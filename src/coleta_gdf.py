import requests
import time
import os
import logging
from datetime import datetime
from tqdm import tqdm 
import glob


def limpar_diretorios():
    """
    Remove arquivos CSV, XLSX e LOG dos diretórios antes de rodar o script.
    Não remove os diretórios, apenas os arquivos.
    """
    pastas = {
        "./data/csv/*.csv": "Arquivos CSV",
        "./data/planilha/*.xlsx": "Arquivos XLSX",
        "./logs/*.log": "Arquivos de LOG"
    }

    print("Limpando diretórios antes da execução...")

    for padrao, descricao in pastas.items():
        arquivos = glob.glob(padrao)
        if arquivos:
            for arquivo in arquivos:
                try:
                    os.remove(arquivo)
                    print(f"Removido: {arquivo}")
                except Exception as e:
                    print(f"Erro ao remover {arquivo}: {e}")
        else:
            print(f"Nenhum {descricao} encontrado para apagar.")
            
def download_arquivo(url: str, nome_arquivo: str):
    """
    Faz o download de arquivo com:
    - nome personalizado
    - detecção automática de extensão
    - barra de progresso
    - logging completo
    - salvamento em ./data/csv/ ou ./data/planilha/
    """

    start = time.time()

    logger.info("*****************************************************")
    logger.info(f"Iniciando download: {nome_arquivo}")
    logger.info(f"URL: {url}")

    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "").lower()
        logger.info(f"Content-Type detectado: {content_type}")

        if "csv" in content_type:
            ext = ".csv"
            folder = "./data/csv"
        elif "excel" in content_type or "spreadsheetml" in content_type:
            ext = ".xlsx"
            folder = "./data/planilha"
        else:
            ext = ".bin"
            folder = "./data/outros"
            logger.warning("Tipo desconhecido. Salvando em ./data/outros")

        os.makedirs(folder, exist_ok=True)

        file_path = os.path.join(folder, f"{nome_arquivo}{ext}")
        logger.info(f"Arquivo será salvo em: {file_path}")

        total_size = int(response.headers.get("content-length", 0))
        logger.info(f"Tamanho total: {total_size / (1024*1024):.2f} MB")

        with open(file_path, "wb") as file, tqdm(
            total=total_size,
            unit='B',
            unit_scale=True,
            desc=f'Baixando {nome_arquivo}',
            ncols=80
        ) as progress:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
                    progress.update(len(chunk))

        logger.info("Download concluído!")

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro durante o download: {e}", exc_info=True)
        return

    end = time.time()
    tempo_total = end - start

    logger.info(f"Tempo total: {tempo_total:.2f} segundos ({tempo_total/60:.2f} minutos)")
    logger.info("*****************************************************\n")


if __name__ == "__main__":

    limpar_diretorios()
    
    LOG_DIR = "./logs"
    os.makedirs(LOG_DIR, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(f"{LOG_DIR}/download_gdf.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

    start = datetime.now()
    logger = logging.getLogger(__name__)
    logger.info(f"Início do processo de download de arquivos GDF {start.strftime('%Y-%m-%d %H:%M:%S')}")
    urls = [
        "https://dados.df.gov.br/dataset/3435c8b7-5d61-4541-bf1c-38bdf9e34fd8/resource/cddb5da1-8ba4-444d-987b-a62174871025/download/tabelasseriehistorica-roubo-a-transeunte.xlsx",
        "https://dados.df.gov.br/dataset/3435c8b7-5d61-4541-bf1c-38bdf9e34fd8/resource/a04ccfa3-1b3f-4bc4-90f8-08e56b1db1f9/download/tabelasseriehistorica-roubo-de-veiculo.xlsx",
        "https://dados.df.gov.br/dataset/3435c8b7-5d61-4541-bf1c-38bdf9e34fd8/resource/9facc622-28c0-4585-a283-a864d75943ec/download/tabelasseriehistorica-roubo-em-transporte-coletivo.xlsx",
        "https://dados.df.gov.br/dataset/3435c8b7-5d61-4541-bf1c-38bdf9e34fd8/resource/59d96053-aefb-419b-a390-381a3ea2bf6d/download/tabelasseriehistorica-roubo-em-comercio.xlsx",
        "https://dados.df.gov.br/dataset/3435c8b7-5d61-4541-bf1c-38bdf9e34fd8/resource/dae1535d-f670-4767-9125-50d6f74063bf/download/tabelasseriehistorica-furto-em-veiculo.xlsx",
        "https://dados.df.gov.br/dataset/7a9ad101-f02a-4179-8a25-0ba2803bf5cb/resource/934a4892-5805-4e01-ac05-4c0fd1c4f25e/download/tabelasseriehistorica-injuria-racial.xlsx",
        "https://dados.df.gov.br/dataset/7a9ad101-f02a-4179-8a25-0ba2803bf5cb/resource/ec9f009f-092f-43e7-9459-1581dad9f396/download/tabelasseriehistorica-racismo.xlsx",
        "https://feminicidio.ssp.df.gov.br/extensions/feminicidio/dados/dados-abertos.xlsx",
        "https://dados.df.gov.br/dataset/3f533c6b-fc09-4597-8054-88bee43b43e8/resource/0432d3e9-0502-4f99-893f-85139be1ff79/download/tabelasseriehistorica-homicidio.xlsx",
        "https://dados.df.gov.br/dataset/3f533c6b-fc09-4597-8054-88bee43b43e8/resource/2a4d28d2-af50-4f1a-87d0-e00e3eaec579/download/tabelasseriehistorica-latrocinio.xlsx",
        "https://dados.df.gov.br/dataset/3f533c6b-fc09-4597-8054-88bee43b43e8/resource/cd60e8e8-6e1c-4852-88f0-57d1ccbbfcb8/download/tabelasseriehistorica-lcsm.xlsx",
        "https://dados.df.gov.br/dataset/3f533c6b-fc09-4597-8054-88bee43b43e8/resource/de08b125-22ca-47ec-ba84-384e5b6efc33/download/tabelasseriehistorica-feminicidio.xlsx",
        "https://dados.df.gov.br/dataset/26328d91-8541-49cc-95de-357b35549f49/resource/f84bfd02-9b30-4358-93ab-7a1f59bc4aa2/download/comparativo-janago-201617-por-ra.csv",
        "https://dados.df.gov.br/dataset/26328d91-8541-49cc-95de-357b35549f49/resource/cee6714a-9fb2-4867-9df3-9ead79ed4cbf/download/violencia-contra-idosos-no-df---dados-mensais.csv",
        "https://dados.df.gov.br/dataset/26328d91-8541-49cc-95de-357b35549f49/resource/cdc0c0cb-6fbd-4810-8ee4-6b33ef03e531/download/violencia-contra-idosos-no-df---ultimos-7-anos.csv",
        "https://dados.df.gov.br/dataset/5f507b65-847c-48cd-8958-5e92bd4aa5b3/resource/48f9e674-d198-4b7d-bd83-0eea9516d687/download/desaparecimento-de-pessoas-por-rajandez-20202021.csv",
        "https://dados.df.gov.br/dataset/5f507b65-847c-48cd-8958-5e92bd4aa5b3/resource/53b24562-a766-462e-addc-6313fa66cd6d/download/desaparecimento-de-pessoasidade-e-sexojandez-20172021.csv",
        "https://dados.df.gov.br/dataset/5f507b65-847c-48cd-8958-5e92bd4aa5b3/resource/b503fb57-5f6c-4bd1-996d-83220d53119e/download/desaparecimento-de-pessoaslocalizadosjandez-2021.csv"
    ]

    planilhas = [
        "roubo-a-transeunte",
        "roubo-de-veiculo",
        "roubo-em-transporte-coletivo",
        "roubo-em-comercio",
        "furto-em-veiculo",
        "injuria-racial",
        "racismo",
        "crimes-contra-mulher",
        "homicidio",
        "latrocinio",
        "lesao-corporal-morte",
        "feminicidio",
        "idosos_2016",
        "idosos_mensais",
        "idosos_7_anos",
        "desaparecimento-regiao",
        "desaparecimento-idade-sexo",
        "desaparecimento-localizados"
    ]

    for i, url in enumerate(urls):
        file_name = planilhas[i]
        logger.info(f"Baixando arquivo {i+1} de {len(urls)}...")
        download_arquivo(url, nome_arquivo=file_name)
    
    end = datetime.now()
    logger.info(f"Fim do processo de download de arquivos GDF {end.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Duração total: {end - start}")