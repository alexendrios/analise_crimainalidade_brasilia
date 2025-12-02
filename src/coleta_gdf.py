import os
import glob
import time
import requests
from datetime import datetime
from tqdm import tqdm

from util.config_loader import get_config
from util.log import logs, fechar_loggers
from util.rotas import montar_url

config = get_config()
logger = logs()
rotas = config["rotas"]


def gerar_urls_rotas():
    """Gera a lista de URLs com seus respectivos nomes de arquivo."""
    urls = []

    for nome, rota in rotas.items():

        # Rota já possui URL explícita
        if "url" in rota:
            urls.append((rota["url"], rota["arquivo"]))

        # Construção via CKAN
        else:
            url = montar_url(
                rota["dataset"],
                rota["resource"],
                rota["arquivo"]
            )
            urls.append((url, rota["arquivo"]))

    return urls


def limpar_diretorios():
    """
    Remove arquivos CSV, XLSX e LOG antes da execução.
    Não remove diretórios, apenas os arquivos internos.
    """
    global logger

    # Fechar handlers antes de deletar arquivos de log
    fechar_loggers()

    print("Limpando diretórios...")

    pastas = {
        "./data/csv/*.csv": "Arquivos CSV",
        "./data/planilha/*.xlsx": "Arquivos XLSX",
        "./logs/*.log": "Arquivos de LOG"
    }

    for padrao, descricao in pastas.items():
        arquivos = glob.glob(padrao)

        if not arquivos:
            print(f"Nenhum {descricao} encontrado.")
            continue

        for arquivo in arquivos:
            try:
                os.remove(arquivo)
                print(f"Removido: {arquivo}")
            except Exception as e:
                print(f"Erro ao remover {arquivo}: {e}")

    # Recriar logger depois que os logs foram apagados
    logger = logs()

    logger.info("*****************************************************")
    logger.info("Limpeza concluída")
    logger.info("Diretórios limpos e logger reiniciado corretamente.")
    logger.info("*****************************************************\n")


def detectar_extensao(content_type: str):
    """Determina extensão e pasta de destino com base no Content-Type."""
    content_type = content_type.lower()

    if "csv" in content_type:
        return ".csv", "./data/csv"
    if "excel" in content_type or "spreadsheetml" in content_type:
        return ".xlsx", "./data/planilha"

    # Formato desconhecido
    return ".bin", "./data/outros"


def download_arquivo(url: str, nome_arquivo: str):
    start = time.time()
    logger.info(f"Iniciando download: {nome_arquivo}")
    logger.info(f"URL: {url}")

    file_path = None
    file_obj = None

    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "").lower()
        # Validação de tipos permitidos
        tipos_permitidos = ["csv", "excel", "spreadsheetml"]
        if not any(tp in content_type for tp in tipos_permitidos):
            logger.warning(f"Tipo de conteúdo não suportado: {content_type}")
            return None

        ext, folder = detectar_extensao(content_type)
        os.makedirs(folder, exist_ok=True)

        file_path = os.path.join(folder, f"{nome_arquivo}{ext}")
        total_size = int(response.headers.get("content-length", 0))
        total_bytes = 0

        with tqdm(total=total_size if total_size > 0 else None, unit="B", unit_scale=True,
                  desc=f"Baixando {nome_arquivo}", ncols=80) as progress:

            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    if file_obj is None:
                        file_obj = open(file_path, "wb")
                    file_obj.write(chunk)
                    progress.update(len(chunk))
                    total_bytes += len(chunk)

        if file_obj:
            file_obj.close()

        # Arquivo vazio -> remove e retorna None
        if total_bytes == 0:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
            logger.warning("Arquivo vazio, download abortado.")
            return None

        logger.info("Download concluído com sucesso!")
        return file_path

    except Exception as e:
        logger.error(f"Erro no download: {e}", exc_info=True)
        if file_obj:
            try:
                    file_obj.close()
            except:
               pass
            if file_path:
                try:
                    os.remove(file_path)
                except Exception:
                    pass  # ignora se não existir
            return None


    finally:
        tempo_total = time.time() - start
        logger.info(f"Tempo total: {tempo_total:.2f} segundos")
        logger.info("*****************************************************\n")




def coleta_dados():
    limpar_diretorios()

    start = datetime.now()
    logger.info(f"Processo iniciado em {start.strftime('%Y-%m-%d %H:%M:%S')}")

    urls_rotas = gerar_urls_rotas()

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


if __name__ == "__main__":
    coleta_dados()
    
