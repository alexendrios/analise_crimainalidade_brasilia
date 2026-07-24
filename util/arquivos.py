import glob
import os
import time
from time import sleep
import requests
from tqdm import tqdm
from util.config_loader import get_config
from util.log import logs, fechar_loggers
from urllib.parse import urlparse



config = get_config()
logger = logs()

def limpar_diretorios():
    """
    Remove arquivos CSV, XLSX e LOG antes da execução.
    Não remove diretórios, apenas os arquivos internos.
    """
        # Fechar handlers antes de deletar arquivos de log
    fechar_loggers()

    print("Removendo diretórios...")

    pastas = {
        "./data/bronze/csv/*.csv": "Arquivos CSV",
        "./data/bronze/planilha/*.xlsx": "Arquivos XLSX",
        "./data/bronze/planilha/*.xls": "Arquivos XLS",
        "./data/bronze/planilha/*.pdf": "Arquivos PDF",
        "./data/bronze/zip/*.zip": "Arquivos ZIP",
        "./logs/*.log": "Arquivos de LOG",
        "./data/silver/output/*.csv": "Arquivos CSV de saída",
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
            except Exception as e: # pragma: no cover
                print(f"Erro ao remover {arquivo}: {e}")

    # Recriar logger depois que os logs foram apagados
    logger = logs()

    logger.info("*****************************************************")
    logger.info("Limpeza concluída")
    logger.info("Diretórios limpos e logger reiniciado corretamente.")
    logger.info("*****************************************************\n")
    return logger

def download_arquivo(url: str, nome_arquivo: str):
    start = time.time()
    logger.info(f"Iniciando download: {nome_arquivo}")
    logger.info(f"URL: {url}")

    file_path = None
    file_obj = None

    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        # Aguarda 4 segundo antes de iniciar a requisição
        sleep(4)
        
        response = requests.get(
            url, headers=headers, stream=True, timeout=80, allow_redirects=True
        )

        logger.info(f"Status Code: {response.status_code}")
        logger.info(f"URL final: {response.url}")
        logger.info(f"Content-Type: {response.headers.get('Content-Type')}")

        response.raise_for_status()
        
        content_type = response.headers.get("Content-Type", "").lower()

        # Se a API retornou JSON, registra a mensagem e interrompe
        if "application/json" in content_type:
            logger.error(f"Resposta da API: {response.text}")
            return None

        # Tipos permitidos
        tipos_permitidos = [
            "csv",
            "excel",
            "spreadsheetml",
            "zip",
            "compressed",
            "octet-stream",
        ]

        if not any(tp in content_type for tp in tipos_permitidos):
            logger.warning(f"Tipo de conteúdo não suportado: {content_type}")
            return None

        # Detecta extensão real
        # Detecta extensão real
        ext, folder = detectar_extensao(response, nome_arquivo)
        os.makedirs(folder, exist_ok=True)

        base, _ = os.path.splitext(nome_arquivo)
        if not base:
          base = nome_arquivo

        file_path = os.path.join(folder, f"{base}{ext}")
        total_size = int(response.headers.get("content-length", 0))
        total_bytes = 0

        with tqdm(
            total=total_size if total_size > 0 else None,
            unit="B",
            unit_scale=True,
            desc=f"Baixando {nome_arquivo}",
            ncols=80,
        ) as progress:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    if file_obj is None:
                        file_obj = open(file_path, "wb")

                    file_obj.write(chunk)
                    progress.update(len(chunk))
                    total_bytes += len(chunk)

        if file_obj:
            file_obj.close()

        if total_bytes == 0:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

            logger.warning("Arquivo vazio, download abortado.")
            return None

        logger.info(f"Arquivo salvo em: {file_path}")
        logger.info("Download concluído com sucesso!")
        return file_path

    except requests.HTTPError:
        logger.error(
            f"Erro HTTP {response.status_code}: {response.text}", exc_info=True
        )

    except Exception as e:
        logger.error(f"Erro no download: {e}", exc_info=True)

    finally:
        if file_obj and not file_obj.closed:
            file_obj.close()

        if file_path and os.path.exists(file_path) and os.path.getsize(file_path) == 0:
            os.remove(file_path)

        tempo_total = time.time() - start
        logger.info(f"Tempo total: {tempo_total:.2f} segundos")
        logger.info("*****************************************************\n")

    return None 

def detectar_extensao(response, nome_arquivo):
    """
    Determina a extensão do arquivo utilizando, nesta ordem:
    1. nome_arquivo informado;
    2. URL final;
    3. Content-Disposition;
    4. Content-Type.
    """

    # 1. Nome informado pela aplicação
    _, ext = os.path.splitext(nome_arquivo)

    # 2. URL final
    if not ext:
        path = urlparse(response.url).path
        _, ext = os.path.splitext(path)

    # 3. Content-Disposition
    if not ext:
        content_disposition = response.headers.get("Content-Disposition", "")

        if "filename=" in content_disposition:
            filename = content_disposition.split("filename=")[-1].strip('"')
            _, ext = os.path.splitext(filename)

    ext = ext.lower()

    # 4. Content-Type (último recurso)
    if not ext:
        content_type = response.headers.get("Content-Type", "").lower()

        if "csv" in content_type:
            ext = ".csv"

        elif "spreadsheetml" in content_type:
            ext = ".xlsx"

        elif "excel" in content_type:
            ext = ".xls"

        elif "zip" in content_type or "compressed" in content_type:
            ext = ".zip"

        elif "pdf" in content_type:
            ext = ".pdf"

        else:
            ext = ".bin"

    pastas = {
        ".csv": "./data/bronze/csv",
        ".xlsx": "./data/bronze/planilha",
        ".xls": "./data/bronze/planilha",
        ".pdf":  "./data/bronze/outros",
        ".zip": "./data/bronze/zip",
        ".bin": "./data/bronze/outros",
    }

    return ext, pastas.get(ext, "./data/bronze/outros")