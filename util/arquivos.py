import glob
import os
import time
import requests
from tqdm import tqdm
from util.config_loader import get_config
from util.log import logs, fechar_loggers
config = get_config()
logger = logs()

def limpar_diretorios():
    """
    Remove arquivos CSV, XLSX e LOG antes da execução.
    Não remove diretórios, apenas os arquivos internos.
    """
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
            except Exception as e: # pragma: no cover
                print(f"Erro ao remover {arquivo}: {e}")

    # Recriar logger depois que os logs foram apagados
    logger = logs()

    logger.info("*****************************************************")
    logger.info("Limpeza concluída")
    logger.info("Diretórios limpos e logger reiniciado corretamente.")
    logger.info("*****************************************************\n")
    return logger

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
                os.remove(file_path) # pragma: no cover
            logger.warning("Arquivo vazio, download abortado.")
            return None

        logger.info("Download concluído com sucesso!")
        return file_path

    except Exception as e:
        logger.error(f"Erro no download: {e}", exc_info=True)
        if file_obj:
            try:
                    file_obj.close()
            except: # pragma: no cover
               pass 
            if file_path:
                try:
                    os.remove(file_path)
                except Exception: # pragma: no cover
                    pass  # ignora se não existir
            return None


    finally:
        tempo_total = time.time() - start
        logger.info(f"Tempo total: {tempo_total:.2f} segundos")
        logger.info("*****************************************************\n")