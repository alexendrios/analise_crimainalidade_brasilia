import zipfile
import os
from util.log import logs

logger = logs()

diretorio_zip = "./data/zip"
diretorio_destino = "./data/planilha"

os.makedirs(diretorio_destino, exist_ok=True)
logger.info(f"Diretório de destino garantido: {diretorio_destino}")


def extrair_zip_seguro(zip_path, destino):
    logger.info(f"Iniciando extração segura do ZIP: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.namelist():
            caminho = os.path.abspath(os.path.join(destino, member))
            if not caminho.startswith(os.path.abspath(destino)):
                logger.error(
                    f"Tentativa de Zip Slip detectada no arquivo {zip_path} "
                    f"(entrada maliciosa: {member})"
                )
                raise Exception("Tentativa de Zip Slip detectada")

        zip_ref.extractall(destino)

    logger.info(f"Extração concluída com sucesso: {zip_path}")


# Lista apenas arquivos .zip
logger.info(f"Procurando arquivos ZIP em: {diretorio_zip}")

with os.scandir(diretorio_zip) as entradas:
    arquivos_zip = [
        e.path for e in entradas if e.is_file() and e.name.lower().endswith(".zip")
    ]

if not arquivos_zip:
    logger.warning("Nenhum arquivo ZIP encontrado para extração.")
else:
    logger.info(f"{len(arquivos_zip)} arquivo(s) ZIP encontrado(s).")

# Extrai cada ZIP
for zip_file in arquivos_zip:
    logger.info(f"Iniciando processamento do arquivo ZIP: {zip_file}")
    extrair_zip_seguro(zip_file, diretorio_destino)

logger.info("Processo de extração finalizado com sucesso.")
