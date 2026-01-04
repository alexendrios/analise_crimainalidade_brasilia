import zipfile
import os
from util.log import logs

logger = logs()

diretorio_zip = "./data/zip"
diretorio_destino = "./data/planilha"

os.makedirs(diretorio_destino, exist_ok=True)
logger.info(f"Diretório de destino garantido: {diretorio_destino}")


def extrair_zip_seguro(zip_path: str, destino: str) -> None:
    logger.info(f"Iniciando extração segura do ZIP: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.namelist():
            destino_final = os.path.abspath(os.path.join(destino, member))

            # 🛡️ Proteção contra Zip Slip
            if not destino_final.startswith(os.path.abspath(destino)):
                logger.error(
                    f"Tentativa de Zip Slip detectada no arquivo {zip_path} "
                    f"(entrada maliciosa: {member})"
                )
                raise Exception("Tentativa de Zip Slip detectada")

            # 🧹 Remove arquivo existente antes de extrair
            if os.path.exists(destino_final):
                try:
                    os.remove(destino_final)
                    logger.info(f"Arquivo existente removido: {destino_final}")
                except PermissionError:
                    logger.error(
                        f"Arquivo em uso ou sem permissão: {destino_final}. "
                        "Feche o Excel ou outro processo que esteja usando o arquivo."
                    )
                    raise

        zip_ref.extractall(destino)

    logger.info(f"Extração concluída com sucesso: {zip_path}")


def arquivos_zip_execucao() -> None:
    logger.info(f"Procurando arquivos ZIP em: {diretorio_zip}")

    if not os.path.isdir(diretorio_zip):
        logger.error(f"Diretório ZIP não encontrado: {diretorio_zip}")
        return

    with os.scandir(diretorio_zip) as entradas:
        arquivos_zip = [
            e.path for e in entradas if e.is_file() and e.name.lower().endswith(".zip")
        ]

    if not arquivos_zip:
        logger.warning("Nenhum arquivo ZIP encontrado para extração.")
        return

    logger.info(f"{len(arquivos_zip)} arquivo(s) ZIP encontrado(s).")

    for zip_file in arquivos_zip:
        logger.info(f"Iniciando processamento do arquivo ZIP: {zip_file}")
        extrair_zip_seguro(zip_file, diretorio_destino)

    logger.info("Processo de extração finalizado com sucesso.")
