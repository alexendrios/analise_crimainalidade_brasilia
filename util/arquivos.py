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
    Remove arquivos CSV, XLSX, XLS, PDF, ZIP e LOG antes da execução.
    Não remove diretórios, apenas os arquivos internos.
    """
    global logger  # Permite reatribuir a variável global 'logger' ao final

    # Fechar handlers antes de deletar arquivos de log
    fechar_loggers()

    print("Removendo arquivos dos diretórios...")

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
            except Exception as e:  # pragma: no cover
                print(f"Erro ao remover {arquivo}: {e}")

    # Recriar logger global depois que os logs antigos foram apagados
    logger = logs()

    logger.info("*****************************************************")
    logger.info("Limpeza concluída")
    logger.info("Diretórios limpos e logger reiniciado corretamente.")
    logger.info("*****************************************************\n")

    return logger


def download_arquivo(url: str, nome_arquivo: str, max_tentativas: int = 5):
    start = time.time()
    logger.info(f"Iniciando download: {nome_arquivo}")
    logger.info(f"URL: {url}")

    file_path = None
    tentativas_realizadas = 0

    for tentativa in range(1, max_tentativas + 1):
        tentativas_realizadas = tentativa
        temp_file_path = None

        logger.info(f"--- Tentativa {tentativa} de {max_tentativas} ---")

        try:
            sleep(1)

            # Cria uma nova sessão HTTP a cada tentativa
            with requests.Session() as session:
                # Limpa cookies e força uma nova conexão
                session.cookies.clear()

                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "Connection": "close",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                }

                response = session.get(
                    url,
                    headers=headers,
                    stream=True,
                    timeout=30,
                    allow_redirects=True,
                )

            logger.info(f"Status Code: {response.status_code}")
            logger.info(f"URL final: {response.url}")

            content_type = response.headers.get("Content-Type", "").lower()
            logger.info(f"Content-Type: {content_type}")

            # Lança HTTPError se o status for 4xx ou 5xx
            response.raise_for_status()

            # Resposta em JSON geralmente indica erro retornado pela API
            if "application/json" in content_type:
                logger.error(f"Resposta inesperada em JSON da API: {response.text}")
                raise Exception(
                    "API retornou resposta em JSON (provável erro do servidor)."
                )

            # Filtro de tipos permitidos
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
                raise Exception(f"Tipo de conteúdo não suportado: {content_type}")

            # Detecta extensão e pasta de destino
            ext, folder = detectar_extensao(response, nome_arquivo)
            os.makedirs(folder, exist_ok=True)

            base, _ = os.path.splitext(nome_arquivo)
            base_name = base if base else nome_arquivo

            file_path = os.path.join(folder, f"{base_name}{ext}")
            temp_file_path = f"{file_path}.tmp"

            # Garante download do zero
            if os.path.exists(file_path):
                os.remove(file_path)
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

            total_size = int(response.headers.get("Content-Length", 0))
            total_bytes = 0

            with (
                open(temp_file_path, "wb") as f,
                tqdm(
                    total=total_size if total_size > 0 else None,
                    unit="B",
                    unit_scale=True,
                    desc=f"Baixando {nome_arquivo} (Tentativa {tentativa})",
                    ncols=80,
                ) as progress,
            ):
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        progress.update(len(chunk))
                        total_bytes += len(chunk)

            if total_bytes == 0:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
                logger.warning("Arquivo vazio obtido na tentativa.")
                raise Exception("O servidor retornou um arquivo de 0 bytes.")

            # Substitui o arquivo temporário pelo definitivo
            os.replace(temp_file_path, file_path)

            logger.info(f"Arquivo salvo em: {file_path}")
            logger.info("Download concluído com sucesso!")

            tempo_total = time.time() - start
            logger.info(f"Tempo total: {tempo_total:.2f} segundos")
            logger.info("*****************************************************\n")

            return file_path

        except requests.exceptions.HTTPError as e:
            status_code = (
                e.response.status_code if e.response is not None else "Desconhecido"
            )
            logger.error(f"Erro HTTP {status_code} na tentativa {tentativa}: {e}")

        except Exception as e:
            logger.error(
                f"Erro na tentativa {tentativa} de download de {nome_arquivo}: {e}"
            )

        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)

        if tentativa < max_tentativas:
            tempo_espera = 5 * tentativa
            logger.info(f"Aguardando {tempo_espera}s antes da próxima tentativa...")
            sleep(tempo_espera)

    # Remove arquivo vazio caso exista
    if file_path and os.path.exists(file_path) and os.path.getsize(file_path) == 0:
        os.remove(file_path)

    tempo_total = time.time() - start
    logger.error(
        f"Falha ao realizar o download após {tentativas_realizadas} tentativa(s). "
        f"Tempo total: {tempo_total:.2f} segundos"
    )
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

        elif (
            "zip" in content_type
            or "compressed" in content_type
            or "octet-stream" in content_type
        ):
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