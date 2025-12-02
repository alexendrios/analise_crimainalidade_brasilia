import logging
import os
from util.config_loader import get_config


def logs():
    """
    Cria e retorna um logger configurado com base no config.yaml + .env.
    O arquivo info.log será criado automaticamente.
    """

    cfg = get_config()

    LOG_DIR = cfg["paths"]["logs_dir"]
    os.makedirs(LOG_DIR, exist_ok=True)

    log_file = os.path.join(LOG_DIR, cfg["logs"]["filename"])
    log_level_str = cfg["logs"]["level"].upper()

    # Converte string do YAML para nível real do logging
    log_level = getattr(logging, log_level_str, logging.INFO)

    logger = logging.getLogger("coleta_gdf")

    # Evita criação duplicada de handlers
    if logger.handlers:
        return logger

    logger.setLevel(log_level)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    # Arquivo
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Console
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


def configurar_logger():
    """Mantém compatibilidade com os testes."""
    return logs()


def fechar_loggers():
    """
    Fecha todos os handlers de todos os loggers.
    Necessário no Windows para liberar os arquivos .log para exclusão.
    """
    all_loggers = [logging.getLogger()] + [
        logger for logger in logging.Logger.manager.loggerDict.values()
        if isinstance(logger, logging.Logger)
    ]

    for logger in all_loggers:
        for handler in list(logger.handlers):
            try:
                handler.flush()
                handler.close()
            except Exception:
                pass
            logger.removeHandler(handler)
