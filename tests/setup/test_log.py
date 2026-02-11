import os
import logging
import pytest
from unittest.mock import MagicMock, patch

from util.log import logs, configurar_logger, fechar_loggers


# ---------------------------------------------------------------------------
# FIXTURE PARA SETUP LIMPO ENTRE TESTES
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def limpar_loggers():
    """
    Garante que cada teste comece com os loggers limpos.
    """
    fechar_loggers()
    yield
    fechar_loggers()


# ---------------------------------------------------------------------------
# TESTES DO logs()
# ---------------------------------------------------------------------------

def test_logs_cria_diretorio_e_arquivo_de_log(tmp_path):
    with patch("util.log.get_config") as mock_cfg:
        mock_cfg.return_value = {
            "paths": {"logs_dir": str(tmp_path / "logs")},
            "logs": {"filename": "info.log", "level": "INFO"},
        }

        logger = logs()

        # Escreve algo para forçar o FileHandler a criar o arquivo
        logger.info("teste de escrita")

        assert os.path.exists(tmp_path / "logs")

        # Arquivo só existe após primeira escrita
        log_file = tmp_path / "logs" / "info.log"
        assert log_file.exists()


def test_logs_nao_duplica_handlers(tmp_path):
    cfg = {
        "paths": {"logs_dir": str(tmp_path / "logs")},
        "logs": {"filename": "info.log", "level": "INFO"},
    }

    with patch("util.log.get_config", return_value=cfg):
        logger1 = logs()
        handlers_before = len(logger1.handlers)

        logger2 = logs()

        assert handlers_before == len(logger2.handlers)
        assert logger1 is logger2


def test_logs_respeita_nivel_log(tmp_path):
    cfg = {
        "paths": {"logs_dir": str(tmp_path / "logs")},
        "logs": {"filename": "info.log", "level": "DEBUG"},
    }

    with patch("util.log.get_config", return_value=cfg):
        logger = logs()

        # DEBUG = 10
        assert logger.level == logging.DEBUG


def test_logs_cria_handlers_com_formatter(tmp_path):
    cfg = {
        "paths": {"logs_dir": str(tmp_path / "logs")},
        "logs": {"filename": "info.log", "level": "INFO"},
    }

    with patch("util.log.get_config", return_value=cfg):
        logger = logs()

        for handler in logger.handlers:
            assert isinstance(handler.formatter, logging.Formatter)


# ---------------------------------------------------------------------------
# TESTE configurar_logger()
# ---------------------------------------------------------------------------

def test_configurar_logger_chama_logs():
    with patch("util.log.logs") as mock_logs:
        mock_logs.return_value = "fake_logger"
        result = configurar_logger()
        mock_logs.assert_called_once()
        assert result == "fake_logger"


# ---------------------------------------------------------------------------
# TESTES fechar_loggers()
# ---------------------------------------------------------------------------

def test_fechar_loggers_fecha_handlers(tmp_path):
    cfg = {
        "paths": {"logs_dir": str(tmp_path / "logs")},
        "logs": {"filename": "info.log", "level": "INFO"},
    }

    with patch("util.log.get_config", return_value=cfg):
        logger = logs()
        assert logger.handlers  # Tem handlers

        fechar_loggers()

        assert len(logger.handlers) == 0


def test_fechar_loggers_nao_quebra_se_handler_falha(tmp_path):
    cfg = {
        "paths": {"logs_dir": str(tmp_path / "logs")},
        "logs": {"filename": "info.log", "level": "INFO"},
    }

    with patch("util.log.get_config", return_value=cfg):
        logger = logs()

        for h in logger.handlers:
            h.close = MagicMock(side_effect=Exception("erro"))

        fechar_loggers()

        assert len(logger.handlers) == 0
