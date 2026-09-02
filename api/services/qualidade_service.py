# api/services/qualidade_service.py
"""
Camada de serviço da API para o Data Quality Score.

Carrega as tabelas gold conhecidas do catálogo (`api.config.TABELAS_GOLD`),
calcula a nota 0-100 por tabela e consolidada via
`validation.qualidade_dados`, com cache em memória TTL (mesmo padrão de
`analise_service`).
"""

import threading
import time

from api.config import TABELAS_GOLD
from api.services.analise_service import DadosIndisponiveisError
from util.log import logs
from validation.esquemas import GOLD as ESQUEMAS_GOLD
from validation.qualidade_dados import avaliar_qualidade_dados

logger = logs()

TTL_CACHE_SEGUNDOS = 300
_cache_resultado: tuple[float, dict] | None = None
_cache_lock = threading.Lock()


def limpar_cache() -> None:
    """Descarta o resultado em cache (útil para testes e recálculos)."""
    global _cache_resultado
    with _cache_lock:
        _cache_resultado = None


def _carregar_tabelas() -> dict:
    """Carrega as tabelas gold do catálogo via Repository (import tardio p/ testes)."""
    from ingestion.repository_adapter import Repository

    tabelas: dict = {}
    for nome in TABELAS_GOLD:
        try:
            df = Repository.load(nome)
        except Exception as exc:  # noqa: BLE001 - robustez de carregamento
            logger.warning("Falha ao carregar a tabela '%s': %s", nome, exc)
            df = None
        if df is not None and not df.empty:
            tabelas[nome] = df
        elif df is None:
            logger.info("Tabela '%s' não materializada (ou vazia)", nome)
    return tabelas


def obter_qualidade_dados() -> dict:
    """Data Quality Score consolidado do catálogo gold (com cache TTL)."""
    global _cache_resultado
    agora = time.monotonic()
    with _cache_lock:
        if _cache_resultado is not None and agora - _cache_resultado[0] < TTL_CACHE_SEGUNDOS:
            logger.info("Data Quality Score servido do cache")
            return _cache_resultado[1]

    try:
        tabelas = _carregar_tabelas()
    except Exception as exc:
        logger.exception("Falha ao carregar tabelas gold para /qualidade")
        raise DadosIndisponiveisError(
            f"Nao foi possivel carregar as tabelas gold: {exc}"
        ) from exc

    resultado = avaliar_qualidade_dados(tabelas, TABELAS_GOLD, ESQUEMAS_GOLD)

    with _cache_lock:
        _cache_resultado = (agora, resultado)
    return resultado