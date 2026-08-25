# dashboard/api_client.py
"""
Cliente HTTP da API de Criminalidade Brasília/DF.

Camada testável que conversa com os endpoints da API FastAPI
(`api/`) usando apenas `requests`, sem depender do Streamlit. A base
URL vem da variável de ambiente `API_BASE_URL` (padrão:
`http://localhost:8000`).
"""

import os
from typing import Any, Dict, List, Optional

import requests

DEFAULT_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")
TIMEOUT_SEGUNDOS = 30


class ApiError(RuntimeError):
    """Levantada quando a API responde com erro, está fora do ar ou
    retorna um corpo inesperado."""


def _montar_url(base_url: str, caminho: str) -> str:
    return f"{base_url.rstrip('/')}/{caminho.lstrip('/')}"


def _get(base_url: str, caminho: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Executa um GET e normaliza erros de rede/HTTP em `ApiError`."""
    url = _montar_url(base_url, caminho)
    try:
        resposta = requests.get(url, params=params, timeout=TIMEOUT_SEGUNDOS)
    except requests.RequestException as exc:
        raise ApiError(f"Falha de conexão com a API ({url}): {exc}") from exc

    if resposta.status_code != 200:
        detalhe = ""
        try:
            corpo = resposta.json()
            if isinstance(corpo, dict):
                detalhe = corpo.get("detail", "")
        except ValueError:
            detalhe = resposta.text[:200]
        raise ApiError(
            f"API respondeu HTTP {resposta.status_code} em {url}"
            + (f": {detalhe}" if detalhe else "")
        )

    try:
        return resposta.json()
    except ValueError as exc:
        raise ApiError(f"Resposta da API não é JSON válido ({url}): {exc}") from exc


def health(base_url: str = DEFAULT_BASE_URL) -> Dict[str, Any]:
    """Retorna o status de saúde da API."""
    return _get(base_url, "/health")


def listar_tabelas(base_url: str = DEFAULT_BASE_URL) -> List[Dict[str, Any]]:
    """Lista as tabelas gold disponíveis (catálogo da API)."""
    payload = _get(base_url, "/gold/tabelas")
    return list(payload.get("tabelas") or [])


def obter_resumo(tabela: str, base_url: str = DEFAULT_BASE_URL) -> Dict[str, Any]:
    """Retorna estatísticas descritivas de uma tabela gold."""
    return _get(base_url, f"/gold/{tabela}/resumo")


def obter_dados(
    tabela: str,
    pagina: int = 1,
    tamanho_pagina: int = 1000,
    ano_min: Optional[int] = None,
    ano_max: Optional[int] = None,
    regiao_administrativa: Optional[str] = None,
    base_url: str = DEFAULT_BASE_URL,
) -> Dict[str, Any]:
    """Consulta registros paginados de uma tabela gold, com filtros opcionais."""
    params: Dict[str, Any] = {"pagina": pagina, "tamanho_pagina": tamanho_pagina}
    if ano_min is not None:
        params["ano_min"] = ano_min
    if ano_max is not None:
        params["ano_max"] = ano_max
    if regiao_administrativa:
        params["regiao_administrativa"] = regiao_administrativa
    return _get(base_url, f"/gold/{tabela}/dados", params=params)


def obter_previsao(
    horizonte_anos: int = 5,
    usar_cache: bool = True,
    base_url: str = DEFAULT_BASE_URL,
) -> Dict[str, Any]:
    """Gera/retorna a previsão de crimes contra a mulher."""
    params: Dict[str, Any] = {
        "horizonte_anos": horizonte_anos,
        "usar_cache": str(usar_cache).lower(),
    }
    return _get(base_url, "/previsao/crimes-contra-mulher", params=params)


def listar_modelos(base_url: str = DEFAULT_BASE_URL) -> List[Dict[str, Any]]:
    """Lista os modelos já treinados e persistidos em models/."""
    payload = _get(base_url, "/previsao/modelos")
    return list(payload.get("modelos") or [])


def obter_classificacao(
    usar_cache: bool = True,
    base_url: str = DEFAULT_BASE_URL,
) -> Dict[str, Any]:
    """Retorna a classificação de criminalidade letal por RA
    (Regressão Logística, endpoint /classificacao/criminalidade-letal)."""
    params: Dict[str, Any] = {"usar_cache": str(usar_cache).lower()}
    return _get(base_url, "/classificacao/criminalidade-letal", params=params)


def obter_correlacoes(
    metodo: str = "pearson",
    top_n: int = 5,
    base_url: str = DEFAULT_BASE_URL,
) -> Dict[str, Any]:
    """Retorna a matriz de correlação multivariada entre indicadores gold
    (endpoint /analise/correlacoes)."""
    params: Dict[str, Any] = {"metodo": metodo, "top_n": top_n}
    return _get(base_url, "/analise/correlacoes", params=params)


def obter_granger(
    max_lag: int = 1,
    apenas_significantes: bool = True,
    limite: int = 50,
    base_url: str = DEFAULT_BASE_URL,
) -> Dict[str, Any]:
    """Retorna a causalidade de Granger pairwise entre indicadores anuais
    (endpoint /analise/granger)."""
    params: Dict[str, Any] = {
        "max_lag": max_lag,
        "apenas_significantes": str(apenas_significantes).lower(),
        "limite": limite,
    }
    return _get(base_url, "/analise/granger", params=params)


def obter_anomalias(
    limite: int = 50,
    base_url: str = DEFAULT_BASE_URL,
) -> Dict[str, Any]:
    """Retorna as anomalias do Isolation Forest no painel RA x ano e na
    série mensal de violência contra idosos (endpoint /analise/anomalias)."""
    params: Dict[str, Any] = {"limite": limite}
    return _get(base_url, "/analise/anomalias", params=params)


def obter_zonas_quentes(
    tamanho_celula_km: float = 1.5,
    top_n: int = 20,
    base_url: str = DEFAULT_BASE_URL,
) -> Dict[str, Any]:
    """Retorna as células da malha com mais ocorrências patrimoniais no
    último ano (endpoint /analise/zonas-quentes)."""
    params: Dict[str, Any] = {
        "tamanho_celula_km": tamanho_celula_km,
        "top_n": top_n,
    }
    return _get(base_url, "/analise/zonas-quentes", params=params)
