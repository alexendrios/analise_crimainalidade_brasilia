# dashboard/ia_client.py
"""
Cliente HTTP do Ollama para o resumo geral gerado por IA.

Camada testável que conversa com a API local do Ollama usando apenas
`requests`, sem depender do Streamlit. A base URL vem da variável de
ambiente `OLLAMA_BASE_URL` (padrão: `http://localhost:11434`).
"""

import os
from typing import List

import requests

DEFAULT_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
MODELO_PADRAO = "llama3.2"
TIMEOUT_LISTAR_SEGUNDOS = 5
TIMEOUT_GERAR_SEGUNDOS = 300


class OllamaError(RuntimeError):
    """Levantada quando o Ollama responde com erro, está fora do ar ou
    retorna um corpo inesperado."""


def _montar_url(base_url: str, caminho: str) -> str:
    return f"{base_url.rstrip('/')}/{caminho.lstrip('/')}"


def listar_modelos_ollama(base_url: str = DEFAULT_BASE_URL) -> List[str]:
    """
    Nomes dos modelos instalados no Ollama (GET /api/tags).

    Descoberta é best-effort: se o serviço estiver fora do ar ou responder
    de forma inesperada, devolve lista vazia para a UI usar o modelo padrão.
    """
    url = _montar_url(base_url, "/api/tags")
    try:
        resposta = requests.get(url, timeout=TIMEOUT_LISTAR_SEGUNDOS)
    except requests.RequestException:
        return []

    if resposta.status_code != 200:
        return []
    try:
        corpo = resposta.json()
    except ValueError:
        return []

    modelos = corpo.get("models") or []
    return [m["name"] for m in modelos if isinstance(m, dict) and m.get("name")]


def gerar_resumo_ia(
    contexto: str,
    base_url: str = DEFAULT_BASE_URL,
    modelo: str = MODELO_PADRAO,
    temperatura: float = 0.2,
) -> str:
    """
    Gera o resumo executivo via POST /api/generate (sem streaming).

    `contexto` deve conter os dados estruturados coletados da API; o prompt
    instrui o modelo a usar exclusivamente essas informações. Temperatura
    baixa reduz alucinação em síntese factual.
    """
    prompt = (
        "Você é um analista de criminalidade do Distrito Federal. Com base "
        "EXCLUSIVAMENTE nos dados estruturados abaixo, escreva um resumo "
        "executivo em português do Brasil, em tópicos curtos, destacando: "
        "panorama geral, pontos críticos, anomalias, correlações e "
        "causalidades relevantes e recomendações. Não invente números que "
        "não estejam no contexto.\n\n"
        f"DADOS:\n{contexto}"
    )
    url = _montar_url(base_url, "/api/generate")
    payload = {
        "model": modelo,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperatura},
    }

    try:
        resposta = requests.post(url, json=payload, timeout=TIMEOUT_GERAR_SEGUNDOS)
    except requests.RequestException as exc:
        raise OllamaError(f"Falha de conexão com o Ollama ({url}): {exc}") from exc

    if resposta.status_code != 200:
        raise OllamaError(
            f"Ollama respondeu HTTP {resposta.status_code} em {url}: "
            f"{resposta.text[:200]}"
        )

    try:
        corpo = resposta.json()
    except ValueError as exc:
        raise OllamaError(f"Resposta do Ollama não é JSON válido ({url}): {exc}") from exc

    texto = str(corpo.get("response") or "").strip()
    if not texto:
        raise OllamaError("O Ollama retornou uma resposta vazia.")
    return texto
