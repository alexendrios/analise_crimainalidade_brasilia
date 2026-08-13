# api/services/gold_service.py
"""
Camada de serviço da API para as tabelas Gold.

Reaproveita deliberadamente o `Repository` (ingestion/repository_adapter.py)
e as funções de `database/repository/repository.py` já existentes e
testadas no projeto, em vez de reimplementar acesso a banco aqui.
"""

from math import ceil
from typing import Any, Dict, Optional

import pandas as pd

from api.config import COLUNA_ANO_POR_TABELA, TABELAS_GOLD
from database.repository.repository import analisar_tabela, listar_tabelas
from ingestion.repository_adapter import Repository
from util.log import logs

logger = logs()


class TabelaInvalidaError(ValueError):
    """Levantada quando o nome de tabela solicitado não é uma tabela gold conhecida/segura."""


class TabelaNaoEncontradaError(LookupError):
    """Levantada quando a tabela é válida mas não existe/não pôde ser carregada no banco."""


def _validar_tabela_gold(nome_tabela: str) -> None:
    if nome_tabela not in TABELAS_GOLD:
        logger.warning("🚫 Tentativa de acesso a tabela não catalogada: %s", nome_tabela)
        raise TabelaInvalidaError(
            f"'{nome_tabela}' não é uma tabela gold conhecida. "
            f"Consulte GET /gold/tabelas para a lista válida."
        )


def listar_tabelas_gold() -> Dict[str, Any]:
    """
    Retorna o catálogo de tabelas gold conhecidas, sinalizando quais já
    existem materializadas no Postgres (best-effort: se o banco estiver
    fora do ar, retorna disponivel_no_banco=False para todas em vez de
    quebrar a resposta).
    """
    try:
        tabelas_no_banco = set(listar_tabelas())
    except Exception:
        logger.exception("⚠️ Não foi possível listar tabelas do banco; seguindo sem essa info")
        tabelas_no_banco = set()

    tabelas = [
        {
            "nome": nome,
            "descricao": descricao,
            "disponivel_no_banco": nome in tabelas_no_banco,
        }
        for nome, descricao in TABELAS_GOLD.items()
    ]

    return {"total": len(tabelas), "tabelas": tabelas}


def obter_resumo_tabela(nome_tabela: str) -> Dict[str, Any]:
    """Estatísticas descritivas de uma tabela gold (linhas, colunas, nulos)."""
    _validar_tabela_gold(nome_tabela)

    try:
        return analisar_tabela(nome_tabela)
    except Exception as exc:
        logger.exception("🔥 Erro ao gerar resumo da tabela '%s'", nome_tabela)
        raise TabelaNaoEncontradaError(
            f"Não foi possível carregar/analisar a tabela '{nome_tabela}': {exc}"
        ) from exc


def obter_dados_tabela(
    nome_tabela: str,
    pagina: int = 1,
    tamanho_pagina: int = 50,
    ano_min: Optional[int] = None,
    ano_max: Optional[int] = None,
    regiao_administrativa: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Retorna registros paginados de uma tabela gold, com filtros opcionais
    de intervalo de anos e Região Administrativa.
    """
    _validar_tabela_gold(nome_tabela)

    df = Repository.load(nome_tabela)

    if df is None:
        raise TabelaNaoEncontradaError(
            f"A tabela '{nome_tabela}' ainda não foi materializada no banco "
            f"(execute o pipeline gold antes de consultar)."
        )

    df = _aplicar_filtros(df, nome_tabela, ano_min, ano_max, regiao_administrativa)

    total_linhas = len(df)
    total_paginas = max(1, ceil(total_linhas / tamanho_pagina))
    pagina = max(1, min(pagina, total_paginas))

    inicio = (pagina - 1) * tamanho_pagina
    fim = inicio + tamanho_pagina

    registros = df.iloc[inicio:fim].to_dict(orient="records")

    return {
        "tabela": nome_tabela,
        "total_linhas": total_linhas,
        "pagina": pagina,
        "tamanho_pagina": tamanho_pagina,
        "total_paginas": total_paginas,
        "registros": registros,
    }


def _aplicar_filtros(
    df: pd.DataFrame,
    nome_tabela: str,
    ano_min: Optional[int],
    ano_max: Optional[int],
    regiao_administrativa: Optional[str],
) -> pd.DataFrame:
    coluna_ano = COLUNA_ANO_POR_TABELA.get(nome_tabela)

    if coluna_ano and coluna_ano in df.columns:
        if ano_min is not None:
            df = df[df[coluna_ano] >= ano_min]
        if ano_max is not None:
            df = df[df[coluna_ano] <= ano_max]

    if regiao_administrativa and "regiao_administrativa" in df.columns:
        alvo = regiao_administrativa.strip().upper()
        df = df[df["regiao_administrativa"].astype(str).str.upper() == alvo]

    return df
