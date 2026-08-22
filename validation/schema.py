# validation/schema.py
"""
Data quality automatizado: schema checks entre camadas (bronze → silver → gold).

Cada tabela tem um `EsquemaTabela` declarativo (colunas obrigatórias e seus
tipos, colunas opcionais e chaves de unicidade). O executor de pipelines roda
`validar_schema` automaticamente após cada step que declara o hook
`validacao`; falhas de schema contam como falha do step (com retry).
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional, Tuple

import pandas as pd

from util.log import logs
from validation.validator import validar_chaves

logger = logs()


class ErroSchema(ValueError):
    """Levantada quando os dados violam o esquema declarado."""


TEXTO = "texto"
NUMERICO = "numerico"
DATA = "data"

TIPOS_VALIDOS = frozenset({TEXTO, NUMERICO, DATA})


@dataclass(frozen=True)
class EsquemaTabela:
    """Contrato de schema de uma camada do pipeline."""

    nome: str
    colunas: Dict[str, str] = field(default_factory=dict)
    colunas_opcionais: Tuple[str, ...] = ()
    chaves: Tuple[str, ...] = ()

    def __post_init__(self):
        invalidos = set(self.colunas.values()) - TIPOS_VALIDOS
        if invalidos:
            raise ValueError(
                f"Esquema '{self.nome}' declara tipos inválidos: {sorted(invalidos)}. "
                f"Tipos válidos: {sorted(TIPOS_VALIDOS)}"
            )


def _tipo_ok(serie: pd.Series, tipo: str) -> bool:
    if tipo == TEXTO:
        return (
            pd.api.types.is_object_dtype(serie)
            or pd.api.types.is_string_dtype(serie)
            or pd.api.types.is_bool_dtype(serie)
        )
    if tipo == NUMERICO:
        return pd.api.types.is_numeric_dtype(serie)
    if tipo == DATA:
        return pd.api.types.is_datetime64_any_dtype(serie)
    return False


def validar_schema(df: Any, esquema: EsquemaTabela) -> None:
    """
    Valida um DataFrame contra o esquema declarado.

    Falha (ErroSchema) quando há coluna obrigatória ausente, tipo incompatível
    ou duplicidade nas chaves. Nulos em colunas obrigatórias são sinalizados
    no log (visíveis), mas não derrubam a execução.
    """
    if not isinstance(df, pd.DataFrame):
        raise ErroSchema(f"[{esquema.nome}] esperado DataFrame, recebido {type(df).__name__}")

    logger.info("Validando schema", extra={"esquema": esquema.nome, "shape": df.shape})

    faltando = [col for col in esquema.colunas if col not in df.columns]
    if faltando:
        logger.error(
            "Schema check falhou: colunas obrigatórias ausentes",
            extra={"esquema": esquema.nome, "faltando": faltando},
        )
        raise ErroSchema(
            f"[{esquema.nome}] colunas obrigatórias ausentes: {faltando}"
        )

    for coluna, tipo in esquema.colunas.items():
        if not _tipo_ok(df[coluna], tipo):
            logger.error(
                "Schema check falhou: tipo incompatível",
                extra={
                    "esquema": esquema.nome,
                    "coluna": coluna,
                    "esperado": tipo,
                    "recebido": str(df[coluna].dtype),
                },
            )
            raise ErroSchema(
                f"[{esquema.nome}] coluna '{coluna}' deveria ser '{tipo}', "
                f"recebido dtype '{df[coluna].dtype}'"
            )

    nulos = {
        coluna: int(df[coluna].isna().sum())
        for coluna in esquema.colunas
        if df[coluna].isna().any()
    }
    if nulos:
        logger.warning(
            "Colunas obrigatórias possuem valores nulos",
            extra={"esquema": esquema.nome, "nulos_por_coluna": nulos},
        )

    if esquema.chaves:
        validar_chaves(df, list(esquema.chaves))

    logger.info("Schema check concluído com sucesso", extra={"esquema": esquema.nome})


def validador_de_esquema(esquema: EsquemaTabela):
    """Hook pronto para `PipelineStep.validacao`: valida o retorno do step."""

    def _validar(resultado) -> None:
        if resultado is None:
            logger.warning(
                "Step sem retorno; schema check ignorado",
                extra={"esquema": esquema.nome},
            )
            return
        validar_schema(resultado, esquema)

    return _validar


def validador_multi(*esquemas: EsquemaTabela, extrator=None):
    """
    Hook para steps que retornam múltiplos DataFrames (ex.: tupla).
    Por padrão aplica cada esquema ao elemento de mesma posição.
    """

    def _validar(resultado) -> None:
        if resultado is None:
            logger.warning("Step sem retorno; schema checks ignorados")
            return
        itens = extrator(resultado) if extrator else resultado
        for esquema, item in zip(esquemas, itens):
            validar_schema(item, esquema)

    return _validar


def resumo_esquemas(esquemas: Iterable[EsquemaTabela]) -> pd.DataFrame:
    """DataFrame resumo dos schemas registrados (documentação viva)."""
    return pd.DataFrame(
        [
            {
                "nome": e.nome,
                "colunas_obrigatorias": len(e.colunas),
                "colunas_opcionais": len(e.colunas_opcionais),
                "chaves": ", ".join(e.chaves),
            }
            for e in esquemas
        ]
    )
