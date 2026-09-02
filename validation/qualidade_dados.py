# validation/qualidade_dados.py
"""
Data Quality Score (0-100) para as tabelas gold.

Calcula, para cada tabela gold, uma nota entre 0 e 100 agregando seis
dimensões ponderadas:

    completude         (25%)  valores não-nulos nas colunas obrigatórias
    unicidade          (20%)  registros sem duplicidade nas chaves
    validade_schema    (20%)  colunas presentes com o tipo declarado
    consistencia       (20%)  RAs canônicas, anos dentro do período e
                              contagens não-negativas
    atualidade         (10%)  frescor do `inserido_em` (janela decrescente)
    cobertura_temporal ( 5%)  anos esperados (2015-2024) presentes

Dimensões não aplicáveis a uma tabela (ex.: tabela sem chave, sem coluna de
ano) são excluídas e seus pesos redistribuídos. A nota geral do Data Quality
Score é a média das notas das tabelas do catálogo — tabelas não materializadas
entram com nota 0.
"""

from __future__ import annotations

import unicodedata
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from util.padronizacao import MAPEAMENTO_REGIOES_ADMINISTRATIVAS
from validation.schema import DATA, NUMERICO, TEXTO, EsquemaTabela, _tipo_ok

ANOS_ESPERADOS = tuple(range(2015, 2025))
FRESCURA_MAXIMA_DIAS = 30
VALIDADE_MAXIMA_DIAS = 365

# (chave, rótulo, peso)
DIMENSOES: Tuple[Tuple[str, str, float], ...] = (
    ("completude", "Completude", 0.25),
    ("unicidade", "Unicidade", 0.20),
    ("validade_schema", "Validade de schema", 0.20),
    ("consistencia", "Consistência", 0.20),
    ("atualidade", "Atualidade", 0.10),
    ("cobertura_temporal", "Cobertura temporal", 0.05),
)
PESOS = {chave: peso for chave, _, peso in DIMENSOES}
ROTULOS = {chave: rotulo for chave, rotulo, _ in DIMENSOES}

PREFIXOS_COLUNAS_CONTAGEM = (
    "ocorrencia",
    "ocorrencias",
    "casos",
    "quantidade",
    "total",
    "masculino",
    "feminino",
    "fato",
    "fatos",
    "registro",
    "registros",
    "crimes",
    "crimes_contra_mulher",
)
FRAGMENTOS_NAO_CONTAGEM = ("variacao", "variação", "percentual", "residual", "previsao", "score")


def _sem_acentos(texto: str) -> str:
    """Normaliza para comparação sem acentos/caixa (ex.: 'Região' -> 'regiao')."""
    texto = unicodedata.normalize("NFKD", texto)
    return texto.encode("ascii", "ignore").decode("ascii").lower()


def _e_coluna_ra(nome: str) -> bool:
    """Heurística: a coluna representa uma Região Administrativa."""
    limpo = _sem_acentos(str(nome)).strip()
    return limpo == "ra" or "regiao" in limpo or "região" in limpo


def _e_coluna_contagem(nome: str) -> bool:
    """Heurística: coluna numérica que representa contagem (fração não-negativa)."""
    limpo = _sem_acentos(str(nome)).strip()
    if any(frag in limpo for frag in FRAGMENTOS_NAO_CONTAGEM):
        return False
    return limpo.startswith(PREFIXOS_COLUNAS_CONTAGEM)


def _pct(numerador: int, denominador: int) -> float:
    return 100.0 if denominador == 0 else round(100.0 * numerador / denominador, 2)


def _escore_completude(df: pd.DataFrame, esquema: EsquemaTabela) -> tuple:
    obrigatorias = list(esquema.colunas) or list(df.columns)
    pontuacoes: List[float] = []
    problemas: List[str] = []
    avisos: List[str] = []
    for coluna in obrigatorias:
        if coluna not in df.columns:
            pontuacoes.append(0.0)
            problemas.append(f"coluna obrigatória ausente: {coluna}")
            continue
        total = len(df)
        nao_nulos = int(df[coluna].notna().sum())
        pontuacoes.append(_pct(nao_nulos, total))
        if total - nao_nulos > 0:
            avisos.append(f"{total - nao_nulos} nulo(s) em '{coluna}'")
    return round(float(sum(pontuacoes)) / len(pontuacoes), 2), problemas, avisos


def _escore_unicidade(df: pd.DataFrame, esquema: EsquemaTabela) -> tuple:
    chaves = list(esquema.chaves)
    if not chaves:
        return None, [], []
    presentes = [c for c in chaves if c in df.columns]
    if not presentes:
        return None, ["chaves não encontradas no DataFrame"], []
    duplicados = int(df[df.duplicated(presentes, keep=False)].shape[0])
    problemas = [
        f"{duplicados} registro(s) duplicado(s) nas chaves {presentes}"
    ] if duplicados else []
    return _pct(len(df) - duplicados, len(df)), problemas, []


def _escore_validade_schema(df: pd.DataFrame, esquema: EsquemaTabela) -> tuple:
    colunas = list(esquema.colunas)
    if not colunas:
        return None, [], []
    pontuacoes: List[float] = []
    problemas: List[str] = []
    for coluna, tipo in esquema.colunas.items():
        if coluna not in df.columns:
            pontuacoes.append(0.0)
            problemas.append(f"coluna obrigatória ausente: {coluna}")
        elif not _tipo_ok(df[coluna], tipo):
            pontuacoes.append(0.0)
            problemas.append(
                f"tipo incompatível em '{coluna}': esperado '{tipo}', "
                f"recebido dtype '{df[coluna].dtype}'"
            )
        else:
            pontuacoes.append(100.0)
    return round(float(sum(pontuacoes)) / len(pontuacoes), 2), problemas, []


def _escore_consistencia(
    df: pd.DataFrame, esquema: EsquemaTabela, ras_referencia: set[str]
) -> tuple:
    subs: List[float] = []
    problemas: List[str] = []

    colunas_ra = [c for c in df.columns if _e_coluna_ra(c)]
    if colunas_ra:
        serie = pd.concat([df[c].dropna().astype(str) for c in colunas_ra])
        serie = serie.str.strip().str.upper()
        em_dominio = int(serie.isin(ras_referencia).sum())
        subs.append(_pct(em_dominio, len(serie)))
        fora = len(serie) - em_dominio
        if fora:
            problemas.append(f"{fora} valor(es) de RA fora do domínio canônico")

    if "ano" in df.columns:
        anos = df["ano"].dropna()
        dentro = int(anos.isin(ANOS_ESPERADOS).sum())
        subs.append(_pct(dentro, len(anos)))
        fora = len(anos) - dentro
        if fora:
            problemas.append(f"{fora} registro(s) com ano fora de {ANOS_ESPERADOS[0]}-{ANOS_ESPERADOS[-1]}")

    colunas_contagem = [
        c
        for c in df.columns
        if _e_coluna_contagem(c) and pd.api.types.is_numeric_dtype(df[c])
    ]
    if colunas_contagem:
        total = 0
        negativos = 0
        for coluna in colunas_contagem:
            valores = df[coluna].dropna()
            total += len(valores)
            negativos += int((valores < 0).sum())
        subs.append(_pct(total - negativos, total))
        if negativos:
            problemas.append(f"{negativos} valor(es) de contagem negativos")

    if not subs:
        return None, [], []
    return round(float(sum(subs)) / len(subs), 2), problemas, []


def _escore_atualidade(df: pd.DataFrame, _esquema: EsquemaTabela | None = None) -> tuple:
    if "inserido_em" not in df.columns or df.empty:
        return None, [], []
    datas = pd.to_datetime(df["inserido_em"], utc=True, errors="coerce").dropna()
    if datas.empty:
        return None, [], []
    ultima = datas.max()
    dias = (datetime.now(timezone.utc) - ultima.to_pydatetime()).days
    if dias <= FRESCURA_MAXIMA_DIAS:
        return 100.0, [], [f"última atualização há {dias} dia(s)"]
    if dias >= VALIDADE_MAXIMA_DIAS:
        return 0.0, [f"dados defasados: última atualização há {dias} dia(s)"], []
    escore = round(
        100.0 * (VALIDADE_MAXIMA_DIAS - dias) / (VALIDADE_MAXIMA_DIAS - FRESCURA_MAXIMA_DIAS), 2
    )
    return escore, [f"dados em envelhecimento: última atualização há {dias} dia(s)"], []


def _escore_cobertura_temporal(df: pd.DataFrame, _esquema: EsquemaTabela | None = None) -> tuple:
    if "ano" not in df.columns or df.empty:
        return None, [], []
    anos = df["ano"].dropna()
    presentes = int(anos[anos.isin(ANOS_ESPERADOS)].nunique())
    avisos = (
        [f"cobre {presentes} de {len(ANOS_ESPERADOS)} anos esperados ({ANOS_ESPERADOS[0]}-{ANOS_ESPERADOS[-1]})"]
        if presentes < len(ANOS_ESPERADOS)
        else []
    )
    return _pct(presentes, len(ANOS_ESPERADOS)), [], avisos


_AVALIADORES = {
    "completude": _escore_completude,
    "unicidade": _escore_unicidade,
    "validade_schema": _escore_validade_schema,
    "consistencia": _escore_consistencia,
    "atualidade": _escore_atualidade,
    "cobertura_temporal": _escore_cobertura_temporal,
}


def _ultima_atualizacao(df: pd.DataFrame) -> Optional[str]:
    if "inserido_em" not in df.columns or df.empty:
        return None
    datas = pd.to_datetime(df["inserido_em"], utc=True, errors="coerce").dropna()
    if datas.empty:
        return None
    return datas.max().isoformat()


def avaliar_tabela(
    nome: str,
    df: pd.DataFrame | None,
    esquema: EsquemaTabela,
    ras_referencia: set[str],
) -> dict:
    """Nota 0-100 de uma tabela gold (dimensões ponderadas + problemas)."""
    if df is None or df.empty:
        return {
            "tabela": nome,
            "materializada": False,
            "linhas": 0,
            "colunas": 0,
            "escore": 0.0,
            "dimensoes": [],
            "problemas": ["tabela não materializada (ou vazia)"],
            "avisos": [],
            "ultima_atualizacao": None,
        }

    dimensoes: List[dict] = []
    problemas: List[str] = []
    avisos: List[str] = []
    aplicaveis: List[Tuple[float, float]] = []

    for chave, _rotulo, peso in DIMENSOES:
        avaliador = _AVALIADORES[chave]
        if chave == "consistencia":
            escore, probs, avss = avaliador(df, esquema, ras_referencia)
        else:
            escore, probs, avss = avaliador(df, esquema)
        dimensoes.append(
            {
                "chave": chave,
                "rotulo": ROTULOS[chave],
                "escore": escore,
                "aplicavel": escore is not None,
                "peso": peso,
            }
        )
        if escore is not None:
            aplicaveis.append((peso, escore))
        problemas.extend(probs)
        avisos.extend(avss)

    peso_total = sum(peso for peso, _ in aplicaveis)
    escore_final = (
        round(sum(peso * escore for peso, escore in aplicaveis) / peso_total, 2)
        if aplicaveis and peso_total > 0
        else 0.0
    )

    return {
        "tabela": nome,
        "materializada": True,
        "linhas": int(df.shape[0]),
        "colunas": int(df.shape[1]),
        "escore": escore_final,
        "dimensoes": dimensoes,
        "problemas": problemas,
        "avisos": avisos,
        "ultima_atualizacao": _ultima_atualizacao(df),
    }


def _referencia_ras(tabelas: Dict[str, pd.DataFrame], catalogo: Iterable[str]) -> set[str]:
    """Domínio canônico de RAs: união das RAs das tabelas materializadas + aliases."""
    ras: set[str] = set()
    for nome in catalogo:
        df = tabelas.get(nome)
        if df is None:
            continue
        for coluna in df.columns:
            if _e_coluna_ra(coluna):
                ras.update(df[coluna].dropna().astype(str).str.strip().str.upper().unique())
    ras.update({valor.upper() for valor in MAPEAMENTO_REGIOES_ADMINISTRATIVAS.values()})
    ras.update({chave.upper() for chave in MAPEAMENTO_REGIOES_ADMINISTRATIVAS.keys()})
    return ras


def avaliar_qualidade_dados(
    tabelas: Dict[str, pd.DataFrame],
    catalogo: Iterable[str],
    esquemas: Dict[str, EsquemaTabela],
) -> dict:
    """
    Data Quality Score consolidado do catálogo gold.

    :param tabelas: {nome_tabela: DataFrame} (tabelas materializadas).
    :param catalogo: nomes de todas as tabelas gold conhecidas.
    :param esquemas: schemas declarados por tabela (`validation.esquemas.GOLD`).
    """
    catalogo = list(catalogo)
    ras_referencia = _referencia_ras(tabelas, catalogo)

    itens = [
        avaliar_tabela(nome, tabelas.get(nome), esquemas.get(nome) or EsquemaTabela(nome=nome), ras_referencia)
        for nome in catalogo
    ]

    materializadas = sum(1 for item in itens if item["materializada"])
    geral = (
        round(float(sum(item["escore"] for item in itens)) / len(itens), 2) if itens else 0.0
    )

    dimensoes_gerais = []
    for chave, rotulo, _peso in DIMENSOES:
        aplicaveis = [
            item["dimensoes"]
            for item in itens
            if item["materializada"]
            for d in [item["dimensoes"]]
        ]
        escores = [
            d["escore"]
            for dims in aplicaveis
            for d in dims
            if d["chave"] == chave and d["aplicavel"] and d["escore"] is not None
        ]
        dimensoes_gerais.append(
            {
                "chave": chave,
                "rotulo": rotulo,
                "escore": round(float(sum(escores)) / len(escores), 2) if escores else None,
                "aplicavel": bool(escores),
                "peso": PESOS[chave],
            }
        )

    return {
        "gerado_em": datetime.now(timezone.utc),
        "escore_geral": geral,
        "total_tabelas": len(itens),
        "materializadas": materializadas,
        "dimensoes": dimensoes_gerais,
        "tabelas": itens,
    }