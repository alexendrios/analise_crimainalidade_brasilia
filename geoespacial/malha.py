# geoespacial/malha.py
"""
Malha de células (grid) sobre o DF para análises espaciais mais finas.

Implementação em pandas/numpy puro (sem PostGIS/GeoPandas): gera células
regulares a partir do bounding box do DF, atribui pontos (lat/lon) a células
e agrega indicadores por RA usando centróides aproximados.

A precisão é adequada para análise exploratória; para produção geoespacial
use o módulo `geoespacial.postgis` com um servidor PostGIS.
"""

import numpy as np
import pandas as pd

from geoespacial.centroides import CENTROIDES_RA
from util.padronizacao import remover_acentos
from util.log import logs

logger = logs()

# Bounding box aproximado do DF (lon_min, lat_min, lon_max, lat_max)
BBOX_DF = (-48.29, -16.10, -47.33, -15.50)

KM_POR_GRAU_LAT = 111.32


def _delta_graus(tamanho_celula_km: float, latitude_referencia: float) -> tuple:
    """Converte o tamanho da célula (km) em deltas de grau lon/lat."""
    if tamanho_celula_km <= 0:
        raise ValueError("tamanho_celula_km deve ser positivo")
    delta_lat = tamanho_celula_km / KM_POR_GRAU_LAT
    delta_lon = tamanho_celula_km / (
        KM_POR_GRAU_LAT * np.cos(np.radians(latitude_referencia))
    )
    return delta_lon, delta_lat


def _eixos(minimo: float, maximo: float, delta: float) -> np.ndarray:
    """
    Bordas do eixo cobrindo integralmente [minimo, maximo].

    A última célula é encolhida quando o intervalo não é múltiplo de delta,
    garantindo que as bordas final/initial coincidam exatamente com o limite.
    """
    quantidade = max(1, int(np.ceil((maximo - minimo) / delta - 1e-9)))
    internas = minimo + np.arange(1, quantidade) * delta
    internas = internas[internas < maximo - 1e-9]
    return np.concatenate(([minimo], internas, [maximo]))


def gerar_malha(tamanho_celula_km: float = 1.0, bbox: tuple = BBOX_DF) -> pd.DataFrame:
    """
    Gera a malha regular de células cobrindo `bbox`.

    Retorna DataFrame com celula_id e os limites/centro de cada célula.
    Células da borda são encolhidas para não ultrapassar o bbox.
    """
    lon_min, lat_min, lon_max, lat_max = bbox
    if not (lon_min < lon_max and lat_min < lat_max):
        raise ValueError("bbox inválido: esperado (lon_min, lat_min, lon_max, lat_max)")

    centro_lat = (lat_min + lat_max) / 2
    delta_lon, delta_lat = _delta_graus(tamanho_celula_km, centro_lat)

    bordas_lon = _eixos(lon_min, lon_max, delta_lon)
    bordas_lat = _eixos(lat_min, lat_max, delta_lat)

    linhas = []
    for j in range(len(bordas_lat) - 1):
        for i in range(len(bordas_lon) - 1):
            c_lon_min = max(bordas_lon[i], lon_min)
            c_lon_max = min(bordas_lon[i + 1], lon_max)
            c_lat_min = max(bordas_lat[j], lat_min)
            c_lat_max = min(bordas_lat[j + 1], lat_max)
            linhas.append(
                {
                    "celula_id": f"R{j:03d}C{i:03d}",
                    "linha": j,
                    "coluna": i,
                    "lon_min": round(c_lon_min, 6),
                    "lon_max": round(c_lon_max, 6),
                    "lat_min": round(c_lat_min, 6),
                    "lat_max": round(c_lat_max, 6),
                    "centro_lon": round((c_lon_min + c_lon_max) / 2, 6),
                    "centro_lat": round((c_lat_min + c_lat_max) / 2, 6),
                }
            )

    malha = pd.DataFrame(linhas)
    logger.info(
        "Malha gerada",
        extra={
            "celulas": len(malha),
            "tamanho_celula_km": tamanho_celula_km,
            "bbox": bbox,
        },
    )
    return malha


def atribuir_celulas(
    df_pontos: pd.DataFrame,
    coluna_lat: str = "latitude",
    coluna_lon: str = "longitude",
    tamanho_celula_km: float = 1.0,
    bbox: tuple = BBOX_DF,
) -> pd.DataFrame:
    """
    Atribui cada ponto (lat/lon) à célula da malha via aritmética de índices.

    Pontos fora do bbox recebem celula_id nulo. Vetorizado, sem loops.
    """
    for coluna in (coluna_lat, coluna_lon):
        if coluna not in df_pontos.columns:
            raise ValueError(f"Coluna '{coluna}' ausente no DataFrame de pontos")

    lon_min, lat_min, lon_max, lat_max = bbox
    delta_lon, delta_lat = _delta_graus(tamanho_celula_km, (lat_min + lat_max) / 2)

    lon = pd.to_numeric(df_pontos[coluna_lon], errors="coerce")
    lat = pd.to_numeric(df_pontos[coluna_lat], errors="coerce")

    dentro = (lon >= lon_min) & (lon <= lon_max) & (lat >= lat_min) & (lat <= lat_max)

    colunas_idx = np.floor((lon - lon_min) / delta_lon).astype("Int64")
    linhas_idx = np.floor((lat - lat_min) / delta_lat).astype("Int64")

    resultado = df_pontos.copy()
    resultado["celula_id"] = pd.NA
    resultado.loc[dentro, "celula_id"] = [
        f"R{int(l):03d}C{int(c):03d}"
        for l, c in zip(linhas_idx[dentro], colunas_idx[dentro])
    ]

    fora = int((~dentro).sum())
    if fora:
        logger.warning("Pontos fora do bounding box ignorados", extra={"quantidade": fora})
    return resultado


def agregar_por_celula(
    df_gold: pd.DataFrame,
    coluna_valor: str,
    coluna_regiao: str = "regiao_administrativa",
    tamanho_celula_km: float = 1.0,
    bbox: tuple = BBOX_DF,
) -> pd.DataFrame:
    """
    Distribui o total de `coluna_valor` de cada RA para a célula que contém
    o centróide aproximado da RA (aproximação ponto-em-célula).

    RAs sem centróide cadastrado são sinalizadas no log e ignoradas.
    """
    if coluna_regiao not in df_gold.columns or coluna_valor not in df_gold.columns:
        raise ValueError(
            f"Tabela exige as colunas '{coluna_regiao}' e '{coluna_valor}'"
        )

    totais = (
        df_gold.groupby(df_gold[coluna_regiao].map(_chave_regiao))[coluna_valor]
        .sum()
    )

    registros = []
    for chave, valor in totais.items():
        centroide = CENTROIDES_RA.get(chave)
        if centroide is None:
            logger.warning("Sem centróide cadastrado para RA", extra={"ra": chave})
            continue
        lat, lon = centroide
        registros.append({"latitude": lat, "longitude": lon, coluna_valor: float(valor)})

    if not registros:
        logger.warning("Nenhuma RA mapeada; agregação vazia", extra={"esquema": "malha"})
        return pd.DataFrame(columns=["celula_id", coluna_valor])

    pontos = pd.DataFrame(registros)
    atribuido = atribuir_celulas(pontos, tamanho_celula_km=tamanho_celula_km, bbox=bbox)

    agregado = (
        atribuido.dropna(subset=["celula_id"])
        .groupby("celula_id")[coluna_valor]
        .sum()
        .reset_index()
        .sort_values(coluna_valor, ascending=False)
        .reset_index(drop=True)
    )
    logger.info(
        "Indicador agregado por célula",
        extra={"celulas": len(agregado), "indicador": coluna_valor},
    )
    return agregado


def _chave_regiao(valor) -> str:
    """Normaliza nomes de RA (sem acento, maiúsculo) para casar com os centróides."""
    return remover_acentos(str(valor).strip()).upper()
