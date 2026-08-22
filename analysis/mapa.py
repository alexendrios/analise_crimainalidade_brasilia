# analysis/mapa.py
"""
Visualização geoespacial de zonas quentes de criminalidade.

Combina a malha regular de células (`geoespacial.malha`) com Folium +
GeoPandas para produzir um mapa de calor interativo (HTML) dos indicadores
agregados por célula — sem depender de shapefiles externos.
"""

from pathlib import Path

import folium
import geopandas as gpd
import numpy as np
import pandas as pd
from folium.plugins import HeatMap
from shapely.geometry import box

from geoespacial.malha import BBOX_DF, agregar_por_celula, gerar_malha
from util.log import logs

logger = logs()

CENTRO_DF = (
    (BBOX_DF[0] + BBOX_DF[2]) / 2,
    (BBOX_DF[1] + BBOX_DF[3]) / 2,
)


def gerar_agregado_celulas(
    tabela_gold: pd.DataFrame,
    coluna_valor: str,
    tamanho_celula_km: float = 1.5,
) -> pd.DataFrame:
    """
    Distribui o indicador da tabela gold (ano x RA) para as células da malha
    usando os centróides aproximados das RAs.

    Por padrão usa o último ano disponível da tabela (recorte mais recente).
    """
    df = pd.DataFrame(tabela_gold)
    if "ano" not in df.columns:
        raise ValueError("Tabela gold exige coluna 'ano' para escolher o recorte")

    ano_recente = int(df["ano"].max())
    recorte = df.query("ano == @ano_recente")

    logger.info(
        "Agregando indicador na malha",
        extra={"indicador": coluna_valor, "ano": ano_recente},
    )
    return agregar_por_celula(recorte, coluna_valor, tamanho_celula_km=tamanho_celula_km)


def gerar_camada_malha(agregado: pd.DataFrame, coluna_valor: str) -> gpd.GeoDataFrame:
    """
    Junta os valores agregados à geometria das células, retornando um
    GeoDataFrame pronto para exportação/plotagem.
    """
    malha = gerar_malha()
    cruzamento = malha.merge(agregado[["celula_id", coluna_valor]], on="celula_id", how="inner")
    if cruzamento.empty:
        raise ValueError("Nenhuma célula da malha recebeu valores do agregado")

    geometrias = [
        box(lado["lon_min"], lado["lat_min"], lado["lon_max"], lado["lat_max"])
        for _, lado in cruzamento.iterrows()
    ]
    return gpd.GeoDataFrame(cruzamento.drop(columns=["centro_lat", "centro_lon"]),
                            geometry=geometrias, crs="EPSG:4326")


def _normalizar_pesos(valores: np.ndarray) -> np.ndarray:
    """Escala linear para [0, 1]; vetor nulo permanece nulo."""
    maximo = valores.max()
    return valores / maximo if maximo > 0 else valores * 0.0


def gerar_mapa_calor(
    agregado: pd.DataFrame,
    coluna_valor: str,
    titulo: str = "Zonas quentes de criminalidade - DF",
) -> folium.Map:
    """
    Mapa de calor Folium a partir dos centróides das células ponderados pelo
    indicador (normalizado para 0-1).
    """
    if coluna_valor not in agregado.columns or "celula_id" not in agregado.columns:
        raise ValueError(f"Agregado exige as colunas 'celula_id' e '{coluna_valor}'")

    pontos = agregado.merge(
        gerar_malha()[["celula_id", "centro_lat", "centro_lon"]],
        on="celula_id",
        how="inner",
    )
    if pontos.empty:
        raise ValueError("Agregado sem células válidas para plotar")

    pesos = _normalizar_pesos(pontos[coluna_valor].to_numpy(dtype=float))

    mapa = folium.Map(location=(CENTRO_DF[1], CENTRO_DF[0]), zoom_start=10)
    HeatMap(
        list(zip(pontos["centro_lat"], pontos["centro_lon"], pesos)),
        radius=18,
        blur=12,
        min_opacity=0.3,
    ).add_to(mapa)
    mapa.get_root().html.add_child(folium.Element(f"<h4>{titulo}</h4>"))

    logger.info("Mapa de calor gerado", extra={"celulas": len(pontos), "titulo": titulo})
    return mapa


def exportar_geopackage(camada: gpd.GeoDataFrame, caminho_saida: str | Path) -> Path:
    """Persiste a camada de células em GeoPackage para reuso em GIS."""
    caminho = Path(caminho_saida)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    camada.to_file(caminho, driver="GPKG")
    logger.info("Camada geoespacial exportada", extra={"caminho": str(caminho)})
    return caminho


def salvar_mapa(mapa: folium.Map, caminho_saida: str | Path) -> Path:
    """Grava o HTML interativo do mapa."""
    caminho = Path(caminho_saida)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    mapa.save(caminho)
    logger.info("Mapa salvo", extra={"caminho": str(caminho)})
    return caminho
