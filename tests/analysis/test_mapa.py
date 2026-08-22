import folium
import geopandas as gpd
import pandas as pd
import pytest

from analysis.mapa import (
    gerar_agregado_celulas,
    gerar_camada_malha,
    gerar_mapa_calor,
    salvar_mapa,
)
from geoespacial.malha import gerar_malha


@pytest.fixture
def agregado_sintetico(dados_gold):
    return gerar_agregado_celulas(
        dados_gold["crimes_roubo_furto_gold"], "ocorrencia_roubo_pedestre"
    )


def test_agregado_usa_ultimo_ano_disponivel(agregado_sintetico):
    assert not agregado_sintetico.empty
    assert {"celula_id", "ocorrencia_roubo_pedestre"}.issubset(agregado_sintetico.columns)


def test_agregado_sem_coluna_ano_levanta_erro():
    with pytest.raises(ValueError, match="coluna 'ano'"):
        gerar_agregado_celulas(pd.DataFrame({"ra": ["Gama"]}), "valor")


def test_camada_malha_produz_geometrias_validas(agregado_sintetico):
    camada = gerar_camada_malha(agregado_sintetico, "ocorrencia_roubo_pedestre")

    assert isinstance(camada, gpd.GeoDataFrame)
    assert (camada.geometry.type == "Polygon").all()
    assert camada.crs.to_epsg() == 4326
    assert camada["ocorrencia_roubo_pedestre"].notna().all()


def test_camada_vazia_levanta_erro():
    malha = gerar_malha(tamanho_celula_km=11.0, bbox=(-48.0, -16.0, -47.8, -15.8))
    falso = malha.head(2)[["celula_id"]].assign(valor=1.0)
    falso["celula_id"] = "INEXISTENTE"

    with pytest.raises(ValueError, match="Nenhuma célula"):
        gerar_camada_malha(falso, "valor")


def test_mapa_calor_contem_heatmap_e_salva_html(agregado_sintetico, tmp_path):
    mapa = gerar_mapa_calor(
        agregado_sintetico, "ocorrencia_roubo_pedestre", titulo="Teste DF"
    )

    assert isinstance(mapa, folium.Map)
    filhos = [type(filho).__name__ for filho in mapa._children.values()]
    assert "HeatMap" in filhos

    caminho = salvar_mapa(mapa, tmp_path / "mapas" / "teste.html")
    conteudo = caminho.read_text(encoding="utf-8")
    assert "<html" in conteudo.lower()
    assert "Teste DF" in conteudo


def test_mapa_calor_exige_colunas():
    with pytest.raises(ValueError, match="celula_id"):
        gerar_mapa_calor(pd.DataFrame({"x": [1]}), "valor")


def test_mapa_calor_sem_celulas_validas_levanta_erro():
    vazio = pd.DataFrame({"celula_id": ["NADA"], "valor": [1.0]})

    with pytest.raises(ValueError, match="sem células válidas"):
        gerar_mapa_calor(vazio, "valor")
