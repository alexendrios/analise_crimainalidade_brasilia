# geoespacial/__init__.py
"""
Camada geoespacial do projeto: malha de células (grid) sobre o DF.

- `gerar_malha`: células regulares a partir do bbox do DF (puro pandas).
- `atribuir_celulas`: ponto → célula, vetorizado.
- `agregar_por_celula`: indicadores por RA distribuídos na malha.
- `geoespacial.postgis`: espelhamento opcional da malha em PostGIS.
"""

from geoespacial.malha import (
    BBOX_DF,
    agregar_por_celula,
    atribuir_celulas,
    gerar_malha,
)

__all__ = ["BBOX_DF", "gerar_malha", "atribuir_celulas", "agregar_por_celula"]
