from unittest.mock import patch

import pandas as pd

from domain.identificacao_crimes import IdentificacaoCrimesService

# Ver nota em tests/domain/test_violencia_mulher.py: mesmo refactor de
# "padronização de RA espalhada", desta vez cobrindo
# IdentificacaoCrimesService, que antes fazia suas próprias duas chamadas
# a renomear_linha (duplicando as mesmas regras usadas em
# ViolenciaMulherService).


@patch("domain.violencia_mulher.Repository.load")
def test_carregar_aplica_mapeamento_mestre_de_ra(mock_load):
    mock_load.return_value = pd.DataFrame(
        {
            "data_do_crime": ["2020-01-01", "2020-06-15"],
            "ra": ["SUDOESTE", "SCIA E ESTRUTURAL"],
            "#_casos": [1, 1],
            "meio_utilizado": ["ARMA DE FOGO", "FISICA"],
            "local": ["RESIDENCIA", "VIA PUBLICA"],
            "motivação": ["CIUME", "DISCUSSAO"],
            "idade___vítima": [25, 30],
            "idade___autor": [40, 35],
        }
    )

    resultado = IdentificacaoCrimesService.carregar()

    valores_ra = set(resultado["regiao_administrativa"])
    assert "SUDOESTE/OCTOGONAL" in valores_ra
    assert "SCIA/ESTRUTURAL" in valores_ra
    assert "SUDOESTE" not in valores_ra
    assert "SCIA E ESTRUTURAL" not in valores_ra
