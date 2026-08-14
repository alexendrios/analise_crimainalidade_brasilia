from unittest.mock import patch

import pandas as pd

from domain.violencia_mulher import ViolenciaMulherService

# ============================================================
# Testes de regressão do refactor de "padronização de RA espalhada"
# (ver Observações e Pontos de Atenção no README).
#
# Antes do refactor, ViolenciaMulherService aplicava as variantes de nome
# de RA de duas formas diferentes dentro da mesma classe:
#   - carregar_crimes_contra_mulher: chamadas sequenciais a renomear_linha
#   - carregar_feminicidio: um dicionário .replace({...}) inline
# Ambas agora usam o mapeamento mestre único
# (util.padronizacao.MAPEAMENTO_REGIOES_ADMINISTRATIVAS via
# renomear_regioes_conhecidas). Estes testes garantem que o comportamento
# observável não mudou.
# ============================================================


@patch("domain.violencia_mulher.Repository.load")
def test_carregar_feminicidio_aplica_mapeamento_mestre_de_ra(mock_load):
    # formato wide: uma coluna por ano, como esperado por transformar_wide_para_long
    mock_load.return_value = pd.DataFrame(
        {
            "região_administrativa": [
                "BRASILIA (PLANO PILOTO)",
                "SIA",
                "GAMA",
            ],
            "2020": [1, 2, 3],
            "2021": [4, 5, 6],
        }
    )

    resultado = ViolenciaMulherService.carregar_feminicidio()

    valores_ra = set(resultado["regiao_administrativa"])
    assert "PLANO PILOTO" in valores_ra
    assert "SIA (SETOR DE INDUSTRIA E ABASTECIMENTO)" in valores_ra
    assert "GAMA" in valores_ra  # sem correspondência no mapa -> intocado
    assert "BRASILIA (PLANO PILOTO)" not in valores_ra
    assert "SIA" not in valores_ra
    assert "casos_feminicidios" in resultado.columns


@patch("domain.violencia_mulher.Repository.load")
def test_carregar_crimes_contra_mulher_aplica_mapeamento_mestre_de_ra(mock_load):
    mock_load.return_value = pd.DataFrame(
        {
            "data_do_crime": ["2020-01-01", "2020-06-15", "2021-03-10"],
            "ra": ["SUDOESTE", "SCIA E ESTRUTURAL", "CEILANDIA"],
            "#_casos": [1, 2, 3],
        }
    )

    resultado, _raw = ViolenciaMulherService.carregar_crimes_contra_mulher()

    valores_ra = set(resultado["regiao_administrativa"])
    assert "SUDOESTE/OCTOGONAL" in valores_ra
    assert "SCIA/ESTRUTURAL" in valores_ra
    assert "CEILANDIA" in valores_ra
    assert "SUDOESTE" not in valores_ra
    assert "SCIA E ESTRUTURAL" not in valores_ra

    # recriar_regiao_com_valor continua funcionando após o refactor:
    # VARJAO e LAGO NORTE devem ser recriados para cada ano com valor 0
    assert "VARJAO" in valores_ra
    assert "LAGO NORTE" in valores_ra


@patch("domain.violencia_mulher.Repository.load")
def test_consolidar_mescla_feminicidio_e_crimes_contra_mulher(mock_load):
    df_feminicidio_bruto = pd.DataFrame(
        {
            "região_administrativa": ["CEILANDIA", "GAMA"],
            "2020": [1, 2],
            "2021": [3, 4],
        }
    )
    df_crimes_bruto = pd.DataFrame(
        {
            "data_do_crime": ["2020-01-01", "2020-06-15", "2021-03-10"],
            "ra": ["CEILANDIA", "GAMA", "CEILANDIA"],
            "#_casos": [5, 6, 7],
        }
    )

    mock_load.side_effect = [df_feminicidio_bruto, df_crimes_bruto]

    resultado = ViolenciaMulherService.consolidar()

    assert {"ano", "regiao_administrativa", "crimes_contra_mulher"} <= set(resultado.columns)
    # crimes_contra_mulher deve ter sido convertido para int
    assert pd.api.types.is_integer_dtype(resultado["crimes_contra_mulher"])
    # linhas com ano == 0 (geradas por recriar_regiao_com_valor sem ano definido,
    # se houver) devem ter sido removidas
    assert (resultado["ano"] != 0).all()
