"""Fixtures de tabelas gold sintéticas para os testes do pacote analysis."""

import numpy as np
import pandas as pd
import pytest

RAS = ["BRASILIA", "GAMA", "TAGUATINGA", "CEILANDIA", "SOBRADINHO"]
ANOS = list(range(2015, 2025))


def _painel(gerador, colunas: dict[str, callable]) -> pd.DataFrame:
    linhas = []
    for ano in ANOS:
        for i, ra in enumerate(RAS):
            registro = {"ano": ano, "regiao_administrativa": ra.title()}
            for nome, formula in colunas.items():
                registro[nome] = formula(ano, i, gerador)
            linhas.append(registro)
    return pd.DataFrame(linhas)


@pytest.fixture
def rng():
    return np.random.default_rng(42)


@pytest.fixture
def dados_gold(rng):
    """Dicionário {tabela: DataFrame} espelhando as gold usadas pelo pipeline."""

    mulher = _painel(
        rng,
        {
            "crimes_contra_mulher": lambda ano, i, g: 50 + (ano - 2015) * 2 + i * 5 + int(g.integers(0, 8)),
            "casos_feminicidios": lambda ano, i, g: 2 + (ano - 2015) // 3 + int(g.integers(0, 3)),
        },
    )

    roubo_furto = _painel(
        rng,
        {
            "ocorrencia_roubo_pedestre": lambda ano, i, g: 200 - (ano - 2015) * 10 + i * 30 + int(g.integers(0, 20)),
            "ocorrencia_roubo_comercio": lambda ano, i, g: 60 + i * 10 + int(g.integers(0, 10)),
            "ocorrencia_roubo_transporte_coletivo": lambda ano, i, g: 10 + int(g.integers(0, 6)),
            "ocorrencia_roubo_veiculo": lambda ano, i, g: 80 + (ano - 2015) * 4 + int(g.integers(0, 15)),
            "ocorrencia_furto_em_veiculo": lambda ano, i, g: 120 + i * 15 + int(g.integers(0, 25)),
        },
    )

    letais = _painel(
        rng,
        {
            "ocorrencia_homicidio": lambda ano, i, g: 20 + i * 4 + int(g.integers(0, 9)),
            "ocorrencia_latrocinio": lambda ano, i, g: 1 + int(g.integers(0, 3)),
            "ocorrencia_lesao_morte": lambda ano, i, g: 2 + int(g.integers(0, 4)),
        },
    )

    discriminatorios = _painel(
        rng,
        {
            "ocorrencia_racismo": lambda ano, i, g: 8 + (ano - 2015) * 2 + int(g.integers(0, 6)),
            "ocorrencia_injuria": lambda ano, i, g: 1 + int(g.integers(0, 3)),
        },
    )

    idosos = pd.DataFrame(
        {
            "regiao_administrativa": [ra.title() for ra in RAS],
            "ranking": list(range(1, len(RAS) + 1)),
            "jan_ago_2016": [40, 35, 28, 22, 12],
            "jan_ago_2017": [44, 33, 30, 21, 11],
        }
    )

    meses = []
    valor_base = 12
    for ano, mes_num in [(2016, m) for m in range(1, 13)] + [(2017, m) for m in range(1, 13)]:
        pico = 90 if (ano == 2017 and mes_num == 6) else 0
        meses.append(
            {
                "ano": ano,
                "mes": f"Mes {mes_num}",
                "mes_num": mes_num,
                "fato": valor_base + int(rng.integers(-2, 3)) + pico,
                "registro": valor_base + int(rng.integers(0, 4)) + pico,
            }
        )
    idosos_mensais = pd.DataFrame(meses)

    return {
        "violencia_contra_mulher_gold": mulher,
        "crimes_roubo_furto_gold": roubo_furto,
        "crimes_letais_gold": letais,
        "crimes_discriminatorios_gold": discriminatorios,
        "violencia_idosos_gold": idosos,
        "violencia_idosos_mensais_gold": idosos_mensais,
    }
