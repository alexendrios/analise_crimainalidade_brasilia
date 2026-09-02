import pandas as pd
import pytest

from util.padronizacao import MAPEAMENTO_REGIOES_ADMINISTRATIVAS
from validation import qualidade_dados as q
from validation.schema import DATA, NUMERICO, TEXTO, EsquemaTabela


def _esquema_perfeito():
    return EsquemaTabela(
        nome="crimes_letais_gold",
        colunas={
            "ano": NUMERICO,
            "regiao_administrativa": TEXTO,
            "ocorrencia_homicidio": NUMERICO,
            "ocorrencia_latrocinio": NUMERICO,
            "ocorrencia_lesao_morte": NUMERICO,
            "inserido_em": DATA,
        },
        chaves=("ano", "regiao_administrativa"),
    )


def _tabela_perfeita():
    anos = list(range(2015, 2025))
    agora = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)
    return pd.DataFrame(
        {
            "ano": anos,
            "regiao_administrativa": ["PLANO PILOTO"] * len(anos),
            "ocorrencia_homicidio": [20 + i for i in anos],
            "ocorrencia_latrocinio": [5] * len(anos),
            "ocorrencia_lesao_morte": [5] * len(anos),
            "inserido_em": [agora] * len(anos),
        }
    )


# ============================================================
# Configuração das dimensões
# ============================================================
def test_dimensoes_e_pesos_consolidados():
    assert len(q.DIMENSOES) == 6
    assert all(
        chave in q.PESOS and chave in q.ROTULOS for chave, _rotulo, _peso in q.DIMENSOES
    )
    assert sum(peso for _chave, _rotulo, peso in q.DIMENSOES) == pytest.approx(1.0)


def test_heuristica_coluna_ra():
    assert q._e_coluna_ra("regiao_administrativa")
    assert q._e_coluna_ra("região")
    assert q._e_coluna_ra("RA")
    assert not q._e_coluna_ra("bairro")
    assert not q._e_coluna_ra("ano")


def test_heuristica_coluna_contagem():
    assert q._e_coluna_contagem("ocorrencias")
    assert q._e_coluna_contagem("ocorrencia_roubo_pedestre")
    assert q._e_coluna_contagem("crimes_contra_mulher")
    assert q._e_coluna_contagem("total")
    assert not q._e_coluna_contagem("variacao_percentual")
    assert not q._e_coluna_contagem("percentual_de_furtos")
    assert not q._e_coluna_contagem("residual")
    assert not q._e_coluna_contagem("previsao_roubo")
    assert not q._e_coluna_contagem("regiao_administrativa")


# ============================================================
# avaliar_tabela
# ============================================================
def test_tabela_vazia_ou_ausente_escore_zero():
    for df in (None, pd.DataFrame()):
        item = q.avaliar_tabela("a_gold", df, _esquema_perfeito(), {"PLANO PILOTO"})
        assert item["materializada"] is False
        assert item["escore"] == 0.0
        assert item["dimensoes"] == []
        assert "não materializada" in item["problemas"][0]


def test_tabela_perfeita_escore_cem():
    item = q.avaliar_tabela(
        "crimes_letais_gold", _tabela_perfeita(), _esquema_perfeito(), {"PLANO PILOTO"}
    )
    assert item["materializada"] is True
    assert item["escore"] == 100.0
    assert item["problemas"] == []
    for d in item["dimensoes"]:
        assert d["aplicavel"] is True
        assert d["escore"] == 100.0


def test_dimensoes_nao_aplicaveis_excluidas_e_pesos_redistribuidos():
    df = pd.DataFrame(
        {"nome": ["a", "b", "c"], "data": ["2020-01-01"] * 3}
    )
    esquema = EsquemaTabela(nome="s", colunas={"nome": TEXTO, "data": DATA})
    item = q.avaliar_tabela("s", df, esquema, {"X"})

    aplicaveis = {d["chave"] for d in item["dimensoes"] if d["aplicavel"]}
    assert aplicaveis == {"completude", "validade_schema"}

    completude = next(d for d in item["dimensoes"] if d["chave"] == "completude")
    validade = next(d for d in item["dimensoes"] if d["chave"] == "validade_schema")
    assert completude["escore"] == 100.0
    assert validade["escore"] == 50.0
    assert item["escore"] == pytest.approx((0.25 * 100 + 0.20 * 50) / 0.45, abs=0.01)
    assert any("tipo incompatível" in p for p in item["problemas"])


def test_completude_penaliza_nulos_em_colunas_obrigatorias():
    df = _tabela_perfeita().copy()
    df.loc[0:2, "ocorrencia_homicidio"] = None
    item = q.avaliar_tabela(
        "crimes_letais_gold", df, _esquema_perfeito(), {"PLANO PILOTO"}
    )
    completude = next(d for d in item["dimensoes"] if d["chave"] == "completude")
    assert completude["escore"] == 95.0
    assert any("nulo(s)" in a for a in item["avisos"])
    assert item["escore"] < 100.0


def test_unicidade_penaliza_duplicatas_nas_chaves():
    df = _tabela_perfeita().copy()
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    item = q.avaliar_tabela(
        "crimes_letais_gold", df, _esquema_perfeito(), {"PLANO PILOTO"}
    )
    unicidade = next(d for d in item["dimensoes"] if d["chave"] == "unicidade")
    assert unicidade["escore"] == pytest.approx((11 - 2) / 11 * 100, abs=0.01)
    assert any("duplicado(s)" in p for p in item["problemas"])


def test_consistencia_detecta_ra_fora_do_dominio():
    df = _tabela_perfeita().copy()
    df["regiao_administrativa"] = ["REPÚBLICA DA CEILÂNDIA"] * len(df)
    item = q.avaliar_tabela(
        "crimes_letais_gold", df, _esquema_perfeito(), {"PLANO PILOTO"}
    )
    consistencia = next(d for d in item["dimensoes"] if d["chave"] == "consistencia")
    assert consistencia["escore"] < 100.0
    assert any("fora do domínio canônico" in p for p in item["problemas"])


def test_consistencia_detecta_ano_fora_do_periodo():
    ra_referencia = {"PLANO PILOTO"}
    df = pd.DataFrame(
        {
            "ano": [1985],
            "regiao_administrativa": ["PLANO PILOTO"],
            "ocorrencia_homicidio": [1],
        }
    )
    esquema = EsquemaTabela(
        nome="g",
        colunas={
            "ano": NUMERICO,
            "regiao_administrativa": TEXTO,
            "ocorrencia_homicidio": NUMERICO,
        },
    )
    item = q.avaliar_tabela("g", df, esquema, ra_referencia)
    assert any("ano fora de" in p for p in item["problemas"])


def test_consistencia_detecta_contagem_negativa():
    df = _tabela_perfeita().copy()
    df.loc[:, "ocorrencia_homicidio"] = -1
    item = q.avaliar_tabela(
        "crimes_letais_gold", df, _esquema_perfeito(), {"PLANO PILOTO"}
    )
    assert any("negativos" in p for p in item["problemas"])


def test_atualidade_defasada_escore_zero():
    df = _tabela_perfeita().copy()
    df["inserido_em"] = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=400)
    item = q.avaliar_tabela(
        "crimes_letais_gold", df, _esquema_perfeito(), {"PLANO PILOTO"}
    )
    atualidade = next(d for d in item["dimensoes"] if d["chave"] == "atualidade")
    assert atualidade["escore"] == 0.0
    assert any("defasados" in p for p in item["problemas"])
    assert item["escore"] == pytest.approx(90.0, abs=0.01)


def test_atualidade_decai_linearmente_entre_30_e_365_dias():
    df = _tabela_perfeita().copy()
    df["inserido_em"] = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=200)
    item = q.avaliar_tabela(
        "crimes_letais_gold", df, _esquema_perfeito(), {"PLANO PILOTO"}
    )
    atualidade = next(d for d in item["dimensoes"] if d["chave"] == "atualidade")
    esperado = 100.0 * (q.VALIDADE_MAXIMA_DIAS - 200) / (
        q.VALIDADE_MAXIMA_DIAS - q.FRESCURA_MAXIMA_DIAS
    )
    assert atualidade["escore"] == pytest.approx(esperado, abs=0.01)


def test_atualidade_recente_escore_cem_com_aviso():
    df = _tabela_perfeita().copy()
    df["inserido_em"] = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=10)
    item = q.avaliar_tabela(
        "crimes_letais_gold", df, _esquema_perfeito(), {"PLANO PILOTO"}
    )
    atualidade = next(d for d in item["dimensoes"] if d["chave"] == "atualidade")
    assert atualidade["escore"] == 100.0
    assert item["problemas"] == []
    assert any("última atualização" in a for a in item["avisos"])


def test_cobertura_temporal_parcial():
    df = _tabela_perfeita().copy()
    df["ano"] = [2020, 2021] * 5
    item = q.avaliar_tabela(
        "crimes_letais_gold", df, _esquema_perfeito(), {"PLANO PILOTO"}
    )
    cobertura = next(d for d in item["dimensoes"] if d["chave"] == "cobertura_temporal")
    assert cobertura["escore"] == 20.0
    assert any("cobre 2 de 10 anos" in a for a in item["avisos"])


# ============================================================
# avaliar_qualidade_dados (consolidado)
# ============================================================
def test_consolidado_considera_nao_materializadas_como_zero():
    df = _tabela_perfeita()
    esquemas = {"crimes_letais_gold": _esquema_perfeito()}
    resultado = q.avaliar_qualidade_dados(
        {"crimes_letais_gold": df},
        ["crimes_letais_gold", "desaparecidos_regiao_gold"],
        esquemas,
    )

    assert resultado["total_tabelas"] == 2
    assert resultado["materializadas"] == 1
    assert resultado["escore_geral"] == 50.0
    assert len(resultado["dimensoes"]) == 6
    assert resultado["dimensoes"][0]["chave"] == "completude"
    assert resultado["tabelas"][1]["escore"] == 0.0
    assert resultado["tabelas"][0]["escore"] == 100.0
    assert "gerado_em" in resultado


def test_consolidado_vazio_escore_zero():
    resultado = q.avaliar_qualidade_dados({}, [], {})
    assert resultado["total_tabelas"] == 0
    assert resultado["materializadas"] == 0
    assert resultado["escore_geral"] == 0.0


def test_consolidado_usa_mapeamento_canonico_de_ras():
    assert "PLANO PILOTO" in {
        v.upper() for v in MAPEAMENTO_REGIOES_ADMINISTRATIVAS.values()
    } or "PLANO PILOTO" in {
        k.upper() for k in MAPEAMENTO_REGIOES_ADMINISTRATIVAS.keys()
    }