# tests/integracao/dados_gold.py
"""
Dados sintéticos fiéis aos schemas GOLD declarados em
`validation/esquemas.py` para a camada de integração.

Cada função retorna um DataFrame pequeno porém realista (mesmas colunas,
tipos e representatividade do pipeline de produção), permitindo exercitar
persistência, resumo e API com dados de maior fidelidade.
"""

import pandas as pd

RAS = ("TAGUATINGA", "CEILÂNDIA", "PLANALTINA", "GAMA")


def df_violencia_contra_mulher():
    return pd.DataFrame(
        {
            "ano": [2018, 2018, 2019, 2019],
            "regiao_administrativa": ["TAGUATINGA", "CEILÂNDIA", "PLANALTINA", "GAMA"],
            "casos_feminicidios": [2, 3, 1, 0],
            "crimes_contra_mulher": [120, 210, 95, 74],
        }
    )


def df_identificacao_crimes_contra_mulher():
    return pd.DataFrame(
        {
            "ano": [2019, 2019, 2020],
            "regiao_administrativa": ["TAGUATINGA", "CEILÂNDIA", "GAMA"],
            "meio_utilizado": ["arma branca", "arma de fogo", "outros"],
            "local": ["residência", "via pública", "residência"],
            "motivacao": ["violência doméstica", "violência doméstica", "outros"],
            "idade_vitima": [31, 44, 28],
            "idade_autor": [38, 50, 33],
            "data_do_crime": pd.to_datetime(["2019-03-10", "2019-07-22", "2020-01-05"]),
        }
    )


def df_violencia_idosos():
    return pd.DataFrame(
        {
            "ranking": [1, 2, 3],
            "regiao_administrativa": ["GAMA", "BRAZNLÂNDIA", "TAGUATINGA"],
            "jan_ago_2016": [88, 51, 40],
            "jan_ago_2017": [63, 59, 33],
        }
    ).assign(regiao_administrativa=lambda d: d["regiao_administrativa"].replace("BRAZNLÂNDIA", "BRAZLÂNDIA"))


def df_violencia_idosos_ocorrencias():
    return pd.DataFrame(
        {
            "ano": [2010, 2011, 2012],
            "ocorrencias": [55, 63, 71],
            "violencia_dentro_de_casa": [22, 25, 30],
        }
    )


def df_violencia_idosos_mensais():
    return pd.DataFrame(
        {
            "ano": [2016, 2016, 2016],
            "mes": ["JAN", "FEV", "MAR"],
            "mes_num": [1, 2, 3],
            "fato": [54, 59, 47],
            "registro": [59, 63, 51],
        }
    )


def df_violencia_idosos_sexo():
    return pd.DataFrame(
        {
            "ano": [2010, 2011],
            "masculino": [36, 32],
            "feminino": [34, 49],
        }
    )


def df_crimes_roubo_furto():
    return pd.DataFrame(
        {
            "ano": [2020, 2020, 2021],
            "regiao_administrativa": ["TAGUATINGA", "CEILÂNDIA", "PLANALTINA"],
            "ocorrencia_roubo_pedestre": [900, 1200, 650],
            "ocorrencia_roubo_comercio": [210, 180, 90],
            "ocorrencia_roubo_transporte_coletivo": [95, 140, 60],
            "ocorrencia_roubo_veiculo": [310, 420, 150],
            "ocorrencia_furto_em_veiculo": [280, 260, 130],
        }
    )


def df_crimes_letais():
    return pd.DataFrame(
        {
            "ano": [2020, 2021, 2021],
            "regiao_administrativa": ["TAGUATINGA", "CEILÂNDIA", "PLANALTINA"],
            "ocorrencia_homicidio": [12, 20, 5],
            "ocorrencia_latrocinio": [2, 4, 1],
            "ocorrencia_lesao_morte": [1, 2, 0],
        }
    )


def df_crimes_discriminatorios():
    return pd.DataFrame(
        {
            "ano": [2021, 2022, 2022],
            "regiao_administrativa": ["TAGUATINGA", "CEILÂNDIA", "GAMA"],
            "ocorrencia_racismo": [3, 7, 2],
            "ocorrencia_injuria": [9, 12, 5],
        }
    )


def df_desaparecidos_idade_sexo():
    return pd.DataFrame(
        {
            "ano": [2020, 2020, 2020],
            "faixa_etaria": ["0 A 17 ANOS", "18 A 29 ANOS", "30 A 59 ANOS"],
            "sexo": ["MASCULINO", "FEMININO", "MASCULINO"],
            "quantidade": [6, 8, 10],
        }
    )


def df_desaparecidos_localizados():
    return pd.DataFrame(
        {
            "ano": [2020, 2021, 2021],
            "faixa_etaria": ["0 A 17 ANOS", "18 A 29 ANOS", "0 A 17 ANOS"],
            "status": ["LOCALIZADOS", "AINDA DESAPARECIDOS", "LOCALIZADOS"],
            "quantidade": [30, 12, 9],
        }
    )


def df_desaparecidos_regiao():
    return pd.DataFrame(
        {
            "regiao_administrativa": ["CEILÂNDIA", "TAGUATINGA"],
            "ocorrencias_2020": [100, 50],
            "ocorrencias_2021": [120, 60],
        }
    )


def df_vazia(colunas):
    return pd.DataFrame({coluna: pd.Series(dtype="float64") for coluna in colunas})


# mapa nome da tabela gold -> função geradora de dados realistas
GOLD_DF = {
    "violencia_contra_mulher_gold": df_violencia_contra_mulher,
    "identificacao_crimes_contra_mulher_gold": df_identificacao_crimes_contra_mulher,
    "violencia_idosos_gold": df_violencia_idosos,
    "violencia_idosos_ocorrencias_gold": df_violencia_idosos_ocorrencias,
    "violencia_idosos_mensais_gold": df_violencia_idosos_mensais,
    "violencia_idosos_sexo_gold": df_violencia_idosos_sexo,
    "crimes_roubo_furto_gold": df_crimes_roubo_furto,
    "crimes_letais_gold": df_crimes_letais,
    "crimes_discriminatorios_gold": df_crimes_discriminatorios,
    "desaparecidos_idade_sexo_gold": df_desaparecidos_idade_sexo,
    "desaparecidos_localizados_gold": df_desaparecidos_localizados,
    "desaparecidos_regiao_gold": df_desaparecidos_regiao,
}


def df_da_gold(tabela: str):
    """Retorna dado realista da tabela gold para os testes de integração."""
    return GOLD_DF[tabela]()