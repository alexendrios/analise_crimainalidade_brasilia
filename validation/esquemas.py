# validation/esquemas.py
"""
Registro declarativo de schemas por camada.

- SILVER: chaves = nomes dos steps em `src/pipeline_busca_transformacao.py`;
  o validador lê o CSV de saída gravado pelo tratamento.
- GOLD: chaves = nomes das tabelas gold; o validador recebe o DataFrame
  retornado pelo serviço de domínio, antes da persistência.
"""

from functools import partial
from pathlib import Path

import pandas as pd

from validation.schema import (
    DATA,
    NUMERICO,
    TEXTO,
    ErroSchema,
    EsquemaTabela,
    validador_multi,
    validador_de_esquema,
    validar_schema,
)
from util.log import logs

logger = logs()

PASTA_SILVER_OUTPUT = Path("./data/silver/output")

ANOS_WIDE = tuple(str(ano) for ano in range(2015, 2025))


def _wide(nome: str, coluna_regiao: str) -> EsquemaTabela:
    """Schema padrão das tabelas silver no formato wide (RA × ano)."""
    return EsquemaTabela(
        nome=nome,
        colunas={coluna_regiao: TEXTO, **{ano: NUMERICO for ano in ANOS_WIDE}},
    )


# ------------------------------------------------------------------
# Silver — saídas dos tratamentos (bronze → silver)
# ------------------------------------------------------------------
SILVER = {
    "crimes_contra_mulher": EsquemaTabela(
        nome="silver:crimes_contra_mulher",
        colunas={
            "data_do_crime": TEXTO,
            "ra": TEXTO,
            "#_casos": NUMERICO,
            "meio_utilizado": TEXTO,
            "local": TEXTO,
            "motivação": TEXTO,
            "idade___vítima": NUMERICO,
            "idade___autor": NUMERICO,
        },
    ),
    "feminicidio": _wide("silver:feminicidio", "região_administrativa"),
    "desaparecidos_idade_sexo": EsquemaTabela(
        nome="silver:desaparecidos_idade_sexo",
        colunas={"ano": NUMERICO, "faixa_etaria": TEXTO, "sexo": TEXTO, "quantidade": NUMERICO},
    ),
    "desaparecidos_localizados": EsquemaTabela(
        nome="silver:desaparecidos_localizados",
        colunas={"ano": NUMERICO, "faixa_etaria": TEXTO, "status": TEXTO, "quantidade": NUMERICO},
    ),
    "desaparecidos_regiao": EsquemaTabela(
        nome="silver:desaparecidos_regiao",
        colunas={
            "regiao_administrativa": TEXTO,
            "ocorrencias_2020": NUMERICO,
            "ocorrencias_2021": NUMERICO,
            "variacao_absoluta": NUMERICO,
            "participacao_percentual_2021": NUMERICO,
        },
    ),
    "furto_veiculo": _wide("silver:furto_veiculo", "Região Administrativa"),
    "homicidio": _wide("silver:homicidio", "regiao_administrativa"),
    "idosos_tabela4": EsquemaTabela(
        nome="silver:idosos_tabela4",
        colunas={"ano": NUMERICO, "ocorrencias": NUMERICO, "violencia_dentro_de_casa": NUMERICO},
    ),
    "idosos_tabela5": EsquemaTabela(
        nome="silver:idosos_tabela5",
        colunas={"ano": NUMERICO, "masculino": NUMERICO, "feminino": NUMERICO, "total": NUMERICO},
    ),
    "crimes_idosos_ranking": EsquemaTabela(
        nome="silver:crimes_idosos_ranking",
        colunas={
            "ranking": NUMERICO,
            "regiao_administrativa": TEXTO,
            "jan_ago_2016": NUMERICO,
            "jan_ago_2017": NUMERICO,
            "variacao_percentual": NUMERICO,
            "variacao_absoluta": NUMERICO,
        },
    ),
    "crimes_idosos_mensais": EsquemaTabela(
        nome="silver:crimes_idosos_mensais",
        colunas={
            "ano": NUMERICO,
            "mes": TEXTO,
            "mes_num": NUMERICO,
            "fato": NUMERICO,
            "registro": NUMERICO,
            "subnotificacao": NUMERICO,
        },
    ),
    "injuria_racial": _wide("silver:injuria_racial", "regiao"),
    "latrocinio": _wide("silver:latrocinio", "regiao"),
    "lesao_corporal_morte_regiao": _wide("silver:lesao_corporal_morte_regiao", "regiao"),
    "lesao_corporal_morte_total": _wide("silver:lesao_corporal_morte_total", "regiao"),
    "racismo": _wide("silver:racismo", "regiao"),
    "roubo_pedestre": _wide("silver:roubo_pedestre", "Região Administrativa"),
    "roubo_veiculo": _wide("silver:roubo_veiculo", "Região Administrativa"),
    "roubo_comercio": _wide("silver:roubo_comercio", "Região Administrativa"),
    "roubo_transporte_coletivo": _wide("silver:roubo_transporte_coletivo", "Região Administrativa"),
}

SAIDAS_SILVER_CSV = {
    "crimes_contra_mulher": "crimes-contra-mulher_tratado.csv",
    "feminicidio": "feminicidio_tratado.csv",
    "desaparecidos_idade_sexo": "desaparecidos_idade_sexo_tratado.csv",
    "desaparecidos_localizados": "desaparecimento-localizados_tratado.csv",
    "desaparecidos_regiao": "desaparecimento-regiao_tratado.csv",
    "furto_veiculo": "furto_em_veiculo_tratado.csv",
    "homicidio": "homicidio_tratado.csv",
    "crimes_idosos_ranking": "idosos_2016_tratado.csv",
    "crimes_idosos_mensais": "idosos_mensais_tratado.csv",
    "injuria_racial": "injuria_racial_tratado.csv",
    "latrocinio": "latrocinio_tratado.csv",
    "lesao_corporal_morte_regiao": "lesao_corporal_morte_tratada.csv",
    "lesao_corporal_morte_total": "lesao_corporal_morte_total_tratada.csv",
    "racismo": "racismo_tratado.csv",
    "roubo_pedestre": "roubo-a-transeunte_tratado.csv",
    "roubo_veiculo": "roubo_veiculo_tratado.csv",
    "roubo_comercio": "roubo_comercio.csv",
    "roubo_transporte_coletivo": "roubo_transporte_coletivo_tratado.csv",
}

# ------------------------------------------------------------------
# Gold — saídas dos serviços de domínio (silver → gold)
# ------------------------------------------------------------------
GOLD = {
    "violencia_contra_mulher_gold": EsquemaTabela(
        nome="gold:violencia_contra_mulher_gold",
        colunas={
            "ano": NUMERICO,
            "regiao_administrativa": TEXTO,
            "casos_feminicidios": NUMERICO,
            "crimes_contra_mulher": NUMERICO,
        },
        chaves=("ano", "regiao_administrativa"),
    ),
    "identificacao_crimes_contra_mulher_gold": EsquemaTabela(
        nome="gold:identificacao_crimes_contra_mulher_gold",
        colunas={
            "ano": NUMERICO,
            "regiao_administrativa": TEXTO,
            "meio_utilizado": TEXTO,
            "local": TEXTO,
            "motivacao": TEXTO,
            "idade_vitima": NUMERICO,
            "idade_autor": NUMERICO,
            "data_do_crime": DATA,
        },
    ),
    "violencia_idosos_gold": EsquemaTabela(
        nome="gold:violencia_idosos_gold",
        colunas={
            "ranking": NUMERICO,
            "regiao_administrativa": TEXTO,
            "jan_ago_2016": NUMERICO,
            "jan_ago_2017": NUMERICO,
        },
    ),
    "violencia_idosos_ocorrencias_gold": EsquemaTabela(
        nome="gold:violencia_idosos_ocorrencias_gold",
        colunas={"ano": NUMERICO, "ocorrencias": NUMERICO, "violencia_dentro_de_casa": NUMERICO},
    ),
    "violencia_idosos_mensais_gold": EsquemaTabela(
        nome="gold:violencia_idosos_mensais_gold",
        colunas={
            "ano": NUMERICO,
            "mes": TEXTO,
            "mes_num": NUMERICO,
            "fato": NUMERICO,
            "registro": NUMERICO,
        },
    ),
    "violencia_idosos_sexo_gold": EsquemaTabela(
        nome="gold:violencia_idosos_sexo_gold",
        colunas={"ano": NUMERICO, "masculino": NUMERICO, "feminino": NUMERICO},
    ),
    "desaparecidos_idade_sexo_gold": EsquemaTabela(
        nome="gold:desaparecidos_idade_sexo_gold",
        colunas={"ano": NUMERICO, "faixa_etaria": TEXTO, "sexo": TEXTO, "quantidade": NUMERICO},
    ),
    "desaparecidos_localizados_gold": EsquemaTabela(
        nome="gold:desaparecidos_localizados_gold",
        colunas={"ano": NUMERICO, "faixa_etaria": TEXTO, "status": TEXTO, "quantidade": NUMERICO},
    ),
    "desaparecidos_regiao_gold": EsquemaTabela(
        nome="gold:desaparecidos_regiao_gold",
        colunas={
            "regiao_administrativa": TEXTO,
            "ocorrencias_2020": NUMERICO,
            "ocorrencias_2021": NUMERICO,
        },
        colunas_opcionais=("variacao_percentual",),
    ),
    "crimes_roubo_furto_gold": EsquemaTabela(
        nome="gold:crimes_roubo_furto_gold",
        colunas={
            "ano": NUMERICO,
            "regiao_administrativa": TEXTO,
            "ocorrencia_roubo_pedestre": NUMERICO,
            "ocorrencia_roubo_comercio": NUMERICO,
            "ocorrencia_roubo_transporte_coletivo": NUMERICO,
            "ocorrencia_roubo_veiculo": NUMERICO,
            "ocorrencia_furto_em_veiculo": NUMERICO,
        },
        chaves=("ano", "regiao_administrativa"),
    ),
    "crimes_letais_gold": EsquemaTabela(
        nome="gold:crimes_letais_gold",
        colunas={
            "ano": NUMERICO,
            "regiao_administrativa": TEXTO,
            "ocorrencia_homicidio": NUMERICO,
            "ocorrencia_latrocinio": NUMERICO,
            "ocorrencia_lesao_morte": NUMERICO,
        },
        chaves=("ano", "regiao_administrativa"),
    ),
    "crimes_discriminatorios_gold": EsquemaTabela(
        nome="gold:crimes_discriminatorios_gold",
        colunas={
            "ano": NUMERICO,
            "regiao_administrativa": TEXTO,
            "ocorrencia_racismo": NUMERICO,
            "ocorrencia_injuria": NUMERICO,
        },
        chaves=("ano", "regiao_administrativa"),
    ),
}


def validar_saida_silver(nome_step: str, resultado) -> None:
    """
    Valida a saída de um tratamento silver.

    Steps que retornam DataFrame(s) são validados direto; os que gravam CSV
    sem retorno têm o arquivo relido e validado contra o schema registrado.
    """
    if nome_step == "violencia_idosos":
        validador_multi(SILVER["idosos_tabela4"], SILVER["idosos_tabela5"])(resultado)
        return

    esquema = SILVER.get(nome_step)
    if esquema is None:
        logger.warning("Sem schema registrado para o step silver '%s'", nome_step)
        return

    if resultado is not None:
        validador_de_esquema(esquema)(resultado)
        return

    caminho_csv = SAIDAS_SILVER_CSV.get(nome_step)
    if caminho_csv is None:
        logger.warning(
            "Step silver '%s' não retorna dados e não tem CSV mapeado; nada a validar",
            nome_step,
        )
        return

    caminho = PASTA_SILVER_OUTPUT / caminho_csv
    if not caminho.exists():
        raise ErroSchema(f"[{esquema.nome}] arquivo de saída não encontrado: {caminho}")

    df = pd.read_csv(caminho, sep=";")
    logger.info(
        "Relendo saída silver para schema check",
        extra={"step": nome_step, "arquivo": str(caminho)},
    )
    validar_schema(df, esquema)


def validador_silver(nome_step: str):
    """Hook pronto para `PipelineStep.validacao` de um step silver."""

    def _validar(resultado) -> None:
        validar_saida_silver(nome_step, resultado)

    return _validar
