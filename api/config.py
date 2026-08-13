# api/config.py
"""
Configuração da camada de API (Consumo).

Mantém, de forma declarativa, a lista de tabelas *_gold conhecidas do
projeto (espelhando os PipelineSteps definidos em
`src/pipeline_tabela_gold.py`) para que a API não precise inspecionar o
banco a cada requisição apenas para saber "o que existe".

A validação final de nome de tabela continua sendo feita em
`database/repository/repository.py` (via `str.isidentifier()`), então
mesmo que esta lista fique desatualizada a API não corre risco de SQL
injection — ela apenas fica menos "amigável" na listagem.
"""

from typing import Dict

# nome_da_tabela_gold -> descrição amigável (pt-BR) exibida na API
TABELAS_GOLD: Dict[str, str] = {
    "violencia_contra_mulher_gold": "Crimes contra a mulher, consolidado anual por RA",
    "identificacao_crimes_contra_mulher_gold": "Identificação/tipificação de crimes contra a mulher",
    "violencia_idosos_gold": "Violência contra idosos - resumo",
    "violencia_idosos_ocorrencias_gold": "Violência contra idosos - ocorrências",
    "violencia_idosos_mensais_gold": "Violência contra idosos - série mensal",
    "violencia_idosos_sexo_gold": "Violência contra idosos - por sexo",
    "crimes_roubo_furto_gold": "Crimes patrimoniais (roubo/furto)",
    "crimes_letais_gold": "Crimes letais violentos intencionais",
    "crimes_discriminatorios_gold": "Crimes discriminatórios",
    "desaparecidos_idade_sexo_gold": "Pessoas desaparecidas - por idade e sexo",
    "desaparecidos_localizados_gold": "Pessoas desaparecidas - localizados",
    "desaparecidos_regiao_gold": "Pessoas desaparecidas - por RA",
}

# Coluna de ano usada nos filtros (?ano_min=&ano_max=) para as tabelas
# que possuem série temporal anual. Tabelas fora desta lista simplesmente
# ignoram o filtro de ano.
COLUNA_ANO_POR_TABELA: Dict[str, str] = {
    "violencia_contra_mulher_gold": "ano",
    "identificacao_crimes_contra_mulher_gold": "ano",
    "crimes_roubo_furto_gold": "ano",
    "crimes_letais_gold": "ano",
    "crimes_discriminatorios_gold": "ano",
}

# Tabela/coluna usadas pelo endpoint de previsão
TABELA_MODELO_PREVISAO = "violencia_contra_mulher_gold"
COLUNA_ALVO_PREVISAO = "crimes_contra_mulher"

# TTL (segundos) do cache em memória da previsão, evitando re-treinar o
# modelo (Prophet + XGBoost) a cada requisição.
CACHE_PREVISAO_TTL_SEGUNDOS = 60 * 30  # 30 minutos
