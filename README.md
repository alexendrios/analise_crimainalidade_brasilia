# Projeto Criminalidade Brasília - DF

> **Nota de atualização:** esta documentação foi revisada para refletir o que está **efetivamente implementado no código** na data desta análise. A versão anterior descrevia uma arquitetura geoespacial com PostGIS, malha hexagonal, dashboard Streamlit e API FastAPI — nenhum desses componentes existe hoje no repositório. Eles foram movidos para a seção [Roadmap](#-roadmap--visão-futura), mantidos apenas como direção futura possível.
>
> **Revisão mais recente:** API de consumo (FastAPI) implementada, suíte de testes de `download_arquivo` corrigida (mock de `requests.Session` em vez de `requests.get`, suíte 19min→16s) e padronização de RA consolidada num mapeamento mestre único. Números atualizados: **343 testes, 0 falhas, 100% de cobertura** em `src`, `util` e `database` (limiar mínimo 95%). Três itens antes listados como pendentes em "Pontos de Atenção" já estavam resolvidos no código mas com documentação desatualizada — corrigido nesta revisão: `requirements.txt` curado, metadados `_meta.json` padronizados para todos os modelos, e `correcoes.patch` já removido do repositório. Detalhes em [Observações e Pontos de Atenção](#-observações-e-pontos-de-atenção-herdados-da-análise-técnica-do-projeto).

### Pipeline de Dados
![alt text](image.png)

### Arquitetura
![alt text](image-1.png)

## 🎯 Visão Geral

O projeto coleta, padroniza e consolida séries históricas de criminalidade do Distrito Federal (fontes SSP-DF e dados populacionais do IBGE/GDF), organiza os dados em um **Data Lakehouse em camadas (Bronze → Silver → Gold)** e utiliza o resultado para treinar um modelo híbrido de previsão de séries temporais (Prophet + XGBoost) aplicado hoje a **crimes contra a mulher** por Região Administrativa (RA).

Não há componente geoespacial (sem PostGIS, sem malha de células), sem dashboard e sem API expostos no código atual — o fluxo é executado localmente via scripts Python (`src/main.py`).

## 🧠 Diagrama de Arquitetura Lógica (estado atual)

```bash
        ┌───────────────────────────────────────────┐
        │            Fontes de Dados Externas        │
        │─────────────────────────────────────────────│
        │ - dados.df.gov.br (SSP-DF, planilhas .xlsx) │
        │ - ftp.ibge.gov.br (população por município) │
        │ - Wikipédia (população por RA)              │
        └──────────────────┬──────────────────────────┘
                            │
                            ▼
        ┌───────────────────────────────────────────┐
        │        Camada de Coleta (src/busca.py,     │
        │        src/scraping.py, src/coleta_gdf.py) │
        │─────────────────────────────────────────────│
        │ • Download via requests + rotas.yaml/       │
        │   rotas_ibge.yaml                            │
        │ • Extração de .zip (util/extrator_zip.py)   │
        │ • Leitura de .xlsx → .csv                    │
        │   (util/leitor_excel.py)                     │
        └──────────────────┬──────────────────────────┘
                            │
                            ▼
        ┌───────────────────────────────────────────┐
        │   BRONZE (data/bronze)                     │
        │   CSV/planilhas brutos, sem tratamento     │
        └──────────────────┬──────────────────────────┘
                            │
                            ▼
        ┌───────────────────────────────────────────┐
        │   Camada de Tratamento (src/tratamento_*)   │
        │─────────────────────────────────────────────│
        │ • Padronização de nomes de RA               │
        │   (util/padronizacao.py)                     │
        │ • Wide→Long, normalização de colunas         │
        │ • Validação estrutural (validation/          │
        │   validator.py)                              │
        └──────────────────┬──────────────────────────┘
                            │
                            ▼
        ┌───────────────────────────────────────────┐
        │   SILVER (data/silver/output)               │
        │   CSVs tratados e padronizados               │
        └──────────────────┬──────────────────────────┘
                            │
                            ▼
        ┌───────────────────────────────────────────┐
        │   Banco de Dados PostgreSQL (SQLAlchemy)    │
        │─────────────────────────────────────────────│
        │  • Sem extensão espacial (sem PostGIS)      │
        │  • Estratégia de carga: FULL REFRESH         │
        │    (to_sql if_exists="replace")              │
        │  • Acesso via Repository Pattern             │
        │    (ingestion/repository_adapter.py →        │
        │    database/repository/repository.py)        │
        └──────────────────┬──────────────────────────┘
                            │
                            ▼
        ┌───────────────────────────────────────────┐
        │   GOLD (src/pipeline_tabela_gold.py)        │
        │─────────────────────────────────────────────│
        │  • PipelineStep + executor com               │
        │    ThreadPoolExecutor, retries e timeout      │
        │    (src/core/executor.py)                     │
        │  • Serviços de domínio (domain/*.py)          │
        │    consolidam e gravam tabelas *_gold          │
        └──────────────────┬──────────────────────────┘
                            │
                            ▼
        ┌───────────────────────────────────────────┐
        │   Camada de Modelagem (analysis/            │
        │   data_analyzer.py)                          │
        │─────────────────────────────────────────────│
        │  • Feature engineering: lags, rolling mean,  │
        │    trend, diff                                │
        │  • Prophet → tendência anual                  │
        │  • XGBoost → aprende o resíduo (log) do        │
        │    Prophet                                    │
        │  • Previsão híbrida com clip dinâmico,        │
        │    decaimento temporal e suavização           │
        │  • Modelos exportados em models/*.pkl          │
        │    com metadados em *_meta.json                │
        └───────────────────────────────────────────┘
```

## ⚙️ Componentes Técnicos e Tecnologias (o que está implementado hoje)

| **Camada** | **Ferramentas / Bibliotecas** | **Responsabilidade** |
|:------------|:------------------------------|:----------------------|
| **Coleta** | `requests`, `pandas`, `beautifulsoup4`/scraping próprio | Baixar planilhas SSP-DF, extrair `.zip`, raspar população por RA na Wikipédia |
| **Configuração de rotas** | `rotas.yaml`, `rotas_ibge.yaml`, `config.yaml`, `config/datasets_config.py` | Descrever datasets, resources e URLs de origem sem hardcode no código |
| **Tratamento (Bronze→Silver)** | `pandas`, funções em `src/tratamento_crimes.py` e `src/tratamento_populacional.py` | Limpeza, padronização de nomes de RA, conversão wide→long |
| **Banco de Dados** | `PostgreSQL 16` (via Docker Compose), `SQLAlchemy`, `psycopg2` | Persistência relacional; **sem PostGIS**; carga full refresh |
| **Camada Gold** | Domain Services (`domain/*.py`) + `PipelineStep`/`executor.py` (paralelismo com `ThreadPoolExecutor`, retry e timeout configuráveis) | Consolidar, validar chaves e gravar tabelas `*_gold` |
| **Modelagem Preditiva** | `scikit-learn`, `XGBoost`, `Prophet`, `joblib` | Modelo híbrido Prophet + resíduo XGBoost para prever `crimes_contra_mulher` 5 anos à frente |
| **Testes** | `pytest`, `pytest-cov`, `pytest-html` | 343 testes automatizados, 0 falhas, **100% de cobertura** em `src`, `util` e `database`, limiar mínimo de 95% (`--cov-fail-under=95`) |
| **Ambiente / Infra** | `Docker Compose` (container `postgres:16`), `.env` para credenciais | Ambiente local reprodutível para o banco |

### 🧩 Interações Principais (fluxo real)

- **Coleta → Bronze:** `src/busca.py`, `src/scraping.py` e `util/extrator_zip.py` baixam e descompactam as planilhas originais em `data/bronze`.
- **Bronze → Silver:** `src/pipeline_busca_transformacao.py` orquestra sequencialmente as funções de `src/tratamento_crimes.py` e `src/tratamento_populacional.py`, gerando CSVs padronizados em `data/silver/output`.
- **Silver → Postgres → Gold:** `database/load_csvs.py` carrega os CSVs tratados no Postgres; `src/pipeline_tabela_gold.py` executa os `PipelineStep`s em paralelo, cada um chamando um serviço de domínio (`domain/*.py`) que lê tabelas via `Repository.load`, aplica regras de negócio (padronização de nomes de RA, merges seguros com validação de chaves) e grava o resultado com `Repository.save` (estratégia full refresh).
- **Gold → Modelagem:** `analysis/data_analyzer.py` lê a tabela `violencia_contra_mulher_gold`, gera features temporais, treina Prophet (tendência) + XGBoost (resíduo em log) e produz uma previsão de 5 anos, salvando o modelo em `models/`.
- **Execução:** `src/main.py` é o ponto de entrada único; hoje executa as três etapas em sequência — `busca_transformacao_dados()` (coleta+tratamento), `criar_tabela_gold(max_workers=6)` (camada Gold) e `executar_pipeline()` (modelagem). Todas as três estão cobertas por teste (ver seção de Qualidade e Testes).

## 📁 Estrutura de Diretórios (resumo)

| Diretório | Conteúdo |
|:---|:---|
| `src/` | Coleta, scraping, tratamento de crimes/população, orquestração (`main.py`, `pipeline_*`, `core/`) |
| `domain/` | Serviços de domínio por tema (violência contra a mulher, idosos, crimes letais, patrimoniais, discriminatórios, desaparecidos) |
| `database/` | Conexão SQLAlchemy, repositório de acesso ao Postgres, carga de CSVs |
| `ingestion/` | Adaptador simples (`Repository`) entre domínio e `database/repository` |
| `processing/` | Transformações genéricas de datasets e pós-processamento |
| `validation/` | Validação de chaves e integridade estrutural antes dos merges |
| `analysis/` | Pipeline de modelagem preditiva (Prophet + XGBoost) |
| `util/` | Utilitários (leitura de Excel, extração de zip, logging, padronização de nomes de RA) |
| `config/` | Configuração de datasets (`datasets_config.py`) e paths |
| `models/` | Modelos treinados (`.pkl`) e metadados (`_meta.json`) de cada execução |
| `data/` | Camadas `bronze/`, `silver/`, `gold/` do lakehouse local (gerada em runtime; ignorada pelo Git, ver `.gitignore`) |
| `tests/` | Suíte de testes (`arquivos`, `core`, `dados`, `database`, `pipeline`, `rotas`, `scrapings`, `setup`, `util`) |
| `docker-compose.yaml` | Serviço `postgres:16` para ambiente local |
| `requirements.txt` | Dependências do projeto (freeze do ambiente de desenvolvimento — ver nota na seção "Como Executar") |

> A antiga pasta `docs/` (com `projeto.md`, `image.png` e `image-1.png`) foi removida do repositório; as imagens de arquitetura hoje ficam na raiz (`image.png`, `image-1.png`) e este `README.md` é a documentação central do projeto.

## 🗃️ Tabelas Gold Geradas

| Tabela | Serviço responsável |
|:---|:---|
| `violencia_contra_mulher_gold` | `ViolenciaMulherService.consolidar` |
| `identificacao_crimes_contra_mulher_gold` | `IdentificacaoCrimesService.carregar` |
| `violencia_idosos_gold` / `_ocorrencias_gold` / `_mensais_gold` / `_sexo_gold` | `ViolenciaIdososService` |
| `crimes_roubo_furto_gold` | `CrimesPatrimoniaisService.consolidar` |
| `crimes_letais_gold` | `CrimesLetaisService.consolidar` |
| `crimes_discriminatorios_gold` | `CrimesDiscriminatoriosService.consolidar` |
| `desaparecidos_idade_sexo_gold` / `_localizados_gold` / `_regiao_gold` | `DesaparecimentosService` |

## 🤖 Modelagem Preditiva (`analysis/data_analyzer.py`)

- **Alvo atual:** `crimes_contra_mulher` (contagem anual por RA, agregado no nível DF na tabela gold consumida).
- **Features:** `lag_1`, `lag_2`, `rolling_mean_2`, `rolling_mean_3`, `taxa_feminicidio`, `feminicidio_lag_1`, `trend`, `ano_num`, `diff_1`.
- **Abordagem híbrida:** Prophet modela a tendência/sazonalidade anual; um `XGBRegressor` aprende o resíduo em escala logarítmica entre o valor real e o previsto pelo Prophet.
- **Pós-processamento da previsão:** clipping dinâmico do resíduo pelos percentis 5%/95%, decaimento de 15% por ano projetado (mínimo de 40% do efeito) e suavização exponencial simples entre anos consecutivos.
- **Horizonte:** 5 anos à frente, com atualização recursiva das features (lags, médias móveis) a cada passo.
- **Persistência:** cada execução gera um novo arquivo `models/xgb_residual_log_<timestamp>.pkl`; **não há atualmente um `_meta.json` salvo para os modelos residuais em log** (apenas os modelos `xgb_*` mais antigos possuem metadados de features/métricas — ponto de atenção para padronizar).

## ✅ Qualidade e Testes

- **343 testes** automatizados (`pytest`), **0 falhas**, **cobertura de 100%** sobre `src`, `util` e `database` (limiar mínimo configurado: 95%, `--cov-fail-under=95`).
- Relatórios gerados automaticamente em `test_report/` (HTML + JUnit) e `coverage_report/` (HTML).
- Suíte organizada por domínio: `tests/arquivos`, `tests/core`, `tests/dados`, `tests/database`, `tests/pipeline`, `tests/rotas`, `tests/scrapings`, `tests/setup`, `tests/util`.

### 🔧 Histórico da retomada de cobertura (desta rodada de QA)

A suíte estava documentada como "195 testes, 0 falhas, 99% de cobertura", mas ao rodar de fato revelou-se desatualizada: 16 falhas, 1 arquivo de teste que nem coletava (import quebrado) e cobertura real de ~56%. O trabalho de correção revelou também **bugs reais no código**, não só gaps de teste:

| Bug encontrado | Onde | Causa raiz | Status |
|:---|:---|:---|:---|
| `.gitignore` não ignorava `.env` | raiz do projeto | regra `.env/` (barra final = diretório) em vez de `.env` (arquivo) | **Parcialmente corrigido:** a regra em `.gitignore` já foi ajustada para `.env` (commit `ade0c3d`), mas o arquivo `.env` **continua rastreado pelo Git** (já estava commitado antes do ajuste, e alterar o `.gitignore` não desfaz isso). Ainda é necessário `git rm --cached .env` + commit, e rotacionar qualquer credencial que já tenha sido exposta no histórico |
| Arquivos `octet-stream` viravam `.bin` em vez de `.zip` | `util/arquivos.py::detectar_extensao` | mapeamento de Content-Type não tratava esse tipo — zips nunca eram descompactados | **Corrigido** |
| `filtrar_distrito_federal` parava de reconhecer colunas de texto | `util/leitor_excel.py` | `dtype == object` não bate mais no pandas ≥ 3.0 (novo dtype nativo `str`) | **Corrigido** (`pd.api.types.is_string_dtype`) |
| Busca de header quebrava com células vazias | 9 funções em `src/tratamento_crimes.py` | `row.astype(str)` não converte `NaN` quando a coluna já é dtype `str` (pandas ≥ 3.0) | **Corrigido** (`row.fillna("").astype(str)`) |
| Teste órfão de refactor anterior | `tests/pipeline/test_pipeline.py` | importava `main()` de um módulo (`src/pipeline.py`) que não existe mais | **Corrigido/reescrito** |
| Dependência não declarada | `statsmodels` (usado em `tratamento_populacional.py`) | ausente da lista de dependências conhecidas | Documentado abaixo |
| Código morto/inatingível | `tratar_racismo` em `src/tratamento_crimes.py` | checagem de "coluna 2024 não encontrada" nunca podia falhar, dado o filtro anterior | **Removido** |

### 🧹 Limpeza realizada

Havia um arquivo duplicado em `tests/core/test_tratamento_crimes_basico1.py` com 19 testes idênticos aos de `tests/core/test_tratamento_crimes_basico.py` (cópia acidental durante o desenvolvimento, gerando 322 execuções em vez de 303). O arquivo foi removido; a contagem de 303 testes acima já reflete essa limpeza.

## 🚀 Como Executar (ambiente local)

```bash
# 1. Subir o Postgres local
docker compose up -d

# 2. Configurar variáveis de ambiente (.env) com as credenciais do banco

# 3. Instalar dependências em um ambiente virtual
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 4. Rodar o pipeline (editar src/main.py para habilitar as etapas desejadas)
python -m src.main

# 5. Rodar a suíte de testes
pytest

# 6. Subir a API (camada de consumo)
uvicorn api.main:app --reload --port 8000
# Documentação interativa (Swagger): http://localhost:8000/docs
```

> ✅ **Atualizado nesta revisão:** o `requirements.txt` já é um manifesto **curado** — só dependências de execução (`pandas`, `numpy`, `sqlalchemy`, `psycopg2-binary`, `python-dotenv`, `xgboost`, `prophet`, `scikit-learn`, `statsmodels`, `joblib`, `requests`, `openpyxl`/`xlrd`, `pyyaml`, `beautifulsoup4`, `fastapi`, `uvicorn`), organizado por seção. Ferramentas de ambiente de desenvolvimento/notebook (`jupyter`, `matplotlib`, `plotly`, `shap`, `docker`, `testcontainers`, `pywin32`/`pywinpty` — este último específico de Windows) já estão isoladas em `requirements-dev.txt`, que não é necessário para rodar o projeto em produção.

## 🌐 Camada de Consumo (API — `api/`)

Uma API REST em **FastAPI** expõe as tabelas gold e o modelo preditivo sem alterar o pipeline de coleta/tratamento/modelagem existente — ela apenas reaproveita `ingestion/repository_adapter.py`, `database/repository/repository.py` e `analysis/data_analyzer.py`.

```
api/
├── main.py                      # app FastAPI + endpoint /health
├── config.py                    # catálogo de tabelas gold expostas
├── schemas.py                   # contratos Pydantic de entrada/saída
├── routers/
│   ├── gold.py                  # /gold/*
│   └── previsao.py              # /previsao/*
└── services/
    ├── gold_service.py          # paginação, filtros, resumo estatístico
    └── forecast_service.py      # treina/serve o modelo Prophet + XGBoost
```

**Endpoints principais:**

| Método | Rota | Descrição |
|---|---|---|
| GET | `/health` | Status da API e da conexão com o Postgres |
| GET | `/gold/tabelas` | Catálogo de tabelas gold conhecidas (e se já existem no banco) |
| GET | `/gold/{tabela}/resumo` | Estatísticas descritivas (linhas, colunas, nulos) |
| GET | `/gold/{tabela}/dados` | Registros paginados, com filtros `ano_min`, `ano_max`, `regiao_administrativa` |
| GET | `/previsao/crimes-contra-mulher` | Previsão híbrida Prophet+XGBoost (`horizonte_anos`, `usar_cache`, `persistir_modelo`) |
| GET | `/previsao/modelos` | Lista os modelos já persistidos em `models/*_meta.json` |

**Decisão de design importante:** os artefatos em `models/*.pkl` guardam apenas o regressor XGBoost do resíduo — o `Prophet` correspondente não é serializado (ponto de atenção já registrado abaixo). Por isso, `GET /previsao/crimes-contra-mulher` re-treina o par Prophet+XGBoost sob demanda a partir da tabela `violencia_contra_mulher_gold` mais recente, com um cache em memória de 30 min (`usar_cache=false` força retreino; `persistir_modelo=true` salva o artefato como o pipeline batch faria). Isso mantém a API sempre consistente com os dados mais recentes, ao custo de uma primeira chamada mais lenta — um passo natural de evolução seria persistir também o Prophet e servir por padrão a partir do artefato, com um endpoint separado de retrain explícito.

Testes: `tests/api/` (31 testes, cobrindo services e endpoints via `TestClient`, com mocks do banco/modelo — não requer Postgres nem treinar o modelo de verdade).

## 📌 Observações e Pontos de Atenção (herdados da análise técnica do projeto)

- ✅ **Padronização de RA consolidada:** as variantes de nome de Região Administrativa (ex.: `SUDOESTE` → `SUDOESTE/OCTOGONAL`) antes eram tratadas por chamadas pontuais e duplicadas de `renomear_linha` em `domain/violencia_mulher.py` e `domain/identificacao_crimes.py`, mais um `.replace({...})` inline e independente em `ViolenciaMulherService.carregar_feminicidio`. Agora existe um único mapeamento mestre (`util.padronizacao.MAPEAMENTO_REGIOES_ADMINISTRATIVAS`, aplicado via `renomear_regioes_conhecidas`), usado pelos três pontos — qualquer nova variante encontrada no futuro deve ser adicionada só ali. Cobertura de teste nova em `tests/util/test_padronizacao.py` e `tests/domain/` (que antes não existiam).
- **Full Refresh:** toda carga no Postgres recria a tabela (`if_exists="replace"`); não há carga incremental.
- **Maturidade desigual entre pipelines:** o pipeline Silver (`pipeline_busca_transformacao.py`) é procedural e sequencial; o pipeline Gold (`pipeline_tabela_gold.py`) já usa o padrão declarativo `PipelineStep` + executor paralelo — seria interessante levar o Silver para o mesmo modelo.
- **`src/main.py` executa as três etapas:** coleta/transformação, tabela gold e modelagem rodam em sequência por padrão — todo o fluxo tem cobertura de teste (incluindo o bloco `if __name__ == "__main__":`, coberto via `runpy`).
- ✅ **Metadados de modelo padronizados:** todos os artefatos em `models/` (incluindo os `xgb_residual_log_*`) já geram `_meta.json` via `save_model_with_metadata` (métricas, hiperparâmetros, features, dataset_info).
- ✅ **`requirements.txt` curado:** ver nota na seção "Como Executar" — já está separado de `requirements-dev.txt`.
- **Cobertura de teste não cobre todo o código:** `pytest.ini` mede cobertura apenas de `src`, `util` e `database` (`--cov=src --cov=util --cov=database`). Os pacotes `domain/`, `processing/`, `ingestion/`, `validation/` e a nova `api/` não entram nesse cálculo — `api/` tem sua própria suíde dedicada com 99% de cobertura medida à parte (ver seção da API), mas `domain/`/`processing/`/`ingestion/`/`validation/` não tinham nenhum teste até esta revisão, que adicionou uma cobertura inicial de regressão em `tests/domain/` para os dois serviços afetados pelo item de padronização de RA acima. Ampliar `--cov` para esses pacotes (e sua cobertura de teste) é candidato a um próximo passo.

## 🗺️ Roadmap / Visão Futura

Os itens abaixo **não existem no código atual** — são direções possíveis de evolução, registradas para não se perderem, mas não devem ser confundidas com o estado presente do projeto:

### Arquitetura e dados
- Camada geoespacial com PostGIS e malha de células (grid) para análises espaciais mais finas.
- Carga incremental (hoje é sempre full refresh).
- Cloud readiness: abstrair sistema de arquivos para suportar object storage (S3/Blob).
- Data quality automatizado entre camadas (schema checks).
- Orquestração unificada (ex.: levar o pipeline Silver para o padrão `PipelineStep`, ou adotar Airflow/Prefect).

### Modelagem e análise (baseado nas propostas de enriquecimento já levantadas para o notebook exploratório)
- Variáveis exógenas: projeção populacional por RA, índices socioeconômicos (IDH/renda), sazonalidade mensal/calendário de eventos.
- Análise de correlação multivariada entre tipos de crime (ex.: Causalidade de Granger) e entre tabelas gold (ex.: violência contra idosos x crimes patrimoniais).
- Visualização geoespacial (mapas de calor com Folium/GeoPandas) para identificar zonas quentes de criminalidade.
- Detecção de outliers/anomalias (ex.: Isolation Forest) para identificar mudanças de padrão ou metodologia.
- Otimização de hiperparâmetros (Optuna/GridSearchCV adaptado a séries temporais).
- Exportação de relatório executivo (PDF/Markdown) com os principais insights de cada previsão.

### Camada de consumo
- ✅ **API (FastAPI) implementada** — ver seção "🌐 Camada de Consumo (API)" acima.
- Dashboard interativo (Streamlit/Plotly) consumindo essa API, para mapas, séries temporais e aba de previsões.
- Autenticação/API key e rate limiting, caso a API passe a ser exposta publicamente.
- Persistir o modelo Prophet junto ao XGBoost em `models/`, permitindo que `/previsao` sirva a partir do artefato salvo por padrão, com endpoint de retrain explícito separado.
