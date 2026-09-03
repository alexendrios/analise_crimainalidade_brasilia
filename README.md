# Análise de Criminalidade do Distrito Federal

![License](https://img.shields.io/badge/license-MIT-lightgrey)
![Java](https://img.shields.io/badge/Java-21-orange)
![Maven](https://img.shields.io/badge/Apache%20Maven-3.9.11-C71A36?logo=apachemaven&logoColor=white)
![Node.js](https://img.shields.io/badge/Node.js-22-339933?logo=node.js&logoColor=white)
![npm](https://img.shields.io/badge/npm-Package%20Manager-CB3837?logo=npm&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.13.0-3776AB?logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.x-150458?logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/NumPy-2.x-013243?logo=numpy&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![PostGIS](https://img.shields.io/badge/PostGIS-3.x-5C8A8A?logo=postgresql&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.x-D71F00?logo=sqlalchemy&logoColor=white)
![psycopg2](https://img.shields.io/badge/psycopg2-PostgreSQL%20Adapter-336791?logo=postgresql&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115.x-009688?logo=fastapi&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.x-FF4B4B?logo=streamlit&logoColor=white)
![Folium](https://img.shields.io/badge/Folium-Maps-77B829?logo=python&logoColor=white)
![GeoPandas](https://img.shields.io/badge/GeoPandas-Geospatial-139C5A?logo=python&logoColor=white)
![Prophet](https://img.shields.io/badge/Prophet-Forecasting-6A5ACD?logo=python&logoColor=white)
![XGBoost](https://img.shields.io/badge/XGBoost-ML-EC6B23?logo=xgboost&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-ML-F7931E?logo=scikitlearn&logoColor=white)
![Joblib](https://img.shields.io/badge/Joblib-Parallel%20Computing-3670A0?logo=python&logoColor=white)
![Requests](https://img.shields.io/badge/Requests-HTTP-3776AB?logo=python&logoColor=white)
![BeautifulSoup](https://img.shields.io/badge/BeautifulSoup-Web%20Scraping-3776AB?logo=python&logoColor=white)
![PyYAML](https://img.shields.io/badge/PyYAML-YAML-3776AB?logo=python&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Containerization-2496ED?logo=docker&logoColor=white)
![Pytest](https://img.shields.io/badge/Pytest-Testing-0A9EDC?logo=pytest&logoColor=white)
![pytest-xdist](https://img.shields.io/badge/pytest--xdist-Parallel%20Tests-0A9EDC?logo=pytest&logoColor=white)
![pytest-cov](https://img.shields.io/badge/pytest--cov-Coverage-0A9EDC?logo=pytest&logoColor=white)
![pytest-html](https://img.shields.io/badge/pytest--html-HTML%20Reports-0A9EDC?logo=pytest&logoColor=white)
![Testcontainers](https://img.shields.io/badge/Testcontainers-Integration%20Testing-4DABCF?logo=docker&logoColor=white)
![Karate DSL](https://img.shields.io/badge/Karate%20DSL-API%20Testing-29BEB0)
![Gatling](https://img.shields.io/badge/Gatling-Load%20Testing-FF9E2C?logo=gatling&logoColor=white)
![CodeceptJS](https://img.shields.io/badge/CodeceptJS-E2E-8B5CF6?logo=javascript&logoColor=white)
![Playwright](https://img.shields.io/badge/Playwright-E2E-2EAD33?logo=playwright&logoColor=white)
![Cucumber](https://img.shields.io/badge/Cucumber-BDD-23D96C?logo=cucumber&logoColor=white)
![Allure](https://img.shields.io/badge/Allure-Test%20Reports-FF6A00)
---

Projeto que coleta, padroniza e consolida séries históricas de criminalidade do **Distrito Federal** (dados abertos da SSP-DF e população do IBGE/Wikipédia), organizando tudo em um **data lakehouse em camadas (Bronze → Silver → Gold)** com banco PostgreSQL/PostGIS. A camada Gold alimenta um **modelo híbrido Prophet + XGBoost** de previsão de séries temporais (aplicado a crimes contra a mulher por RA), análises executivas (correlações, causalidade de Granger, anomalias e zonas quentes), uma **API REST (FastAPI)** e um **dashboard interativo (Streamlit + Plotly)** — tudo coberto por testes unitários, de integração (banco real via Testcontainers), E2E de API e UI e de carga.

## Arquitetura

![Pipeline de Dados](image.png)

![Arquitetura](image-1.png)

```
        ┌─────────────────────────────────────────────
        │            Fontes de Dados Externas         │
        │─────────────────────────────────────────────│
        │ - dados.df.gov.br (SSP-DF, .xlsx via CKAN)  │
        │ - ftp.ibge.gov.br (população por município) │
        │ - Wikipédia (população por RA)              │
        └──────────────────┬──────────────────────────┘
                           │
                           ▼
        ┌─────────────────────────────────────────────┐
        │        Camada de Coleta (src/busca.py,      │
        │        src/scraping.py, src/coleta_gdf.py)  │
        │─────────────────────────────────────────────│
        │ • download via requests + rotas declaradas  │
        │   (rotas.yaml / rotas_ibge.yaml / config)   │
        │ • extração de .zip (util/extrator_zip.py)   │
        │ • leitura de .xlsx → .csv                   │
        └──────────────────┬──────────────────────────┘
                           │
                           ▼
        ┌────────────────────────────────────────────┐
        │   BRONZE (data/bronze)                     │
        │   arquivos brutos, sem tratamento          │
        └──────────────────┬─────────────────────────┘
                           │
                           ▼
        ┌─────────────────────────────────────────────┐
        │   Camada de Tratamento (src/tratamento_*)   │
        │─────────────────────────────────────────────│
        │ • padronização de nomes de RA               │
        │   (util/padronizacao.py)                    │
        │ • conversão wide→long e normalização        │
        │ • validação estrutural (validation/)        │
        └──────────────────┬──────────────────────────┘
                           │
                           ▼
        ┌─────────────────────────────────────────────┐
        │   SILVER (data/silver/output)               │
        │   CSVs tratados e padronizados              │
        └──────────────────┬──────────────────────────┘
                           │
                           ▼
        ┌─────────────────────────────────────────────┐
        │   PostgreSQL 16 + PostGIS (SQLAlchemy)      │
        │─────────────────────────────────────────────│
        │ • imagem postgis/postgis:16-3.4             │
        │ • carga FULL REFRESH (to_sql replace)       │
        │ • acesso via Repository Pattern             │
        │   (ingestion/repository_adapter.py →        │
        │   database/repository/repository.py)        │
        └──────────────────┬──────────────────────────┘
                           │
                           ▼
        ┌─────────────────────────────────────────────┐
        │   GOLD (src/pipeline_tabela_gold.py)        │
        │─────────────────────────────────────────────│
        │ • PipelineStep + executor (ThreadPool→      │
        │   com retries e timeout)                    │
        │ • serviços de domínio (domain/*.py)         │
        │   consolidam as tabelas *_gold              │
        └──────────────────┬──────────────────────────┘
                           │
                           ▼
        ┌───────────────────────────────────────────────┐
        │   Camada de Consumo                           │
        │───────────────────────────────────────────────│
        │ • API REST FastAPI (api/)                     │
        │ • Dashboard Streamlit + Plotly (dashboard/)   │
        │ • Análises executivas (analysis/)             │
        └───────────────────────────────────────────────┘
```

## Componentes e Tecnologias

| Camada | Tecnologias | Responsabilidade |
|:---|:---|:---|
| Coleta | `requests`, `pandas`, `beautifulsoup4`, `util/extrator_zip.py`, `util/leitor_excel.py` | Baixar planilhas SSP-DF, extrair ZIP, raspar população por RA |
| Configuração de fontes | `rotas.yaml`, `rotas_ibge.yaml`, `config.yaml`, `config/datasets_config.py`, `util/config_loader.py` | Catálogo declarativo de datasets/URLs (sem hardcode) |
| ETL (Bronze → Silver) | `pandas`, `src/tratamento_crimes.py`, `src/tratamento_populacional.py`, `src/pipeline_busca_transformacao.py` | Limpeza, padronização, wide→long; orquestração com `PipelineStep`s paralelos |
| Banco de Dados | PostgreSQL 16 + **PostGIS** (`postgis/postgis:16-3.4`), `SQLAlchemy`, `psycopg2` | Persistência relacional + espacial (malha de células, DDL opcional em `geoespacial/postgis.py`) |
| Camada Gold | `domain/*.py` + `src/core/executor.py` (paralelismo, retry, timeout) | Consolidar, validar e gravar as tabelas `*_gold` |
| Modelagem | `scikit-learn`, `XGBoost`, `Prophet`, `joblib`, `statsmodels` | Modelo híbrido Prophet + resíduo XGBoost; regressão logística; correlação/Granger/anomalias |
| Análise geoespacial | `pandas`, `numpy`, `GeoPandas`, `Folium` | Malha regular de células, mapa de calor, export GeoPackage |
| API | `FastAPI`, `uvicorn`, `Pydantic`, `httpx` | Exposição das tabelas gold, previsão, classificação, análises e qualidade |
| Dashboard | `Streamlit`, `Plotly` | Painel interativo com 13 abas (tema dark) |
| Testes | `pytest` + `pytest-cov` + `pytest-html` + `pytest-xdist` + `testcontainers` | Suíte de 3.681 itens com cobertura ≥ 95% (ver seção Qualidade) |
| E2E / Carga / UI | `Karate DSL` (Cucumber/Allure), `Gatling` (Scala/Maven), `CodeceptJS` + `Playwright` (Cucumber/Allure) | E2E da API, carga/performance e E2E de UI do dashboard |
| Infra | `Docker Compose` (PostGIS + pgAdmin), `.env` | Ambiente local reprodutível |

## Pipeline de Dados (Bronze → Silver → Gold)

O fluxo é orquestrado por `src/main.py`, que executa as três fases em sequência: `busca_transformacao_dados()` (coleta + tratamento), `criar_tabela_gold(max_workers=6)` (camada Gold) e `executar_pipeline()` (modelagem).

1. **Coleta → Bronze:** `src/busca.py`, `src/scraping.py` e `util/extrator_zip.py` baixam as planilhas originais para `data/bronze`. As fontes são declaradas em `rotas.yaml` (**18 rotas de criminalidade**: 17 do CKAN de `dados.df.gov.br` + URL direta do feminicídio da SSP) e `rotas_ibge.yaml` (**população anual/IBGE**), carregadas por `util/config_loader.py` (com override por `.env`).
2. **Bronze → Silver:** `src/pipeline_busca_transformacao.py` preserva em sequência as fases com dependência (coleta → população → planilhas → carga) e executa os tratamentos independentes em paralelo (via `PipelineStep` + `executar_pipeline`, com retry e timeout), gerando CSVs padronizados em `data/silver/output`.
3. **Silver → Postgres → Gold:** `database/load_csvs.py` carrega os CSVs tratados; `src/pipeline_tabela_gold.py` executa os `PipelineStep`s (paralelo), cada um chamando um serviço de domínio (`domain/*.py`) que lê tabelas via `Repository.load`, aplica regras de negócio (padronização de RAs via `util/padronizacao.MAPEAMENTO_REGIOES_ADMINISTRATIVAS`, merges com validação de chaves) e grava com `Repository.save` (full refresh, idempotente).
4. **Validação e qualidade:** schemas declarativos silver/gold em `validation/schema.py` e `validation/esquemas.py`, aplicados automaticamente pelo hook `PipelineStep.validacao`; Data Quality Score em `validation/qualidade_dados.py`.

## Tabelas Gold

| Tabela | Serviço |
|:---|:---|
| `violencia_contra_mulher_gold` | `ViolenciaMulherService.consolidar` |
| `identificacao_crimes_contra_mulher_gold` | `IdentificacaoCrimesService.carregar` |
| `violencia_idosos_gold` / `_ocorrencias_gold` / `_mensais_gold` / `_sexo_gold` | `ViolenciaIdososService` |
| `crimes_roubo_furto_gold` | `CrimesPatrimoniaisService.consolidar` |
| `crimes_letais_gold` | `CrimesLetaisService.consolidar` |
| `crimes_discriminatorios_gold` | `CrimesDiscriminatoriosService.consolidar` |
| `desaparecidos_idade_sexo_gold` / `_localizados_gold` / `_regiao_gold` | `DesaparecimentosService` |

## Modelagem Preditiva (`analysis/data_analyzer.py`)

- **Alvo:** `crimes_contra_mulher` — contagem anual por RA (consultada via `violencia_contra_mulher_gold`).
- **Features (9):** `lag_1`, `lag_2`, `rolling_mean_2`, `rolling_mean_3`, `taxa_feminicidio`, `feminicidio_lag_1`, `trend`, `ano_num`, `diff_1` (versão de features `VERSAO_FEATURES=2`).
- **Abordagem híbrida:** Prophet modela a tendência anual; um `XGBRegressor` aprende o resíduo em escala logarítmica (`log1p`) entre o valor real e o previsto pelo Prophet.
- **Pós-processamento:** clipping dinâmico do resíduo pelos percentis 5%/95%, decaimento de 15% ao ano (mínimo de 40% do efeito) e suavização exponencial (0,7/0,3) entre anos consecutivos. Reconstrução recursiva das features a cada passo.
- **Horizonte:** padrão de 5 anos pela API `GET /previsao/crimes-contra-mulher` (`horizonte_anos` de 1 a 10).
- **Persistência:** cada execução gera um **bundle** `models/xgb_residual_log_<timestamp>.pkl` — dict `{xgb_model, prophet_model}` serializado com `joblib` via `save_model_with_metadata` — acompanhado de `_meta.json` (métricas MAE/RMSE, hiperparâmetros, features, `dataset_info`, `artifact_format: "bundle"`, limites de clipping e horizonte). A API serve a previsão a partir do artefato sem re-treinar.

## Análises Executivas (`analysis/`)

Execução única via `python -m analysis.pipeline_analise`, com saída em `data/analises/` (`relatorio_executivo.md/.html` + `mapa_calor_roubo_pedestre.html`):

| Módulo | O que faz |
|:---|:---|
| `analysis/correlacoes.py` | Matriz ano × indicador (12 crimes), correlações Pearson/Spearman, causalidade de **Granger** (salvaguardas p/ séries curtas) e correlação espacial entre tabelas gold |
| `analysis/anomalias.py` | **Isolation Forest** sobre features causais (lag, diferença, média móvel) na série mensal de idosos e no painel RA × ano patrimoniais |
| `analysis/mapa.py` | **Mapa de calor Folium** sobre a malha de células do `geoespacial.malha` + export GeoPackage |
| `analysis/relatorio.py` | Relatório executivo Markdown + **HTML autocontido** (imprimível em PDF) |
| `analysis/logistic_regression.py` | Regressão Logística de criminalidade letal por RA/ano (alta vs baixa) |

Resultados típicos: correlação forte entre patrimoniais e letais; Granger aponta `roubo_comercio` antecedendo roubo no transporte/pedestre/veículo; Spearman **+0,63 (p=0,001)** entre violência contra idosos e patrimoniais por RA (2016); anomalias concentradas em 2020.

## Camada de Consumo — API REST (`api/`)

API **FastAPI v1.1.0** que reaproveita `ingestion/repository_adapter.py`, `database/repository/repository.py` e `analysis/data_analyzer.py`:

```
api/
├── main.py                      # app FastAPI + endpoint /health
├── config.py                    # catálogo declarativo das tabelas gold expostas
├── schemas.py                   # contratos Pydantic
├── routers/
│   ├── gold.py                  # /gold/*
│   ├── previsao.py              # /previsao/*
│   ├── classificacao.py         # /classificacao/*
│   ├── analise.py               # /analise/*
│   └── qualidade.py             # /qualidade/*
└── services/
    ├── gold_service.py          # paginação, filtros, resumo estatístico
    ├── forecast_service.py      # serve/treina o par Prophet + XGBoost
    ├── classificacao_service.py # Regressão Logística (cache + artefato)
    ├── analise_service.py       # correlações, Granger, anomalias, zonas quentes
    └── qualidade_service.py     # Data Quality Score (cache TTL 300s)
```

### Endpoints principais

| Método | Rota | Descrição |
|---|---|---|
| GET | `/health` | Status da API e da conexão com o Postgres |
| GET | `/gold/tabelas` | Catálogo das tabelas gold conhecidas (e se existem no banco) |
| GET | `/gold/{tabela}/resumo` | Estatísticas descritivas (linhas, colunas, nulos) |
| GET | `/gold/{tabela}/dados` | Registros paginados com filtros `ano_min`, `ano_max`, `regiao_administrativa` |
| GET | `/previsao/crimes-contra-mulher` | Previsão híbrida a partir do artefato persistido (`horizonte_anos`, `usar_cache`, `persistir_modelo`) |
| POST | `/previsao/retrain` | Force novo treino Prophet+XGBoost e persiste o bundle |
| GET | `/previsao/modelos` | Lista os modelos em `models/*_meta.json` (`formato_artefato`: bundle/legacy) |
| GET | `/classificacao/criminalidade-letal` | Classificação alta/baixa por RA/ano (probabilidade, odds ratios, matriz, CV ROC-AUC) |
| POST | `/classificacao/retrain` | Re-treina e persiste o artefato de classificação |
| GET | `/analise/correlacoes` | Matriz de correlação, pares destaque e insights (`metodo`, `top_n`) |
| GET | `/analise/granger` | Causalidade de Granger pairwise (`max_lag`, `apenas_significantes`, `limite`) |
| GET | `/analise/anomalias` | Anomalias Isolation Forest (painel patrimoniais + série idosos) |
| GET | `/analise/zonas-quentes` | Células da malha com mais ocorrências no último ano (`tamanho_celula_km`, `top_n`) |
| GET | `/qualidade/dados` | Data Quality Score (0–100) do catálogo gold por tabela |

- Endpoints `/analise/*` calculam sobre as tabelas gold com **cache em memória de 30 min** (treino do Isolation Forest por RA custa ~10 s) e retornam **503** quando as tabelas necessárias não estão materializadas.
- **Serving do bundle:** `GET /previsao/crimes-contra-mulher` primeiro procura o bundle mais recente (`localizar_ultimo_modelo_bundle`) e serve direto do artefato (`fonte_modelo: "artefato"`); sem bundle utilizável, treina sob demanda (`fonte_modelo: "retreino"`). `POST /previsao/retrain` ignora cache e artefato e sempre persiste.
- Artefatos antigos (`artifact_format: "legacy"`, apenas XGBoost) continuam legíveis por `carregar_modelo`, mas não servem previsão (sem Prophet).

## Dashboard Interativo (`dashboard/`)

Painel **Streamlit** com tema dark (`.streamlit/config.toml`) que consome a API e plota com **Plotly** (`plotly_dark`). **13 abas**: **Visão Geral**, **Séries Temporais**, **Mapa de Calor**, **Mancha Criminal**, **Identificação crimes**, **Desaparecidos**, **Violência contra idosos**, **Previsões**, **Classificação**, **Análises**, **Resumo Geral**, **Tabelas** e **Qualidade dos Dados**.

- **Visão Geral:** métricas-resumo do indicador (valor no último ano, variação vs ano anterior, período, RA crítica). Carga cacheada 10 min. As colunas numéricas são **comparadas ao schema gold** de `validation/esquemas.py`: se nenhum indicador esperado existir (tabela degenerada/sobrescrita), a aba exibe aviso com o comando de reconstrução `python -m src.pipeline_tabela_gold`.
- **Séries Temporais:** total anual consolidado + RAs selecionáveis + média móvel; rótulos pt-BR.
- **Mapa de Calor:** heatmap RA × ano com escala YlOrRd e ranking horizontal.
- **Mancha Criminal:** mapa com DensityMap/ScatterMap consumindo `/analise/zonas-quentes`.
- **Desaparecidos:** grade 2×2 (sexo, faixa etária, localizados × desaparecidos, por RA).
- **Violência contra idosos:** sexo, faixa etária, ocorrências por mês e evolução anual.
- **Análises:** Correlações, Granger, Anomalias e Zonas Quentes (endpoints `/analise/*`).
- **Qualidade dos Dados:** `GET /qualidade/dados` — score geral, nota média por dimensão, detalhe por tabela.
- **Resumo Geral:** síntese executiva via **Ollama** (modelo local, fallback amigável) com contexto de 8 seções (`contexto_ia.py`).
- **Previsões / Classificação / Tabelas:** consumo direto dos endpoints correspondentes (ranking de RAs, probabilidades, exploração paginada).

```
dashboard/
├── app.py              # interface Streamlit (13 abas)
├── api_client.py       # cliente HTTP da API (requests)
├── ia_client.py        # cliente HTTP do Ollama
├── contexto_ia.py      # construtor de contexto para o Ollama (8 seções)
└── visualizacoes.py    # transformações pandas + figuras Plotly + rótulos pt-BR (funções puras)
```

## Qualidade e Testes

**Suíte: 3.681 itens coletados, todos passando** — **97,66% de cobertura** sobre `analysis`, `api`, `config`, `dashboard`, `database`, `domain`, `geoespacial`, `ingestion`, `processing`, `src`, `util` e `validation` (limiar mínimo: `--cov-fail-under=95`).

**Pirâmide de testes:**
- **2.897 itens de integração (78,7%)** — Postgres/PostGIS real via **Testcontainers** (`tests/integracao/`). Composição: 2.585 itens parametrizados da malha geoespacial (1 caso por célula), 105 repositório, 108 API gold grade, 36 pipeline gold grade, 21 repositório, 19 API gold, 16 geoespacial, 7 pipeline tabela gold.
- **784 itens não-integração:** **674 unitários** + **52 de endpoints via TestClient** (28 `test_api_main`, 18 `test_analise_service`, 6 `test_qualidade_service`) + **58 de UI via AppTest**.
  - `tests/api/` = 95 itens (services + endpoints via TestClient, com mocks de banco/modelo — sem Postgres nem treino real).
  - `tests/dashboard/` = 185 itens (58 AppTest em 5 arquivos + 96 `visualizacoes` + 20 `api_client` + 9 `ia_client` + 2 `contexto_ia`).

**Execução:**

```bash
pytest -m "not integracao" -n auto --dist=loadfile  # 784 não-integração (paralela, sem Docker)
pytest tests/integracao -n 0                        # 2.897 integração (~4 min, Testcontainers)
pytest --cov                                        # com cobertura (gate --cov-fail-under=95)
scripts\gerar-relatorios.bat                        # fluxo completo + relatórios (Windows)
```

- O `addopts` do `pytest.ini` **não força paralelismo**: um `pytest` simples roda em série (evita levantar N containers PostGIS); paralelismo fica explícito nos scripts, apenas na camada não-integração.
- Relatórios em `test_report/`: `relatorio-testes.html`, `junit.xml`, `coverage/index.html`, `coverage.xml`, `cobertura-executiva.html` (executivo, por `scripts/gerar_relatorio_cobertura.py`); log em `logs/testes.log`.

**Data Quality Score (0–100):** `validation/qualidade_dados.py` calcula por tabela gold uma nota com **6 dimensões ponderadas** — Completude (25%), Unicidade (20%), Validade de schema (20%), Consistência (20%), Atualidade (10%, frescor do `inserido_em`, janela 30–365 dias decrescente) e Cobertura temporal (5%, 2015–2024). Dimensões não aplicáveis são excluídas e os pesos redistribuídos; tabelas não materializadas entram com nota 0. Consistência valida RAs contra o domínio canônico (`util.padronizacao`), anos no período e contagens não negativas. Exposto em `GET /qualidade/dados` (cache TTL 300s, 503 se indisponível) e na aba **Qualidade dos Dados**.

### Suítes externas de API e UI

| Suíte | Stack | Cobertura |
|:---|:---|:---|
| **E2E da API** (`karate-tests/`) | Karate DSL + Cucumber + Allure | 15 features; **85 `Scenario` + 1 `Scenario Outline` (6 linhas de `Examples`) = 91 execuções no total**; fluxo padrão executa **89** (os 2 cenários `@retreino` — `POST /previsao/retrain` e `/classificacao/retrain` — ficam fora; roda com `-Dkarate.options=--tags @retreino`) |
| **Carga da API** (`gatling-tests/`) | Gatling (Scala/Maven) | 2 simulações: `SmokeSimulation` (1 execução/endpoint) e `ApiCargaSimulation` (rampa leitura 1→5 usuários/s + análises 0,2→1 usuários/s, 30s); falha o build se sucesso global < 99% ou p95 acima de 4000 ms (leitura) / 10000 ms (análises) — ajustados no commit `99a0823` |
| **E2E de UI** (`e2e-tests/`) | CodeceptJS + Playwright + Cucumber + Allure + POM | **115 cenários** em 15 features (visão geral, abas, widgets, interações e conteúdo por aba, com asserções de valores reais via API) |

## Como Executar (ambiente local)

```bash
# 1. Rodar a aplicação via Docker
docker compose up -d

# 2. Configurar credenciais em .env (ver .env.example)

#  Executar Local
docker compose up -d postgres pgadmin ollama
python -m venv venv
venv\Scripts\activate          # Windows  |  source venv/bin/activate (Linux/macOS)
pip install -r requirements.txt

# 4. Pipeline completo (coleta + gold + modelagem)
python -m src.main

# 5. Análises executivas
python -m analysis.pipeline_analise
# Saída em data/analises/: relatorio_executivo.md/.html + mapa_calor_roubo_pedestre.html

# 6. Testes
pytest -m "not integracao" -n auto    # não-integração (sem Docker)
pytest tests/integracao -n 0          # integração via Testcontainers (requer Docker, ~4 min)
pytest --cov                          # cobertura (gate 95%)
scripts\gerar-relatorios.bat           # fluxo completo + relatórios (Windows)

# 7. API (documentação em http://localhost:8000/docs)
uvicorn api.main:app --reload --port 8000

# 8. Dashboard
streamlit run dashboard/app.py        # http://localhost:8501 (URL da API configurável na sidebar)

# 9. Suítes externas (requerem API e/ou dashboard no ar)
cd karate-tests   && mvn test          # E2E da API
cd ../gatling-tests && mvn gatling:test # carga (ou -Dgatling.simulationClass=...SmokeSimulation)
cd ../e2e-tests   && npm install && npx playwright install chromium && npm run test:all
```

## Estrutura de Diretórios

| Diretório | Conteúdo |
|:---|:---|
| `src/` | Coleta, scraping, tratamento (crimes/população), orquestração (`main.py`, `pipeline_*`, `core/`) |
| `domain/` | Serviços de domínio por tema (mulher, idosos, letais, patrimoniais, discriminatórios, desaparecidos) |
| `database/` | Conexão SQLAlchemy, repositório do Postgres, carga de CSVs |
| `ingestion/` | Adaptador (`Repository`) entre domínio e `database/repository` |
| `geoespacial/` | Malha regular de células 1 km, centróides das RAs e DDL PostGIS opcional |
| `processing/` | Transformações genéricas e pós-processamento |
| `validation/` | Schemas (`schema.py`, `esquemas.py`) e Data Quality Score (`qualidade_dados.py`) |
| `analysis/` | Modelagem e análises executivas (Prophet+XGBoost, correlações, Granger, anomalias, mapa, relatório) |
| `api/` | API REST FastAPI (routers + services) |
| `dashboard/` | Painel Streamlit/Plotly (13 abas, cliente de API e IA, visualizações) |
| `util/` | Utilitários (Excel, ZIP, logging, padronização de RAs, loader de configuração) |
| `config/` | Configuração de datasets (`datasets_config.py`) |
| `models/` | Artefatos treinados (bundles `.pkl` + `_meta.json`) |
| `data/` | Camadas `bronze/`, `silver/` do lakehouse local (gerada em runtime; ignorada pelo Git) |
| `tests/` | Suíte pytest: unitários, API (TestClient), dashboard (AppTest) e `integracao/` (Testcontainers) |
| `karate-tests/` | E2E da API (Karate DSL + Cucumber + Allure) |
| `gatling-tests/` | Carga/performance da API (Gatling, Scala/Maven) |
| `e2e-tests/` | E2E de UI do dashboard (CodeceptJS + Playwright + Cucumber + Allure + POM) |
| `scripts/` | `executar_testes.bat`, `testar-com-coverage.bat`, `executar_testes.ps1`, `gerar_relatorio_cobertura.py` |
| `docker-compose.yaml` | PostgreSQL 16 + PostGIS + pgAdmin para ambiente local |
| `requirements.txt` / `requirements-dev.txt` | Dependências de runtime e de desenvolvimento (pytest, xdist, cov, html, testcontainers) |

## Observações relevantes

- **Padronização de RA centralizada:** variantes de nomes (`SUDOESTE` → `SUDOESTE/OCTOGONAL`) tratadas por um único mapeamento mestre em `util.padronizacao.MAPEAMENTO_REGIOES_ADMINISTRATIVAS`, aplicado via `renomear_regioes_conhecidas` (`domain/violencia_mulher.py`, `domain/identificacao_crimes.py` e `ViolenciaMulherService.carregar_feminicidio`). Novas variantes devem ser adicionadas só ali.
- **Maturidade igual entre pipelines:** o pipeline Silver usa o mesmo motor declarativo `PipelineStep` + `executar_pipeline` (paralelismo, retry, timeout, agendamento topológico e detecção de ciclo) do Gold.
- **Metadados padronizados:** todos os artefatos em `models/` geram `_meta.json` via `save_model_with_metadata`.
- **Dependências curadas:** `requirements.txt` (runtime) separado de `requirements-dev.txt` (testes); `.env` não é rastreado pelo Git.
- **Guard de schema no dashboard:** a Visão Geral compara as colunas carregadas ao schema gold de `validation/esquemas.py`; se a tabela foi sobrescrita com colunas genéricas (ex.: gold degenerado), exibe aviso com o comando de reconstrução (`python -m src.pipeline_tabela_gold`) em vez de exibir métricas incorretas — coberto por teste de regressão.
- **Carga de carga da API:** os defaults do Gatling foram reduzidos (commit `99a0823`) porque os originais derrubavam a API local sob alta concorrência; parâmetros sobrecarregáveis via `-Dcarga.*`.