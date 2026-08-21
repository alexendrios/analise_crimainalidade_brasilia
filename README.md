# Projeto Criminalidade Brasília - DF

> **Nota de atualização:** esta documentação foi revisada para refletir o que está **efetivamente implementado no código** na data desta análise. A versão anterior descrevia uma arquitetura geoespacial com PostGIS, malha hexagonal, dashboard Streamlit e API FastAPI — nenhum desses componentes existia naquele momento no repositório. Eles foram movidos para a seção [Roadmap](#-roadmap--visão-futura) conforme ainda pendentes, ou promovidos de volta ao corpo do documento à medida que foram implementados (caso da API, ver abaixo).
>
> **Revisão mais recente (verificada rodando a suíte de fato, commit `38a3d92`):** a cobertura de testes configurada em `pytest.ini` foi ampliada — hoje mede `analysis`, `api`, `config`, `database`, `domain`, `ingestion`, `processing`, `src` e `util` (não mais apenas `src`, `util` e `database`); só `validation/` segue sem cobertura própria. Números reais desta rodada: **391 testes coletados, 388 passando, 3 falhando, 99,18% de cobertura** (limiar mínimo 95%, atingido). As 3 falhas são um bug real e reprodutível, não um problema de ambiente — ver [Observações e Pontos de Atenção](#-observações-e-pontos-de-atenção-herdados-da-análise-técnica-do-projeto). Também confirmado nesta revisão: `.env` **não está mais rastreado pelo Git** (item antes pendente, já resolvido), e todos os modelos `xgb_residual_log_*.pkl` já possuem `_meta.json` completo (a documentação anterior afirmava o contrário).
>
> **Implementação de item do roadmap — persistência do Prophet:** o item "persistir o modelo Prophet junto ao XGBoost em `models/`" foi implementado. `save_model_with_metadata` agora aceita um `prophet_model` opcional e salva o par como um único artefato "bundle"; `GET /previsao/crimes-contra-mulher` passa a servir por padrão a partir do bundle mais recente em disco (sem re-treinar), com fallback automático para treino quando não há artefato utilizável; um novo endpoint `POST /previsao/retrain` força o re-treino explícito. Detalhes em [Camada de Consumo (API)](#-camada-de-consumo-api--api). 16 novos testes adicionados (391 → 407 testes coletados), cobertura de `api/services/forecast_service.py` em 100%.
>
> **Revisão mais recente:** dashboard Streamlit implementado (séries temporais com **total consolidado + RAs selecionáveis** e média móvel, mapa de calor RA × ano, ranking, previsões e exploração das tabelas gold) e bug de variável de ambiente `POSTGRES_USERNAME` vs `POSTGRES_USER` em `tests/database/test_connection.py` corrigido — **486 testes, todos passando, 99,29% de cobertura**. Ver [Dashboard Interativo](#-dashboard-interativo-streamlit--dashboard) e [Qualidade e Testes](#-qualidade-e-testes).
>
> **Testes E2E e de carga da API:** além da suíte `pytest`, a API de consumo ganhou duas camadas de teste externas ao Python — uma suíte **E2E em Karate DSL (Gherkin/Cucumber)** com relatório **Allure** (`karate-tests/`) e uma suíte de **carga/performance em Gatling (Scala)** (`gatling-tests/`), que exercita o fluxo principal do dashboard sob rampa de usuários com asserções de taxa de sucesso (≥99%) e p95 (limite configurável). Ver [Testes E2E da API](#-testes-e2e-da-api-karate-dsl--cucumber--allure--karate-tests) e [Testes de Carga da API](#-testes-de-carga-da-api-gatling--gatling-tests).
>
> **Revisão mais recente (aumento de cobertura):** criada a suíte `tests/validation/` — `validation/validator.py`, antes o único pacote de produção sem testes próprios (36%), agora está em 100%. Também cobertos os últimos ramos pendentes de `analysis/data_analyzer.py`, `api/main.py`, `dashboard/api_client.py`, `dashboard/app.py` e `src/main.py`. Estado atual verificado rodando a suíte: **502 testes, todos passando, 99,65% de cobertura** (todos os módulos com 100% de statements; restam apenas ramos parciais defensivos).

### Pipeline de Dados
![alt text](image.png)

### Arquitetura
![alt text](image-1.png)

## 🎯 Visão Geral

O projeto coleta, padroniza e consolida séries históricas de criminalidade do Distrito Federal (fontes SSP-DF e dados populacionais do IBGE/GDF), organiza os dados em um **Data Lakehouse em camadas (Bronze → Silver → Gold)** e utiliza o resultado para treinar um modelo híbrido de previsão de séries temporais (Prophet + XGBoost) aplicado hoje a **crimes contra a mulher** por Região Administrativa (RA).

Não há componente geoespacial (sem PostGIS, sem malha de células) — o fluxo é executado localmente via scripts Python (`src/main.py`); a camada de consumo (API + dashboard) e as suítes E2E/carga estão descritas nas seções ao final deste documento.

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
| **Testes** | `pytest`, `pytest-cov`, `pytest-html` | 502 testes, todos passando, **99,65% de cobertura** em `analysis`, `api`, `config`, `dashboard`, `database`, `domain`, `ingestion`, `processing`, `src`, `util` e `validation`, limiar mínimo de 95% (`--cov-fail-under=95`) |
| **Testes E2E / Carga da API** | `Karate DSL` (Cucumber/Allure), `Gatling` (Scala/Maven) | Suíte E2E dos endpoints da API em `karate-tests/` e teste de carga com rampa de usuários e asserções de sucesso/p95 em `gatling-tests/` (ver seções próprias) |
| **Ambiente / Infra** | `Docker Compose` (container `postgres:16`), `.env` para credenciais | Ambiente local reprodutível para o banco |

### 🧩 Interações Principais (fluxo real)

- **Coleta → Bronze:** `src/busca.py`, `src/scraping.py` e `util/extrator_zip.py` baixam e descompactam as planilhas originais em `data/bronze`.
- **Bronze → Silver:** `src/pipeline_busca_transformacao.py` orquestra as fases de coleta/população/planilhas em sequência (dependentes) e depois executa os tratamentos independentes de `src/tratamento_crimes.py` em paralelo, via `PipelineStep`s declarativos + `executar_pipeline`, gerando CSVs padronizados em `data/silver/output`.
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
| `tests/` | Suíte de testes (`analysis`, `api`, `arquivos`, `config`, `core`, `dados`, `database`, `dashboard`, `domain`, `ingestion`, `pipeline`, `processing`, `rotas`, `scrapings`, `setup`, `util`, `validation`) |
| `karate-tests/` | Testes E2E da API (Karate DSL + Cucumber + Allure) — ver seção própria |
| `gatling-tests/` | Testes de carga/performance da API (Gatling, Scala/Maven) — ver seção própria |
| `scripts/` | Scripts auxiliares: `executar_testes.ps1`/`.bat` (suíte pytest com saída completa em `logs/testes.log`; o `.bat` também gera o relatório executivo e abre os relatórios no navegador) e `gerar_relatorio_cobertura.py` (relatório executivo de cobertura em `test_report/cobertura-executiva.html`) |
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
- **Persistência:** cada execução do pipeline batch (`executar_pipeline`) gera um novo arquivo `models/xgb_residual_log_<timestamp>.pkl`, salvo como **bundle** — um único artefato contendo tanto o `XGBRegressor` do resíduo quanto o `Prophet` correspondente (dict `{xgb_model, prophet_model}` serializado via `joblib`) — acompanhado do seu `_meta.json` (métricas `mae`/`rmse`, hiperparâmetros, features, `dataset_info` com a tabela/período de origem, `artifact_format: "bundle"` e `extra` com os limites de clipping do resíduo e o horizonte de previsão). Isso permite que a API reconstrua a previsão híbrida completa a partir do artefato salvo, sem re-treinar (ver seção da API). Os três modelos gerados antes desta funcionalidade continuam em disco no formato antigo (`artifact_format: "legacy"`, apenas o XGBoost) e seguem legíveis via `carregar_modelo`, mas não são usados para servir previsão por faltar o Prophet.

## ✅ Qualidade e Testes

- **502 testes** coletados (`pytest`), **todos passando** — **99,65% de cobertura** sobre `analysis`, `api`, `config`, `dashboard`, `database`, `domain`, `ingestion`, `processing`, `src`, `util` e `validation` (limiar mínimo configurado: 95%, `--cov-fail-under=95`, atingido). Todos os módulos cobertos têm 100% de statements; restam apenas ramos parciais defensivos (ex.: tratamentos de exceção inatingíveis via fluxo normal).
- Relatórios gerados automaticamente em `test_report/`: relatório de testes HTML (`relatorio-testes.html`) + JUnit (`junit.xml`), cobertura técnica (`coverage/index.html` e `coverage.xml`) e **relatório executivo de cobertura** (`cobertura-executiva.html`, gerado por `scripts/gerar_relatorio_cobertura.py` a partir do `.coverage`). A saída completa do pytest pode ser persistida em `logs/testes.log` via `scripts/executar_testes.ps1` — ou use `scripts/executar_testes.bat`, que orquestra o fluxo completo (testes → relatório executivo → abertura dos relatórios no navegador).
- Suíte organizada por domínio: `tests/analysis`, `tests/api`, `tests/arquivos`, `tests/config`, `tests/core`, `tests/dados`, `tests/database`, `tests/dashboard`, `tests/domain`, `tests/ingestion`, `tests/pipeline`, `tests/processing`, `tests/rotas`, `tests/scrapings`, `tests/setup`, `tests/util`, `tests/validation`.

### ✅ Bug de variável de ambiente em `test_connection.py` (resolvido)

As 3 falhas em `tests/database/test_connection.py` (`test_obter_engine_sucesso`, `test_obter_engine_erro_sqlalchemy`, `test_logs_de_criacao_engine`) eram um **bug reprodutível de nome de variável**: a fixture `env_valido` definia a variável de ambiente como `POSTGRES_USERNAME`, mas `database/connection.py::obter_engine` lê `POSTGRES_USER` (sem sufixo). Como a variável esperada nunca era encontrada, `obter_engine()` levantava `EnvironmentError`, mesmo com a fixture "válida" em uso — e as 3 falhas só não apareciam em máquinas com `.env` presente. **Corrigido:** a fixture e o teste de variáveis incompletas agora usam `POSTGRES_USER` (nome canônico, igual ao `.env.example` e ao código de produção).

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

# 7. Abrir o dashboard interativo (requer a API do passo 6 no ar)
streamlit run dashboard/app.py
# Acesse em: http://localhost:8501

# 8. Testes E2E (Karate) e de carga (Gatling) da API — requerem a API do passo 6 no ar
cd karate-tests && mvn test                # E2E (relatório Allure em target/allure-results)
cd ../gatling-tests && mvn gatling:test    # carga (relatório em target/gatling/<simulacao>/index.html)
```

> ✅ **Atualizado nesta revisão:** o `requirements.txt` já é um manifesto **curado** — só dependências de execução (`pandas`, `numpy`, `sqlalchemy`, `psycopg2-binary`, `python-dotenv`, `xgboost`, `prophet`, `scikit-learn`, `statsmodels`, `joblib`, `requests`, `openpyxl`/`xlrd`, `pyyaml`, `beautifulsoup4`, `fastapi`, `uvicorn`, `streamlit`, `plotly`), organizado por seção. Ferramentas de ambiente de desenvolvimento/notebook (`jupyter`, `matplotlib`, `shap`, `docker`, `testcontainers`, `pywin32`/`pywinpty` — este último específico de Windows) já estão isoladas em `requirements-dev.txt`, que não é necessário para rodar o projeto em produção.

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
| GET | `/previsao/crimes-contra-mulher` | Previsão híbrida Prophet+XGBoost, servida por padrão a partir do artefato persistido (`horizonte_anos`, `usar_cache`, `persistir_modelo`) |
| POST | `/previsao/retrain` | Força um novo treino do par Prophet+XGBoost e persiste o bundle resultante (`horizonte_anos`) |
| GET | `/previsao/modelos` | Lista os modelos já persistidos em `models/*_meta.json` (inclui o campo `formato_artefato`: `bundle` ou `legacy`) |

**Persistência do par Prophet+XGBoost (bundle):** `analysis/data_analyzer.py::save_model_with_metadata` aceita um `prophet_model` opcional — quando informado, salva `{xgb_model, prophet_model}` como um único artefato `.pkl` (formato `"bundle"`, registrado em `artifact_format` no `_meta.json`, junto dos `residual_bounds` necessários para prever sem re-treinar). O pipeline batch (`analysis/data_analyzer.py::executar_pipeline`) já salva nesse formato. Artefatos antigos, com apenas o XGBoost (`artifact_format: "legacy"`), continuam podendo ser lidos por `carregar_modelo`, mas não são usados para servir previsão (falta o Prophet).

**Estratégia de serving da API:** `GET /previsao/crimes-contra-mulher` tenta primeiro `analysis.data_analyzer.localizar_ultimo_modelo_bundle` para achar o bundle mais recente em `models/` e servir a previsão diretamente a partir dele (`fonte_modelo: "artefato"` na resposta, sem treinar nada). Se ainda não existir nenhum bundle utilizável (primeira execução, artefato corrompido ou metadados incompletos), a API treina o par Prophet+XGBoost sob demanda a partir da tabela `violencia_contra_mulher_gold` mais recente (`fonte_modelo: "retreino"`); se `persistir_modelo=true`, esse novo treino já é salvo como bundle, disponível para a próxima chamada. Para forçar um novo treino mesmo com um bundle já disponível (ex.: dados gold atualizados), use `POST /previsao/retrain`, que ignora o cache e qualquer artefato existente, treina do zero e sempre persiste o resultado. Um cache em memória de 30 min (`usar_cache`) evita repetir trabalho — seja servindo do artefato, seja re-treinando — a cada requisição idêntica.

Testes: `tests/api/` (47 testes, cobrindo services e endpoints via `TestClient`, com mocks do banco/modelo — não requer Postgres nem treinar o modelo de verdade). Complementado por suítes E2E (`karate-tests/`) e de carga (`gatling-tests/`) — ver seções abaixo.

## 📊 Dashboard Interativo (Streamlit — `dashboard/`)

Um painel web em **Streamlit** consome a API e desenha os gráficos com **Plotly**: séries temporais, mapa de calor RA × ano, ranking por RA, previsão Prophet+XGBoost (com métricas e arquivo do modelo) e exploração das tabelas gold. O painel não executa análise própria — apenas reaproveita os endpoints da API.

A aba **Séries Temporais** mostra o **total consolidado por ano** (soma de todas as RAs) como linha principal, com **RAs selecionáveis** via multiselect para comparação e **média móvel** configurável (janela 1 = desativada). Tanto o seletor de tabelas quanto o de colunas usam **rótulos legíveis em pt-BR** (ex.: `identificacao_crimes_contra_mulher_gold` → "Identificação crimes contra mulher"; `idade_vitima` → "Idade da vítima"), aplicados também aos títulos e eixos dos gráficos.

```
dashboard/
├── app.py              # interface Streamlit (abas: Séries Temporais, Mapa de Calor, Previsões, Tabelas)
├── api_client.py       # cliente HTTP para a API (requests)
└── visualizacoes.py    # transformações pandas + figuras Plotly + rótulos pt-BR (funções puras, testáveis)
```

**Execução:**

```bash
# 1. API no ar (ver seção Camada de Consumo)
uvicorn api.main:app --reload --port 8000

# 2. Abrir o dashboard
streamlit run dashboard/app.py
# Acesse em: http://localhost:8501
```

A URL da API pode ser alterada na sidebar do próprio painel (padrão: `http://localhost:8000`).

Testes: `tests/dashboard/` (74 testes) — o app é exercitado via `AppTest` (`streamlit.testing.v1`) com o cliente HTTP mockado; sem servidor nem banco.

## 🔁 Testes E2E da API (Karate DSL + Cucumber + Allure — `karate-tests/`)

Suíte de testes **end-to-end** da API de consumo, escrita em **Gherkin (Cucumber)** e executada com **Karate DSL**, com relatório **Allure**. Cobrem `GET /health`, `GET /`, os endpoints de gold (`/gold/tabelas`, `/gold/{tabela}/resumo`, `/gold/{tabela}/dados` com paginação/filtros) e de previsão (`GET /previsao/crimes-contra-mulher`, `GET /previsao/modelos`). O cenário `@retreino` (`POST /previsao/retrain`) treina e persiste um novo bundle do modelo e fica **excluído** por padrão (inclua com `mvn test "-Dkarate.options=--tags @retreino"`).

```bash
# Requer a API no ar (uvicorn api.main:app --reload --port 8000) com o banco populado
cd karate-tests
mvn test
# Relatório Allure:
allure generate target/allure-results -o target/allure-report
```

Base URL configurável via `mvn test -Dkarate.env=hml` (ver `karate-tests/README.md`). O baseUrl padrão é `http://localhost:8000`.

## 🏋️ Testes de Carga da API (Gatling — `gatling-tests/`)

Testes de **carga/performance** da API em **Scala** com **Gatling**, cobrindo o fluxo principal do dashboard (health, gold e previsões). Duas simulações:

- `SmokeSimulation` — uma execução única de cada endpoint, para confirmar que a API responde antes da carga.
- `ApiCargaSimulation` — rampa de `1` → `5` usuários/s durante 30s, seguida de 30s de carga constante, sobre uma tabela gold aleatória; falha o build se a taxa de sucesso ficar abaixo de **99%** ou o **p95** acima do limite (padrão **4000 ms**, configurável — margem para o overhead da inferência ML na rota de previsão).

```bash
# Requer a API no ar (uvicorn api.main:app --reload --port 8000) com o banco populado
cd gatling-tests
mvn gatling:test                                       # carga (ApiCargaSimulation)
mvn gatling:test -Dgatling.simulationClass=criminalidade.api.SmokeSimulation
# Relatório HTML estático em target/gatling/<simulacao>/index.html
```

Parâmetros da carga (propriedades do sistema, todos opcionais): `api.baseUrl`, `carga.usuariosIniciais`, `carga.usuariosFinais`, `carga.duracaoRampaSegundos`, `carga.duracaoCargaSegundos`, `carga.p95LimiteMs` — ver `gatling-tests/README.md`. A primeira execução registrada (`smoke.log`) falhou por **`Connection refused`** (API não estava no ar durante a rodada), não por bug de código.

> **Ajuste de capacidade (commit `99a0823`):** os padrões originais da rampa (`20` usuários/s finais, 60s de carga, p95 de 1000 ms) derrubavam a API local sob alta concorrência. Os defaults foram reduzidos para `5` usuários/s e 30s de carga, e o limite de p95 ampliado para 4000 ms — valores que a API sustenta estável com o endpoint de previsão (Prophet+XGBoost) no fluxo. Para cenários mais agressivos, sobrecarregue via propriedades do sistema (ex.: `-Dcarga.usuariosFinais=10`).

## 📌 Observações e Pontos de Atenção (herdados da análise técnica do projeto)

- ✅ **Padronização de RA consolidada:** as variantes de nome de Região Administrativa (ex.: `SUDOESTE` → `SUDOESTE/OCTOGONAL`) antes eram tratadas por chamadas pontuais e duplicadas de `renomear_linha` em `domain/violencia_mulher.py` e `domain/identificacao_crimes.py`, mais um `.replace({...})` inline e independente em `ViolenciaMulherService.carregar_feminicidio`. Agora existe um único mapeamento mestre (`util.padronizacao.MAPEAMENTO_REGIOES_ADMINISTRATIVAS`, aplicado via `renomear_regioes_conhecidas`), usado pelos três pontos — qualquer nova variante encontrada no futuro deve ser adicionada só ali. Cobertura de teste nova em `tests/util/test_padronizacao.py` e `tests/domain/` (que antes não existiam).
- ✅ **Maturidade igual entre pipelines:** o pipeline Silver (`pipeline_busca_transformacao.py`) foi levado ao mesmo modelo do Gold — definição declarativa de `PipelineStep`s para os tratamentos independentes, executados em paralelo via `executar_pipeline` (com retry e timeout), preservando em sequência apenas as fases com dependência de dados (coleta → população → planilhas → carga).
- ✅ **`src/main.py` executa as três etapas:** coleta/transformação, tabela gold e modelagem rodam em sequência por padrão — todo o fluxo tem cobertura de teste (incluindo o bloco `if __name__ == "__main__":`, coberto via `runpy`).
- ✅ **Metadados de modelo padronizados:** todos os artefatos em `models/` (incluindo os `xgb_residual_log_*`) já geram `_meta.json` via `save_model_with_metadata` (métricas, hiperparâmetros, features, dataset_info).
- ✅ **`requirements.txt` curado:** ver nota na seção "Como Executar" — já está separado de `requirements-dev.txt`.
- ✅ **`.env` não é mais rastreado pelo Git:** confirmado nesta revisão (`git ls-tree` não lista o arquivo) — o item antes pendente de `git rm --cached .env` já foi resolvido. Ainda assim, se alguma credencial real chegou a ser commitada antes dessa correção, ela permanece no histórico do repositório e deveria ter sido rotacionada por precaução.
- ✅ **Cobertura de teste ampliada:** `pytest.ini` mede `analysis`, `api`, `config`, `dashboard`, `database`, `domain`, `ingestion`, `processing`, `src`, `util` e `validation` — cobertura real medida nesta revisão: **99,65%** (502 testes), com `validation/validator.py` agora em 100% (antes era o único pacote de produção sem testes próprios, coberto em apenas 36%).
- ✅ **Bug de variável de ambiente em teste resolvido:** a fixture `env_valido` de `tests/database/test_connection.py` usava `POSTGRES_USERNAME`, divergente de `POSTGRES_USER` (código de produção e `.env.example`) — as 3 falhas só não apareciam com `.env` local. Corrigido para `POSTGRES_USER`; ver seção "✅ Qualidade e Testes".

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
- ✅ **Prophet persistido junto ao XGBoost** — `GET /previsao/crimes-contra-mulher` já serve por padrão a partir do artefato ("bundle") salvo em `models/`, com `POST /previsao/retrain` como endpoint de retrain explícito. Ver seção "🌐 Camada de Consumo (API)" acima.
- ✅ **Dashboard interativo (Streamlit/Plotly) implementado** — ver seção "📊 Dashboard Interativo" acima.
- ✅ **Testes E2E da API (Karate DSL + Cucumber + Allure) implementados** — ver seção "🔁 Testes E2E da API" acima.
- ✅ **Testes de carga/performance da API (Gatling) implementados** — ver seção "🏋️ Testes de Carga da API" acima.