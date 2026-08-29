# Testes End-to-End UI — Criminalidade Brasília/DF

Automação E2E da **UI (Streamlit Dashboard)** com **CodeceptJS**, **Playwright**, **Cucumber** (BDD), **Allure Report** e **Page Object Model (POM)**.

> **Nota:** Os testes de API E2E são cobertos pelo **Karate DSL** (`karate-tests/`).
> Este pacote foca exclusivamente nos testes de UI do dashboard Streamlit.

## Estrutura (POM)

```
e2e-tests/
├── codecept.conf.js              # Configuração CodeceptJS + Playwright
├── package.json                   # Dependências e scripts
├── allure.config.json             # Configuração Allure
├── steps_file.js                  # Helper global (I.amOnPage)
│
├── pages/                         # PAGE OBJECTS
│   ├── BasePage.js                # Classe base com seletores Streamlit
│   ├── SidebarPage.js             # Page Object da sidebar
│   ├── index.js                   # barrel export de todas as pages
│   └── tabs/                      # Page Objects das abas
│       ├── VisaoGeralTab.js
│       ├── SeriesTemporaisTab.js
│       ├── MapaCalorTab.js
│       ├── ManchaCriminalTab.js
│       ├── IdentificacaoCrimesTab.js
│       ├── DesaparecidosTab.js
│       ├── ViolenciaIdososTab.js
│       ├── PrevisoesTab.js
│       ├── ClassificacaoTab.js
│       ├── AnalisesTab.js         # Inclui sub-tabs (Correlações, Granger, etc.)
│       ├── ResumoGeralTab.js
│       └── TabelasTab.js
│
├── features/                      # CENÁRIOS BDD (Gherkin)
│   └── ui/
│       ├── dashboard.feature      # Carregamento + sidebar (4 cenários)
│       ├── tabs.feature           # Navegação entre 12 abas (11 cenários)
│       └── interactions.feature   # Interações + sub-abas (18 cenários)
│
└── steps/                         # STEP DEFINITIONS
    └── ui/
        ├── dashboard_steps.js     # Given/When/Then do dashboard
        ├── tabs_steps.js          # Given/When/Then das abas
        └── interactions_steps.js  # Given/When/Then das interações
```

## Requisitos

- Node.js >= 18
- Dashboard rodando em `localhost:8501`
- API rodando em `localhost:8000` (necessária para o dashboard)

## Instalação

```bash
cd e2e-tests
npm install
npx playwright install chromium
```

## Execução

```bash
# Testes da UI
npm run test:ui

# Todos os testes
npm run test:e2e

# Smoke tests
npm run test:smoke

# Com Allure Report
npm run test:all
npm run allure:serve
```

## Cenários

| Feature | Cenários | Cobertura |
|---------|----------|-----------|
| dashboard | 4 | Carregamento, sidebar, título, aba padrão |
| tabs | 11 | Todas as 12 abas do dashboard |
| interactions | 18 | KPIs, gráficos, sub-abas de Análises, tabelas |
| **Total** | **33** | **12 abas + 4 sub-abas + sidebar** |

## Page Objects

| Page Object | Responsabilidade |
|-------------|-----------------|
| `BasePage` | Seletores Streamlit comuns, métodos de espera e verificação |
| `SidebarPage` | URL input, health check, selectbox da sidebar |
| `VisaoGeralTab` | Métricas, selectboxes, gráficos de visão geral |
| `SeriesTemporaisTab` | Gráficos de série temporal, sliders, selectboxes |
| `MapaCalorTab` | Heatmap RA × ano, selectboxes |
| `ManchaCriminalTab` | Densidade geoespacial, métricas de ranking |
| `IdentificacaoCrimesTab` | Distribuição de idades, sliders, tabelas |
| `DesaparecidosTab` | Gráficos de desaparecidos |
| `ViolenciaIdososTab` | Gráficos de violência contra idosos |
| `PrevisoesTab` | Forecast Prophet + XGBoost, métricas, tabelas |
| `ClassificacaoTab` | Regressão logística, matriz de confusão |
| `AnalisesTab` | Sub-tabs: Correlações, Granger, Anomalias, Zonas Quentes |
| `ResumoGeralTab` | IA (Ollama), controles de geração |
| `TabelasTab` | Exploração de dados gold, filtros |

## Tags

- `@smoke` — Testes críticos de sanity
- `@ui` — Testes de UI
- `@regression` — Todos os testes

## Variáveis de Ambiente

```bash
UI_URL=http://localhost:8501    # URL do Dashboard
SHOW=true                       # Modo headed (visível)
```

## Tecnologias

| Tecnologia | Versão | Uso |
|-----------|--------|-----|
| CodeceptJS | 3.x | Framework de testes E2E |
| Playwright | 1.49+ | Browser automation (Chromium) |
| Cucumber | (built-in) | BDD com Gherkin |
| Allure | 2.x | Relatório de testes |
| POM | — | Page Object Model (design pattern) |
