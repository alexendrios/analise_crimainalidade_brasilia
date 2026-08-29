# Testes End-to-End UI — Criminalidade Brasília/DF

Automação E2E da **UI (Streamlit Dashboard)** com **CodeceptJS**, **Playwright**, **Cucumber** (BDD), **Allure Report** e **Page Object Model (POM)**.

> **Nota:** Os testes de API E2E são cobertos pelo **Karate DSL** (`karate-tests/`).
> Este pacote foca exclusivamente nos testes de UI do dashboard Streamlit.

## Estrutura (POM)

```
e2e-tests/
├── codecept.conf.js                    # Configuração CodeceptJS + Playwright
├── package.json                        # Dependências e scripts
├── .gitignore                          # Ignora node_modules/ e artefatos gerados
│
├── helpers/                            # Helpers customizados
│   ├── custom.js                       # Bootstrap do run (data/hora/localidade)
│   ├── allure-evidence-helper.js       # Log de evidências (debug)
│   └── allure-attachments.js           # Anexo de evidências via allure-js-commons
│
├── utils/
│   ├── util.js                         # Utilitários de exibição (bootstrap)
│   └── attach-allure-evidence.js       # Anexa evidências ao Allure (teardown)
│
├── tests/
│   ├── features/ui/                    # CENÁRIOS BDD (Gherkin)
│   │   ├── dashboard.feature          # Carregamento, título, sidebar (3 cenários)
│   │   ├── tabs.feature               # Navegação entre as 12 abas (12 cenários)
│   │   └── interactions.feature       # Health check + sub-abas de Análises (5 cenários)
│   │
│   ├── pages/                          # PAGE OBJECTS
│   │   ├── BasePage.js                # Base: seletores Streamlit e navegação de abas
│   │   ├── SidebarPage.js             # Page Object da sidebar (extends BasePage)
│   │   ├── index.js                   # barrel export
│   │   └── tabs/                      # Page Objects das abas (extends TabsBase)
│   │       ├── TabsBase.js
│   │       ├── VisaoGeralTab.js
│   │       ├── SeriesTemporaisTab.js
│   │       ├── MapaCalorTab.js
│   │       ├── ManchaCriminalTab.js
│   │       ├── IdentificacaoCrimesTab.js
│   │       ├── DesaparecidosTab.js
│   │       ├── ViolenciaIdososTab.js
│   │       ├── PrevisoesTab.js
│   │       ├── ClassificacaoTab.js
│   │       ├── AnalisesTab.js         # inclui sub-tabs (Correlações, Granger, etc.)
│   │       ├── ResumoGeralTab.js
│   │       └── TabelasTab.js
│   │
│   └── steps/ui/                       # STEP DEFINITIONS
│       ├── dashboard_steps.js         # Given/Then do carregamento e sidebar
│       ├── tabs_steps.js              # When/Then das abas
│       └── interactions_steps.js      # When/Then das interações e sub-abas
```

## Requisitos

- Node.js >= 18
- Dashboard rodando em `localhost:8501`
- API rodando em `localhost:8000` (necessária para o dashboard)
- Allure CLI (ou o `allure-commandline` local em `devDependencies`)

## Instalação

```bash
cd e2e-tests
npm install
npx playwright install chromium
```

## Execução

```bash
# Testes críticos (smoke)
npm run test:smoke

# Testes de UI
npm run test:ui

# Todos os testes
npm run test:e2e

# Com Allure Report (gera evidências + abre o relatório)
npm run test:all
npm run allure:serve
npm run allure:report
```

### Scripts

| Script | Descrição |
|--------|-----------|
| `test:ui` | Roda cenários `@ui` |
| `test:e2e` | Roda todos os cenários (`codeceptjs run --steps`) |
| `test:smoke` | Roda cenários `@smoke` |
| `test:regression` | Roda cenários `@regression` |
| `test:all` | Roda todos com o plugin Allure |
| `test:headed` | Roda `@ui` em modo visível (`SHOW=true`) |
| `allure:attach` | Anexa evidências aos resultados Allure |
| `allure:serve` | Serve o relatório Allure |
| `allure:report` | Gera o relatório estático em `allure-report/` |

## Cenários

| Feature | Cenários | Cobertura |
|---------|----------|-----------|
| dashboard | 3 | Carregamento, título, sidebar, aba padrão |
| tabs | 12 | Navegação por todas as 12 abas do dashboard |
| interactions | 5 | Health check da API + sub-abas de Análises |
| **Total** | **20** | **12 abas + 4 sub-abas + sidebar** |

### Arquitetura dos cenários e `async/await`

Os page objects e step definitions fazem uso de `async/await` para operações assíncronas:

- **Abrir o app / navegar** — `I.amOnPage` e `I.waitForElement` são aguardados com `await`.
- **Ativar uma aba** — `I.click` seguido de `await I.waitForElement('[data-testid="stApp"]')`, pois a renderização do conteúdo dispara chamadas à API e pode demorar.
- **Aguardar conteúdo** — `I.waitForText(subheader, 45)` tolera a renderização lenta das abas pesadas (ex.: Análises, que chama correlações/Granger/anomalias/zonas quentes).
- **Health check da API** — `I.waitForClickable` + `I.click` no botão e `I.waitForText` para a resposta assíncrona.

### Evidências no Allure

A cada execução, screenshot, vídeo e trace (Playwright) são anexados ao relatório Allure via `utils/attach-allure-evidence.js`, acionado pelo `teardown` do `codecept.conf.js`. As evidências são geradas para cenários **que falham** (extensões `.failed.png/.webm/.zip`) e casadas por nome sanitizado do cenário.

## Page Objects

| Page Object | Responsabilidade |
|-------------|-----------------|
| `BasePage` | Seletores Streamlit comuns, navegação e verificação de abas (`[role="tab"]`, `aria-selected`) |
| `SidebarPage` | Configuração da API, botão "Verificar conexão", mensagem de health check |
| `VisaoGeralTab` | ABA: "Visão Geral" |
| `SeriesTemporaisTab` | ABA: "Séries Temporais" |
| `MapaCalorTab` | ABA: "Mapa de Calor" |
| `ManchaCriminalTab` | ABA: "Mancha Criminal" |
| `IdentificacaoCrimesTab` | ABA: "Identificação crimes" |
| `DesaparecidosTab` | ABA: "Desaparecidos" |
| `ViolenciaIdososTab` | ABA: "Violência contra idosos" |
| `PrevisoesTab` | ABA: "Previsões" |
| `ClassificacaoTab` | ABA: "Classificação" |
| `AnalisesTab` | ABA: "Análises" (sub-tabs Correlações, Granger, Anomalias, Zonas Quentes) |
| `ResumoGeralTab` | ABA: "Resumo Geral" |
| `TabelasTab` | ABA: "Tabelas" |

## Tags

- `@smoke` — Testes críticos de sanity
- `@ui` — Testes de UI
- `@regression` — Todos os testes
- `@severity=<value>` — Severidade no Allure (critical / normal)

## Variáveis de Ambiente

```bash
UI_URL=http://localhost:8501    # URL do Dashboard
SHOW=true                       # Modo headed (visível)
```

## Tecnologias

| Tecnologia | Versão | Uso |
|-----------|--------|-----|
| CodeceptJS | 3.x | Framework de testes E2E |
| Playwright | recente | Browser automation (Chromium) |
| Cucumber | (built-in) | BDD com Gherkin |
| Allure | 2.x | Relatório de testes + evidências |
| POM | — | Page Object Model (design pattern) |
