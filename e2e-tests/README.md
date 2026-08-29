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
│   ├── features/ui/                    # CENÁRIOS BDD (Gherkin, 15 features)
│   │   ├── dashboard.feature          # Carregamento, título, sidebar (TC-DASH, 3)
│   │   ├── tabs.feature               # Navegação entre as 12 abas (TC-TABS, 12)
│   │   ├── interactions.feature       # Health check + sub-abas de Análises (TC-INT, 5)
│   │   ├── widgets.feature            # Controles e métricas por aba (TC-WDGT, 12)
│   │   ├── visao_geral.feature        # Métricas-resumo, legenda e troca de tabela (TC-VG, 12)
│   │   ├── series_temporais.feature   # Indicadores, RAs, média móvel, modos (TC-ST, 12)
│   │   ├── mapa_calor.feature         # Mapa RA × ano e ranking (TC-MC, 10)
│   │   ├── mancha_criminal.feature    # Indicador e recorte temporal (TC-MCA, 12)
│   │   ├── identificacao_crimes.feature # Largura dos bins (TC-IDC, 5)
│   │   ├── desaparecidos.feature      # Gráficos por sexo/idade/status/RA (TC-DES, 5)
│   │   ├── violencia_idosos.feature   # Gráficos de violência contra idosos (TC-VID, 5)
│   │   ├── previsoes.feature          # Horizonte da previsão (TC-PRV, 5)
│   │   ├── classificacao.feature      # Ano do ranking e avaliação do modelo (TC-CLA, 5)
│   │   ├── analises.feature           # Correlações/Granger/Zonas Quentes (TC-ANA, 7)
│   │   └── tabelas.feature            # Resumo, troca de tabela e filtros (TC-TBL, 5)
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
│       ├── interactions_steps.js      # When/Then das interações e sub-abas
│       ├── widgets_steps.js           # Despacho de widgets por aba
│       ├── visao_geral_steps.js       # Métricas e legenda
│       ├── series_temporais_steps.js  # Indicadores, categorias, RAs e média móvel
│       ├── mapa_calor_steps.js        # Mapa, ranking e ano
│       ├── mancha_criminal_steps.js   # Indicador e recorte temporal
│       ├── identificacao_crimes_steps.js
│       ├── desaparecidos_steps.js
│       ├── violencia_idosos_steps.js
│       ├── previsoes_steps.js
│       ├── classificacao_steps.js
│       ├── analises_steps.js
│       └── tabelas_steps.js
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
| widgets | 12 | Presença de selectboxes, sliders, checkboxes, campos e métricas por aba (inclui Sub-abas de Análises) |
| visao_geral | 12 | Métricas-resumo (período coberto, tabela, indicador, variação), legenda e troca de tabela de crimes |
| series_temporais | 12 | Seleção de indicador/categoria, total consolidado, comparação de RAs, média móvel e modo de análise |
| mapa_calor | 10 | Mapa RA × ano, ranking horizontal e seleção de ano |
| mancha_criminal | 12 | Indicador, recorte temporal do mapa e ranking lateral das RAs |
| identificacao_crimes | 5 | Widgets (largura dos bins) e alteração da largura dos bins |
| desaparecidos | 5 | Gráficos por sexo, faixa etária, localizados × ainda desaparecidos e por RA |
| violencia_idosos | 5 | Gráficos de ocorrências por RA/ano, série mensal e vítimas por sexo |
| previsoes | 5 | Slider de horizonte, gráfico da previsão e tabela |
| classificacao | 5 | Ano do ranking, limiar, heatmap, odds ratios, matriz de confusão e avaliação do modelo |
| analises | 7 | Correlações (método), Granger (defasagem + filtro de significância) e Zonas Quentes (célula/top-N) |
| tabelas | 5 | Métricas de resumo, troca de tabela, intervalo de anos e região administrativa |
| **Total** | **115** | **12 abas + 4 sub-abas + sidebar + conteúdo e interação por aba** |

### Arquitetura dos cenários e `async/await`

Os page objects e step definitions fazem uso de `async/await` para operações assíncronas:

- **Abrir o app / navegar** — `I.amOnPage` e `I.waitForElement` são aguardados com `await`.
- **Ativar uma aba** — `I.click` seguido de `await I.waitForElement('[data-testid="stApp"]')`, pois a renderização do conteúdo dispara chamadas à API e pode demorar.
- **Aguardar conteúdo** — `I.waitForText(subheader, 45)` tolera a renderização lenta das abas pesadas (ex.: Análises, Tabelas — a carga da tabela completa via API chega a ~40 s; a aba Tabelas usa waits de até 90 s no `stDataFrame`).
- **Health check da API** — `I.waitForClickable` + `I.click` no botão e `I.waitForText` para a resposta assíncrona.

As features que repetem o mesmo passo para vários valores usam **Scenario Outline + Examples** (placeholders `<id>`, `<aba>`, `<subaba>`, `<tabela>`, `<indicador>`, etc. no título e nos passos), e cada linha de `Examples` gera um teste independente nos relatórios/evidências — é o caso de `tabs`, `interactions`, `widgets`, `visao_geral`, `series_temporais`, `mapa_calor`, `mancha_criminal`, `identificacao_crimes`, `previsoes`, `classificacao` e `tabelas`.

### Técnicas de interação E2E

Padrões consolidados nesta suíte (importantes para manutenção futura):

- **Sliders do Streamlit** — são `input[type="range"]` com `aria-label` igual ao rótulo do widget (não `[role="slider"]`), e o input é visualmente clipeado (o clique é interceptado pelo overlay do react-aria). A interação foca o input via `I.executeScript` (busca pelo `aria-label` exato) e ajusta com `I.pressKey('ArrowRight'/'ArrowLeft')`; o valor é lido de volta por `I.executeScript` (`parseInt(input.value, 10)`). Sliders de **intervalo** têm dois inputs (`aria-label "… — start"` e `"… — end"`).
- **ComboBoxes (react-aria)** — o seletor é `[data-testid="stSelectbox"]:visible:has-text("<rótulo>") input`; abre com `I.click` e escolhe a opção por `[role="option"]:visible:has-text("<opção>")`. **Fallback de digitação:** dropdowns são virtualizados e podem não renderizar as últimas opções (ex.: "Desaparecidos — por RA", a 12ª tabela da aba Tabelas) — nesse caso `I.fillField(combobox, opcao)` filtra a lista do react-aria e expõe o item desejado.
- **Checkboxes** — o contêiner `[data-testid="stCheckbox"]:visible:has-text("<texto parcial>")` é clicável e alterna o estado (ex.: filtro de significância do Granger).
- **Polling sem-throw** — helpers que lançam (`waitForElement`, `click`) **envenenam o recorder do CodeceptJS** dentro de loops de retry; os loops usam `I.grabNumberOfVisibleElements` + `setTimeout` (`aguardar`) até o elemento aparecer/desaparecer.
- **Asserções com dados reais** — vários cenários assertam valores vindos da API (ex.: métricas de resumo das tabelas gold, lidas via `[data-testid="stMetricValue"]`, e a seleção de RA lida pelo atributo `value` do combobox — a seleção não aparece como texto no `body`). `restart: true` no Playwright garante browser fresco por cenário, então os preenchimentos sempre começam nos valores padrão.

### Cenários de widgets (`widgets.feature`)

Cobertura da presença dos controles estáveis de cada aba (rótulos fixos, independentes do valor dos dados). É um **Scenario Outline + Examples** (12 exemplos) que reutiliza o passo parametrizado `Then eu visualizo os widgets de "<tipo>"`, despachando para o page object correto via `widgets_steps.js`; cada linha de `Examples` gera um teste independente (TC-WDGT-01..12, incluindo as sub-abas de Análises).

- `#waitForText(label, 45)` é usado em vez de `I.see` para tolerar a renderização assíncrona do conteúdo de cada aba (a carga dispara chamadas à API).
- Sub-abas não padrão da aba Análises (Granger, Zonas Quentes) exigem **ativar a sub-aba** (`AnalisesTab.clicarSubAba`, aguardado com `await`) antes de verificar seus widgets, pois o conteúdo só é carregado sob clique; Correlações é a sub-aba padrão e carrega automaticamente.

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