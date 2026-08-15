# Testes E2E da API — Karate DSL + Cucumber + Allure

Suíte de testes **end-to-end** da API de consumo (`api/`) do projeto
Criminalidade Brasília/DF, escrita em **Gherkin (Cucumber)** e executada com
**Karate DSL**, com relatório **Allure**.

## Pré-requisitos

- JDK 8+ (o `JAVA_HOME` apontando para o JDK 21 já instalado na máquina)
- Maven 3.9+
- Allure Commandline (`allure` no PATH)
- A API rodando localmente com o banco populado:

```bash
uvicorn api.main:app --reload --port 8000
```

> O `baseUrl` padrão é `http://localhost:8000`. Para outro ambiente, use
> `mvn test -Dkarate.env=hml` (configurável em `src/test/resources/karate-config.js`).

## Estrutura

```
karate-tests/
├── pom.xml                              # dependências (karate-junit5, allure-junit5) e build
├── src/test/
│   ├── java/br/com/criminalidade/api/
│   │   └── RunnerTest.java              # runner JUnit5 que varre classpath:karate
│   └── resources/
│       ├── karate-config.js             # configuração global (baseUrl)
│       ├── allure.properties            # diretório de saída do Allure
│       └── karate/
│           ├── health.feature           # GET /health
│           ├── raiz.feature             # GET /
│           ├── gold/
│           │   ├── tabelas.feature      # GET /gold/tabelas
│           │   ├── resumo.feature       # GET /gold/{tabela}/resumo
│           │   └── dados.feature        # GET /gold/{tabela}/dados (paginação, filtros)
│           └── previsao/
│               ├── crimes_contra_mulher.feature  # GET /previsao/crimes-contra-mulher
│               └── modelos.feature      # GET /previsao/modelos
```

## Como rodar

```bash
mvn test
```

- Relatório nativo do Karate: `target/karate-reports/`
- Resultados Allure: `target/allure-results/`
- Relatório Allure (HTML estático):

```bash
allure generate target/allure-results -o target/allure-report
allure open target/allure-report
```

> No Allure 3.x o `generate` não aceita `--clean`; apague o diretório
> `target/allure-report` antes de regenerar, se necessário.

Ou, para abrir um servidor com o relatório:

```bash
allure serve target/allure-results
```

### Cenários de retreino

O cenário `@retreino` (`POST /previsao/retrain`) treina e persiste um novo
bundle do modelo em `models/` — é **excluído** por padrão. Para incluí-lo:

```bash
mvn test -Dkarate.options="--tags @retreino"
```

> No PowerShell, o `@` precisa ser escapado mantendo o argumento entre aspas:
> `mvn test "-Dkarate.options=--tags @retreino"`.
