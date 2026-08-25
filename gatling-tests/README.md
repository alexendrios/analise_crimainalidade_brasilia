# Testes de Carga da API — Gatling

Testes de **carga/performance** da API de consumo (`api/`) do projeto
Criminalidade Brasília/DF, escritos em **Scala** com **Gatling**. Cobrem três
cenários: warmup (preenchimento de cache), leitura do dashboard (health,
tabelas gold, resumo, dados paginados, previsões e classificação) e as
análises executivas (correlações, Granger, anomalias e zonas quentes).

## Pré-requisitos

- JDK 11+ (`JAVA_HOME` apontando para o JDK 21 instalado na máquina)
- Maven 3.9+
- A API rodando localmente com o banco populado:

```bash
uvicorn api.main:app --reload --port 8000
```

> O `baseUrl` padrão é `http://localhost:8000`. Para outro ambiente, use
> `-Dapi.baseUrl=http://localhost:8001`.

## Estrutura

```
gatling-tests/
├── pom.xml                                          # gatling-maven-plugin + scala-maven-plugin
├── src/test/resources/
│   └── gatling.conf                                 # requestTimeout=120s, GA desabilitado
└── src/test/scala/criminalidade/api/
    ├── SmokeSimulation.scala                        # verificação rápida dos endpoints (1 usuário)
    └── ApiCargaSimulation.scala                     # teste de carga com warmup + rampa + asserções
```

## Como rodar

Teste de carga (simulação padrão):

```bash
mvn gatling:test
```

Smoke test (validar que a API responde antes da carga):

```bash
mvn gatling:test -Dgatling.simulationClass=criminalidade.api.SmokeSimulation
```

### Parâmetros da `ApiCargaSimulation`

Todos opcionais (propriedades do sistema):

| Propriedade                    | Padrão   | Descrição                                   |
| ------------------------------ | -------- | ------------------------------------------- |
| `api.baseUrl`                  | `http://localhost:8000` | Base da API                    |
| `carga.usuariosIniciais`       | `1`      | Usuários/segundo no início da rampa (perfil leitura) |
| `carga.usuariosFinais`         | `5`      | Usuários/segundo no fim da rampa (perfil leitura) |
| `carga.duracaoRampaSegundos`   | `30`     | Duração da rampa                            |
| `carga.duracaoCargaSegundos`   | `30`     | Carga constante após a rampa                |
| `carga.p95LimiteMs`            | `4000`   | Limite do p95 (ms) do perfil de leitura     |
| `carga.analiseUsuariosFinais`  | `1`      | Usuários/segundo no fim da rampa (perfil análises) |
| `carga.p95AnaliseLimiteMs`     | `30000`  | Limite do p95 (ms) do perfil de análises    |

Exemplo de uma execução curta:

```bash
mvn gatling:test "-Dcarga.duracaoRampaSegundos=10" "-Dcarga.duracaoCargaSegundos=20"
```

> No PowerShell, argumentos com `.`/`=` podem exigir aspas em torno da
> propriedade inteira, como acima.

## Cenários

A `ApiCargaSimulation` executa três cenários simultaneamente:

### 1. Warmup (`atOnceUsers(1)`)

Executa uma única vez cada endpoint de análise (correlações, Granger,
anomalias, zonas-quentes) para **preencher o cache da API** antes do load
principal. Isso evita thundering herd — múltiplas requisições concorrentes
batendo cache miss e recarregando as 6 tabelas gold simultaneamente.

### 2. Leitura de dados (rampa `usuariosIniciais` → `usuariosFinais`)

Fluxo principal do dashboard com `pause(20)` inicial para aguardar o warmup:

1. `GET /health`
2. `GET /gold/tabelas`
3. `GET /gold/{tabela}/resumo` (tabela aleatória)
4. `GET /gold/{tabela}/dados` (paginação)
5. `GET /previsao/crimes-contra-mulher`
6. `GET /previsao/modelos`
7. `GET /classificacao/criminalidade-letal`

### 3. Análises executivas (rampa `0.1` → `analiseUsuariosFinais`)

Endpoints computacionais com `pause(20)` inicial para aguardar o warmup:

1. `GET /analise/correlacoes`
2. `GET /analise/granger`
3. `GET /analise/anomalias`
4. `GET /analise/zonas-quentes`

## `gatling.conf`

O timeout de requisição foi elevado para **120 segundos** (padrão Gatling: 60s)
porque os endpoints de análise realizam cálculos pesados sobre as tabelas gold
(isolation forest, grid geoespacial, causalidade de Granger). Sob carga
concorrente, o tempo de resposta pode exceder o timeout padrão.

```
gatling.http.requestTimeout = 120000
```

## Asserções

A simulação falha o build se:

- menos de **99%** das requisições obtiverem sucesso; ou
- o **p95** do tempo de resposta ficar acima do limite configurado.

Os limites de p95 são avaliados **por perfil** via grupos do Gatling:

| Grupo    | Limite padrão | Parâmetro                |
| -------- | ------------- | ------------------------ |
| Leitura  | 4.000 ms      | `carga.p95LimiteMs`      |
| Análises | 30.000 ms     | `carga.p95AnaliseLimiteMs` |

> **Nota:** os endpoints de análise são intrinsicamente mais lentos porque
> recalculam sobre as tabelas gold (correlações multivariadas, testes de
> Granger, Isolation Forest, grid geoespacial). O `gatling.conf` eleva o
> timeout para 120s e o default do p95 foi ajustado para 30.000ms.

## Arquitetura do cache e concorrência

A API usa cache em memória com TTL para evitar recálculos:

| Cache            | TTL    | Proteção      | Função                                    |
| ---------------- | ------ | ------------- | ----------------------------------------- |
| `_cache_resultados` | 30 min | `threading.Lock` | Resultados das 4 análises              |
| `_cache_dados`      | 5 min  | `threading.Lock` | Tabelas gold (6 tabelas via SQL)       |

O warmup do Gatling garante que `_cache_resultados` já está preenchido quando
o load principal inicia. O `_cache_dados` evita que cada chamada de análise
recarregue as mesmas 6 tabelas do banco.

## Relatório

O relatório HTML estático é gerado em:

```
target/gatling/<nome-da-simulacao>/index.html
```

A pasta mais recente pode ser encontrada com:

```bash
ls -td gatling-tests/target/gatling/*/ | head -1
```
