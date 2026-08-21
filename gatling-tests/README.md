# Testes de Carga da API — Gatling

Testes de **carga/performance** da API de consumo (`api/`) do projeto
Criminalidade Brasília/DF, escritos em **Scala** com **Gatling**. Cobrem o
fluxo principal do dashboard: health, tabelas gold, resumo, dados paginados,
previsões de crimes contra a mulher e classificação de criminalidade letal
por Regressão Logística.

## Pré-requisitos

- JDK 11+ (o `JAVA_HOME` apontando para o JDK 21 já instalado na máquina)
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
├── pom.xml                              # gatling-maven-plugin + scala-maven-plugin
└── src/test/scala/criminalidade/api/
    ├── SmokeSimulation.scala            # verificação rápida dos endpoints (1 usuário)
    └── ApiCargaSimulation.scala         # teste de carga com rampa e asserções
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
| `carga.usuariosIniciais`       | `1`      | Usuários/segundo no início da rampa         |
| `carga.usuariosFinais`         | `5`      | Usuários/segundo no fim da rampa            |
| `carga.duracaoRampaSegundos`   | `30`     | Duração da rampa                            |
| `carga.duracaoCargaSegundos`   | `30`     | Carga constante após a rampa                |
| `carga.p95LimiteMs`            | `4000`   | Limite do p95 (ms) para as asserções (margem para o overhead da inferência ML na rota de previsão) |

> **Nota de capacidade:** os padrões originais (`usuariosFinais=20`, `duracaoCargaSegundos=60`,
> `p95LimiteMs=1000`) derrubavam a API local sob alta concorrência e foram ajustados para os
> valores acima, que a API sustenta estável com os endpoints de previsão e classificação no fluxo.
> A rota de classificação usa cache em memória (30 min), então após a primeira chamada ela
> responde sem recarregar o banco nem re-executar inferência.

Exemplo de uma execução curta:

```bash
mvn gatling:test "-Dcarga.duracaoRampaSegundos=10" "-Dcarga.duracaoCargaSegundos=20"
```

> No PowerShell, argumentos com `.`/`=` podem exigir aspas em torno da
> propriedade inteira, como acima.

## Asserções

A `ApiCargaSimulation` falha o build se:

- menos de **99%** das requisições obtiverem sucesso; ou
- o **p95** do tempo de resposta ficar acima do limite configurado.

## Relatório

O relatório HTML estático é gerado em:

```
target/gatling/<nome-da-simulacao>/index.html
```
