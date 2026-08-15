package criminalidade.api

import io.gatling.core.Predef._
import io.gatling.http.Predef._

/**
 * Verificação rápida (smoke) dos endpoints principais da API.
 *
 * Executa cada chamada uma única vez, validando status e campos essenciais.
 * Útil para confirmar que a API está no ar antes de rodar a carga real.
 *
 *   mvn gatling:test -Dgatling.simulationClass=criminalidade.api.SmokeSimulation
 */
class SmokeSimulation extends Simulation {

  private val baseUrl = sys.props.getOrElse("api.baseUrl", "http://localhost:8000")

  private val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .userAgentHeader("Gatling Smoke - Criminalidade Brasilia/DF")

  private val fluxo = scenario("Smoke: endpoints principais")
    .exec(
      http("GET /health")
        .get("/health")
        .check(status.is(200), jsonPath("$.status").is("ok"))
    )
    .pause(1)
    .exec(
      http("GET /gold/tabelas")
        .get("/gold/tabelas")
        .check(status.is(200), jsonPath("$.total").gt("0"))
    )
    .pause(1)
    .exec(
      http("GET /gold/{tabela}/resumo")
        .get("/gold/crimes_letais_gold/resumo")
        .check(status.is(200), jsonPath("$.linhas").gt("0"))
    )
    .pause(1)
    .exec(
      http("GET /gold/{tabela}/dados")
        .get("/gold/crimes_letais_gold/dados")
        .queryParam("tamanho_pagina", "10")
        .check(status.is(200), jsonPath("$.registros").count.gte(1))
    )
    .pause(1)
    .exec(
      http("GET /previsao/crimes-contra-mulher")
        .get("/previsao/crimes-contra-mulher")
        .queryParam("horizonte_anos", "2")
        .check(status.is(200), jsonPath("$.previsao").count.is(2))
    )
    .pause(1)
    .exec(
      http("GET /previsao/modelos")
        .get("/previsao/modelos")
        .check(status.is(200), jsonPath("$.total").gt("0"))
    )

  setUp(fluxo.inject(atOnceUsers(1))).protocols(httpProtocol)
}
