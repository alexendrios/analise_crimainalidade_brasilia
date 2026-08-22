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
        .check(status.is(200), jsonPath("$.registros[*]").count.gte(1))
    )
    .pause(1)
    .exec(
      http("GET /previsao/crimes-contra-mulher")
        .get("/previsao/crimes-contra-mulher")
        .queryParam("horizonte_anos", "2")
        .check(status.is(200), jsonPath("$.previsao[*]").count.is(2))
    )
    .pause(1)
    .exec(
      http("GET /previsao/modelos")
        .get("/previsao/modelos")
        .check(status.is(200), jsonPath("$.total").gt("0"))
    )
    .pause(1)
    .exec(
      http("GET /classificacao/criminalidade-letal")
        .get("/classificacao/criminalidade-letal")
        .check(
          status.is(200),
          jsonPath("$.fonte_modelo").in("artefato", "retreino"),
          jsonPath("$.total_registros").gt("0"),
          jsonPath("$.classificacoes[*]").count.gte(1)
        )
    )
    .pause(1)
    .exec(
      http("GET /analise/correlacoes")
        .get("/analise/correlacoes")
        .queryParam("metodo", "pearson")
        .queryParam("top_n", "3")
        .check(
          status.is(200),
          jsonPath("$.metodo").is("pearson"),
          jsonPath("$.indicadores[*]").count.gte(2),
          jsonPath("$.pares_destaque[*]").count.lte(3),
          jsonPath("$.insights[*]").count.gte(1)
        )
    )
    .pause(1)
    .exec(
      http("GET /analise/granger")
        .get("/analise/granger")
        .queryParam("max_lag", "1")
        .queryParam("limite", "10")
        .check(
          status.is(200),
          jsonPath("$.max_lag").is("1"),
          jsonPath("$.alpha").is("0.05"),
          jsonPath("$.pares[*]").count.lte(10)
        )
    )
    .pause(1)
    .exec(
      http("GET /analise/anomalias")
        .get("/analise/anomalias")
        .queryParam("limite", "5")
        .check(
          status.is(200),
          jsonPath("$.total_painel").gt("0"),
          jsonPath("$.painel[*]").count.lte(5)
        )
    )
    .pause(1)
    .exec(
      http("GET /analise/zonas-quentes")
        .get("/analise/zonas-quentes")
        .queryParam("tamanho_celula_km", "1.5")
        .queryParam("top_n", "3")
        .check(
          status.is(200),
          jsonPath("$.ano_referencia").exists,
          jsonPath("$.zonas[*]").count.gte(1),
          jsonPath("$.celulas_com_ocorrencias").gt("0")
        )
    )

  setUp(fluxo.inject(atOnceUsers(1))).protocols(httpProtocol)
}
