package criminalidade.api

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

/**
 * Teste de carga da API de consumo (camada Gold + previsões).
 *
 * Simula usuários executando o fluxo principal do dashboard: health, listagem
 * de tabelas, resumo e dados paginados de uma tabela gold (aleatória) e as
 * previsões de crimes contra a mulher.
 *
 * Parâmetros (propriedades do sistema, todos opcionais):
 *   -Dapi.baseUrl=...              base da API (padrão: http://localhost:8000)
 *   -Dcarga.usuariosIniciais=N     usuários/seg no início da rampa (padrão: 1)
 *   -Dcarga.usuariosFinais=N       usuários/seg no fim da rampa (padrão: 20)
 *   -Dcarga.duracaoRampaSegundos=N duração da rampa (padrão: 30)
 *   -Dcarga.duracaoCargaSegundos=N carga constante após a rampa (padrão: 60)
 *   -Dcarga.p95LimiteMs=N          limite do p95 (ms) para as asserções (padrão: 1000)
 *
 * Exemplos:
 *   mvn gatling:test
 *   mvn gatling:test "-Dcarga.usuariosFinais=5" "-Dcarga.duracaoCargaSegundos=30"
 */
class ApiCargaSimulation extends Simulation {

  private val baseUrl = sys.props.getOrElse("api.baseUrl", "http://localhost:8000")

  private val usuariosIniciais = sys.props.get("carga.usuariosIniciais").map(_.toDouble).getOrElse(1.0)
  private val usuariosFinais = sys.props.get("carga.usuariosFinais").map(_.toDouble).getOrElse(20.0)
  private val duracaoRampa = sys.props.get("carga.duracaoRampaSegundos").map(_.toInt).getOrElse(30)
  private val duracaoCarga = sys.props.get("carga.duracaoCargaSegundos").map(_.toInt).getOrElse(60)
  private val p95LimiteMs = sys.props.get("carga.p95LimiteMs").map(_.toInt).getOrElse(1000)

  private val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .userAgentHeader("Gatling - Criminalidade Brasilia/DF")
    .connectionHeader("keep-alive")

  private val tabelas = Seq(
    "crimes_letais_gold",
    "violencia_contra_mulher_gold",
    "identificacao_crimes_contra_mulher_gold",
    "crimes_roubo_furto_gold",
    "desaparecidos_regiao_gold"
  )

  private val tabelaAleatoria = Iterator.continually(Map("tabela" -> tabelas(scala.util.Random.nextInt(tabelas.size))))

  private val leitura = scenario("Leitura de dados (gold + previsoes)")
    .feed(tabelaAleatoria)
    .exec(
      http("GET /health")
        .get("/health")
        .check(status.is(200), jsonPath("$.status").is("ok"))
    )
    .pause(1, 3)
    .exec(
      http("GET /gold/tabelas")
        .get("/gold/tabelas")
        .check(status.is(200), jsonPath("$.total").exists)
    )
    .pause(1, 2)
    .exec(
      http("GET /gold/{tabela}/resumo")
        .get("/gold/#{tabela}/resumo")
        .check(status.is(200), jsonPath("$.linhas").gt("0"))
    )
    .pause(1, 2)
    .exec(
      http("GET /gold/{tabela}/dados")
        .get("/gold/#{tabela}/dados")
        .queryParam("pagina", "1")
        .queryParam("tamanho_pagina", "50")
        .queryParam("ano_min", "2015")
        .queryParam("ano_max", "2025")
        .check(status.is(200), jsonPath("$.registros").count.gte(1))
    )
    .pause(1, 2)
    .exec(
      http("GET /previsao/crimes-contra-mulher")
        .get("/previsao/crimes-contra-mulher")
        .queryParam("horizonte_anos", "5")
        .check(status.is(200), jsonPath("$.previsao").count.is(5))
    )
    .pause(1)
    .exec(
      http("GET /previsao/modelos")
        .get("/previsao/modelos")
        .check(status.is(200), jsonPath("$.total").exists)
    )

  setUp(
    leitura.inject(
      rampUsersPerSec(usuariosIniciais).to(usuariosFinais).during(duracaoRampa.seconds),
      constantUsersPerSec(usuariosFinais).during(duracaoCarga.seconds)
    )
  )
    .protocols(httpProtocol)
    .assertions(
      global.successfulRequests.percent.gte(99.0),
      global.responseTime.percentile3.lt(p95LimiteMs)
    )
}
