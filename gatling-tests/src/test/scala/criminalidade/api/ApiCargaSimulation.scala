package criminalidade.api

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

/**
 * Teste de carga da API de consumo (camada Gold + previsões + análises).
 *
 * Simula dois perfis de usuário:
 *  - "Leitura": fluxo principal do dashboard — health, listagem de tabelas,
 *    resumo e dados paginados de uma tabela gold (aleatória), previsões e
 *    classificação de criminalidade letal;
 *  - "Análises executivas": correlações multivariadas, causalidade de Granger,
 *    anomalias por Isolation Forest e zonas quentes da malha. Cada requisição
 *    recalcula sobre as tabelas gold, então este perfil usa uma rampa bem
 *    mais leve que a leitura.
 *
 * Parâmetros (propriedades do sistema, todos opcionais):
 *   -Dapi.baseUrl=...                   base da API (padrão: http://localhost:8000)
 *   -Dcarga.usuariosIniciais=N          usuários/seg no início da rampa (padrão: 1)
 *   -Dcarga.usuariosFinais=N            usuários/seg no fim da rampa (padrão: 5)
 *   -Dcarga.duracaoRampaSegundos=N      duração da rampa (padrão: 30)
 *   -Dcarga.duracaoCargaSegundos=N      carga constante após a rampa (padrão: 30)
 *   -Dcarga.p95LimiteMs=N               limite do p95 do fluxo de leitura (padrão: 4000)
 *   -Dcarga.analiseUsuariosFinais=N     usuários/seg no perfil de análises (padrão: 1)
 *   -Dcarga.p95AnaliseLimiteMs=N        limite do p95 do fluxo de análises (padrão: 10000)
 *
 * Exemplos:
 *   mvn gatling:test
 *   mvn gatling:test "-Dcarga.usuariosFinais=5" "-Dcarga.duracaoCargaSegundos=30"
 */
class ApiCargaSimulation extends Simulation {

  private val baseUrl = sys.props.getOrElse("api.baseUrl", "http://localhost:8000")

  private val usuariosIniciais = sys.props.get("carga.usuariosIniciais").map(_.toDouble).getOrElse(1.0)
  private val usuariosFinais = sys.props.get("carga.usuariosFinais").map(_.toDouble).getOrElse(5.0)
  private val duracaoRampa = sys.props.get("carga.duracaoRampaSegundos").map(_.toInt).getOrElse(30)
  private val duracaoCarga = sys.props.get("carga.duracaoCargaSegundos").map(_.toInt).getOrElse(30)
  private val p95LimiteMs = sys.props.get("carga.p95LimiteMs").map(_.toInt).getOrElse(4000)
  private val analiseUsuariosFinais = sys.props.get("carga.analiseUsuariosFinais").map(_.toDouble).getOrElse(1.0)
  private val p95AnaliseLimiteMs = sys.props.get("carga.p95AnaliseLimiteMs").map(_.toInt).getOrElse(10000)

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

  private val nomeLeitura = "Leitura de dados (gold + previsoes + classificacao)"
  private val nomeAnalises = "Analises executivas (correlacoes + granger + anomalias + zonas)"

  private val leitura = scenario(nomeLeitura)
    .feed(tabelaAleatoria)
    .group(nomeLeitura) {
      exec(
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
        .check(status.is(200), jsonPath("$.registros[*]").count.gte(1))
    )
    .pause(1, 2)
    .exec(
      http("GET /previsao/crimes-contra-mulher")
        .get("/previsao/crimes-contra-mulher")
        .queryParam("horizonte_anos", "5")
        .check(status.is(200), jsonPath("$.previsao[*]").count.is(5))
    )
    .pause(1)
    .exec(
      http("GET /previsao/modelos")
        .get("/previsao/modelos")
        .check(status.is(200), jsonPath("$.total").exists)
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
    }

  private val analises = scenario(nomeAnalises)
    .group(nomeAnalises) {
      exec(
      http("GET /analise/correlacoes")
        .get("/analise/correlacoes")
        .queryParam("metodo", "pearson")
        .queryParam("top_n", "5")
        .check(
          status.is(200),
          jsonPath("$.metodo").is("pearson"),
          jsonPath("$.indicadores[*]").count.gte(2),
          jsonPath("$.pares_destaque[*]").count.lte(5)
        )
    )
    .pause(2, 4)
    .exec(
      http("GET /analise/granger")
        .get("/analise/granger")
        .queryParam("apenas_significantes", "false")
        .queryParam("limite", "50")
        .check(
          status.is(200),
          jsonPath("$.total_pares").gt("0"),
          jsonPath("$.pares[*]").count.lte(50)
        )
    )
    .pause(2, 4)
    .exec(
      http("GET /analise/anomalias")
        .get("/analise/anomalias")
        .queryParam("limite", "20")
        .check(
          status.is(200),
          jsonPath("$.total_painel").gt("0"),
          jsonPath("$.painel[*]").count.lte(20)
        )
    )
    .pause(2, 4)
    .exec(
      http("GET /analise/zonas-quentes")
        .get("/analise/zonas-quentes")
        .queryParam("tamanho_celula_km", "1.5")
        .queryParam("top_n", "10")
        .check(
          status.is(200),
          jsonPath("$.ano_referencia").exists,
          jsonPath("$.zonas[*]").count.gte(1),
          jsonPath("$.celulas_com_ocorrencias").gt("0")
        )
    )
    }

  setUp(
    leitura.inject(
      rampUsersPerSec(usuariosIniciais).to(usuariosFinais).during(duracaoRampa.seconds),
      constantUsersPerSec(usuariosFinais).during(duracaoCarga.seconds)
    ),
    analises.inject(
      rampUsersPerSec(0.2).to(analiseUsuariosFinais).during(duracaoRampa.seconds),
      constantUsersPerSec(analiseUsuariosFinais).during(duracaoCarga.seconds)
    )
  )
    .protocols(httpProtocol)
    .assertions(
      global.successfulRequests.percent.gte(99.0),
      // p95 avaliado por perfil: análises recalculam sobre as gold e são mais lentas
      details(nomeLeitura).responseTime.percentile3.lt(p95LimiteMs),
      details(nomeAnalises).responseTime.percentile3.lt(p95AnaliseLimiteMs)
    )
}
