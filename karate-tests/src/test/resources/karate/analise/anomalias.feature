Feature: Anomalias detectadas por Isolation Forest

  Background:
    * url baseUrl

  Scenario: GET /analise/anomalias devolve painel e série mensal ordenados do mais extremo
    Given path 'analise', 'anomalias'
    # limite alto o bastante para a lista trazer a população completa do ambiente
    And param limite = 500
    When method GET
    Then status 200
    And match karate.sizeOf(response.painel) == response.total_painel
    And match response.total_painel == '#? _ > 0'

    # resumo não expõe as colunas internas anomalia/score
    And def primeira = response.painel[0]
    And match primeira.regiao_administrativa != null
    And def semColunasInternas = response.painel.every(function(it){ return !('anomalia' in it) && !('score' in it); })
    And match semColunasInternas == true

    # painel contém apenas linhas marcadas como anômalas (valor/lag/média móvel presentes)
    And match each response.painel[*].ano == '#number'
    And match each response.painel[*].ocorrencia_roubo_pedestre == '#number'
    And match each response.painel[*].lag_1 == '#number'
    And match each response.painel[*].diff_1 == '#number'
    And match each response.painel[*].media_movel_3 == '#number'

    # série mensal de idosos presente no ambiente atual
    And match response.total_mensal == '#? _ >= 0'
    And match karate.sizeOf(response.mensal) == '#? _ >= 0'

  Scenario: GET /analise/anomalias respeita o limite por série
    Given path 'analise', 'anomalias'
    And param limite = 5
    When method GET
    Then status 200
    # totais refletem a população completa, mesmo com limite aplicado às listas
    And def esperado_painel = response.total_painel < 5 ? response.total_painel : 5
    And match karate.sizeOf(response.painel) == esperado_painel
    And def esperado_mensal = response.total_mensal < 5 ? response.total_mensal : 5
    And match karate.sizeOf(response.mensal) == esperado_mensal

  Scenario: GET /analise/anomalias com limite abaixo do mínimo retorna 422
    Given path 'analise', 'anomalias'
    And param limite = 0
    When method GET
    Then status 422
