Feature: Causalidade de Granger entre indicadores anuais

  Background:
    * url baseUrl

  Scenario: GET /analise/granger com apenas significantes devolve somente pares significante
    Given path 'analise', 'granger'
    When method GET
    Then status 200
    And match response.max_lag == 1
    And match response.alpha == 0.05
    And match response.total_pares == '#? _ >= 0'
    And match response.total_significantes == response.total_pares
    And match karate.sizeOf(response.pares) == response.total_pares
    And match each response.pares[*].significante == true

  Scenario: GET /analise/granger sem filtro devolve todos os pares testados
    Given path 'analise', 'granger'
    And params { apenas_significantes: false, limite: 200 }
    When method GET
    Then status 200
    And def total_completo = response.total_pares
    And def total_sig = response.total_significantes
    And def filtro_consistente = total_completo >= total_sig
    And match filtro_consistente == true
    And match karate.sizeOf(response.pares) == total_completo
    # todo par tem origem/destino distintos e melhor_lag dentro do intervalo testado
    And def paresValidos = response.pares.every(function(it){ return it.origem != it.destino; })
    And match paresValidos == true
    # significantes têm sempre p-valor abaixo do alpha
    And def sigComPvalor = response.pares.every(function(it){ return !it.significante || (it.p_valor != null && it.p_valor < response.alpha); })
    And match sigComPvalor == true

  Scenario: GET /analise/granger respeita o limite de pares retornados
    Given path 'analise', 'granger'
    And params { apenas_significantes: false, limite: 5 }
    When method GET
    Then status 200
    And match karate.sizeOf(response.pares) == '#? _ <= 5'

  Scenario: GET /analise/granger aceita max_lag=2 e reflete o parâmetro
    Given path 'analise', 'granger'
    And params { apenas_significantes: false, max_lag: 2, limite: 10 }
    When method GET
    Then status 200
    And match response.max_lag == 2
    And def lagsValidos = response.pares.every(function(it){ return it.melhor_lag == null || (it.melhor_lag >= 1 && it.melhor_lag <= 2); })
    And match lagsValidos == true

  Scenario: GET /analise/granger com max_lag fora do intervalo retorna 422
    Given path 'analise', 'granger'
    And param max_lag = 4
    When method GET
    Then status 422
