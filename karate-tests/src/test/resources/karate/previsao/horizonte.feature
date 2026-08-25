Feature: Validação do parâmetro horizonte_anos na previsão

  Background:
    * url baseUrl

  Scenario: horizonte_anos=1 devolve exatamente 1 ponto de previsão
    Given path 'previsao', 'crimes-contra-mulher'
    And params { horizonte_anos: 1, usar_cache: true }
    When method GET
    Then status 200
    And match karate.sizeOf(response.previsao) == 1
    And match response.horizonte_anos == 1

  Scenario: horizonte_anos=5 (padrão) devolve 5 pontos
    Given path 'previsao', 'crimes-contra-mulher'
    And params { horizonte_anos: 5, usar_cache: true }
    When method GET
    Then status 200
    And match karate.sizeOf(response.previsao) == 5
    And match response.horizonte_anos == 5

  Scenario: horizonte_anos=10 (máximo permitido) não retorna erro
    Given path 'previsao', 'crimes-contra-mulher'
    And params { horizonte_anos: 10, usar_cache: true }
    When method GET
    Then status 200
    And match karate.sizeOf(response.previsao) == 10

  Scenario: horizonte_anos=0 retorna 422 (ge=1)
    Given path 'previsao', 'crimes-contra-mulher'
    And param horizonte_anos = 0
    When method GET
    Then status 422

  Scenario: horizonte_anos negativo retorna 422
    Given path 'previsao', 'crimes-contra-mulher'
    And param horizonte_anos = -3
    When method GET
    Then status 422

  Scenario: horizonte_anos=11 retorna 422 (le=10)
    Given path 'previsao', 'crimes-contra-mulher'
    And param horizonte_anos = 11
    When method GET
    Then status 422

  Scenario: sem parâmetro usa horizonte_anos=5 (default)
    Given path 'previsao', 'crimes-contra-mulher'
    And param usar_cache = true
    When method GET
    Then status 200
    And match response.horizonte_anos == 5
    And match karate.sizeOf(response.previsao) == 5
