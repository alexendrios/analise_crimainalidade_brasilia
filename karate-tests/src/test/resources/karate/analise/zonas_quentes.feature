Feature: Zonas quentes na malha geoespacial

  Background:
    * url baseUrl

  Scenario: GET /analise/zonas-quentes devolve células ordenadas por ocorrências
    Given path 'analise', 'zonas-quentes'
    When method GET
    Then status 200
    And match response.ano_referencia == '#number'
    And match response.tamanho_celula_km == 1.5
    And match response.celulas_com_ocorrencias == '#? _ > 0'
    And match karate.sizeOf(response.zonas) == '#? _ >= 1 && _ <= 20'

    # cada zona é uma célula da malha identificada e com contagem positiva
    And def zonasValidas = response.zonas.every(function(it){ return it.celula_id != null && it.ocorrencia_roubo_pedestre > 0; })
    And match zonasValidas == true

    # ordenação descendente por ocorrências
    And def ordenadoDesc = response.zonas.every(function(it, i, arr){ return i == 0 || arr[i-1].ocorrencia_roubo_pedestre >= it.ocorrencia_roubo_pedestre; })
    And match ordenadoDesc == true

    # celula_id segue o padrão R<linha>C<coluna> zero-preenchido em 3 dígitos
    And def padraoConsistente = response.zonas.every(function(it){ return /^R\d{3}C\d{3}$/.test(it.celula_id); })
    And match padraoConsistente == true

  Scenario: GET /analise/zonas-quentes respeita top_n e reflete tamanho_celula_km customizado
    Given path 'analise', 'zonas-quentes'
    And params { top_n: 3, tamanho_celula_km: 2 }
    When method GET
    Then status 200
    And match response.tamanho_celula_km == 2.0
    And match karate.sizeOf(response.zonas) == '#? _ <= 3'

  Scenario: GET /analise/zonas-quentes com tamanho de célula inválido retorna 422
    Given path 'analise', 'zonas-quentes'
    And param tamanho_celula_km = 0
    When method GET
    Then status 422

  Scenario: GET /analise/zonas-quentes com top_n acima do máximo retorna 422
    Given path 'analise', 'zonas-quentes'
    And param top_n = 201
    When method GET
    Then status 422

  Scenario: GET /analise/zonas-quentes com tamanho_celula_km zero retorna 422
    Given path 'analise', 'zonas-quentes'
    And param tamanho_celula_km = 0
    When method GET
    Then status 422

  Scenario: GET /analise/zonas-quentes com tamanho_celula_km negativo retorna 422
    Given path 'analise', 'zonas-quentes'
    And param tamanho_celula_km = -1.5
    When method GET
    Then status 422

  Scenario: GET /analise/zonas-quentes com tamanho_celula_km acima do máximo retorna 422
    Given path 'analise', 'zonas-quentes'
    And param tamanho_celula_km = 21
    When method GET
    Then status 422

  Scenario: GET /analise/zonas-quentes com top_n zero retorna 422
    Given path 'analise', 'zonas-quentes'
    And param top_n = 0
    When method GET
    Then status 422

  Scenario: GET /analise/zonas-quentes com top_n negativo retorna 422
    Given path 'analise', 'zonas-quentes'
    And param top_n = -5
    When method GET
    Then status 422

  Scenario: GET /analise/zonas-quentes com tamanho_celula_km=20 (máximo) respeita o parâmetro
    Given path 'analise', 'zonas-quentes'
    And params { tamanho_celula_km: 20, top_n: 3 }
    When method GET
    Then status 200
    And match response.tamanho_celula_km == 20.0

  Scenario: GET /analise/zonas-quentes com top_n=1 devolve exatamente 1 célula
    Given path 'analise', 'zonas-quentes'
    And param top_n = 1
    When method GET
    Then status 200
    And match karate.sizeOf(response.zonas) == 1
