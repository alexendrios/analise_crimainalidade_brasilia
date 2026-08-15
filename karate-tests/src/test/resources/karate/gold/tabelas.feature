Feature: Listagem das tabelas gold disponíveis

  Background:
    * url baseUrl

  Scenario: GET /gold/tabelas lista as tabelas esperadas
    Given path 'gold', 'tabelas'
    When method GET
    Then status 200
    And match response.total == 12
    And match karate.sizeOf(response.tabelas) == response.total
    And match each response.tabelas[*].nome contains 'gold'
    And match each response.tabelas[*].descricao != null
    And match response.tabelas[*].nome contains 'crimes_letais_gold'
    And match response.tabelas[*].nome contains 'violencia_contra_mulher_gold'
    And match response.tabelas[*].nome contains 'identificacao_crimes_contra_mulher_gold'
    And match response.tabelas[*].nome contains 'desaparecidos_regiao_gold'

  Scenario: toda tabela gold está materializada no banco
    Given path 'gold', 'tabelas'
    When method GET
    Then status 200
    And match each response.tabelas[*].disponivel_no_banco == true
