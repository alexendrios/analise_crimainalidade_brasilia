Feature: Validação de paginação e filtros na consulta de dados gold

  Background:
    * url baseUrl

  Scenario: página=0 retorna 422 (ge=1)
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And param pagina = 0
    When method GET
    Then status 422

  Scenario: página negativa retorna 422
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And param pagina = -1
    When method GET
    Then status 422

  Scenario: tamanho_pagina=0 retorna 422 (ge=1)
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And param tamanho_pagina = 0
    When method GET
    Then status 422

  Scenario: tamanho_pagina negativo retorna 422
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And param tamanho_pagina = -10
    When method GET
    Then status 422

  Scenario: filtro por RA inexistente devolve total_linhas=0 e registros vazio
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And params { tamanho_pagina: 10, regiao_administrativa: 'Cidade_Inexistente' }
    When method GET
    Then status 200
    And match response.total_linhas == 0
    And match response.total_paginas == 0
    And match karate.sizeOf(response.registros) == 0

  Scenario: filtro por ano_min > ano_max devolve registros vazios
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And params { tamanho_pagina: 100, ano_min: 2030, ano_max: 2010 }
    When method GET
    Then status 200
    And match response.total_linhas == 0
    And match karate.sizeOf(response.registros) == 0

  Scenario: tamanho_pagina=1 devolve exatamente 1 registro por página
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And params { pagina: 1, tamanho_pagina: 1 }
    When method GET
    Then status 200
    And match response.tamanho_pagina == 1
    And match karate.sizeOf(response.registros) == 1
    And match response.total_paginas == response.total_linhas

  Scenario: tamanho_pagina=1000 (máximo permitido) não retorna erro
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And params { pagina: 1, tamanho_pagina: 1000 }
    When method GET
    Then status 200
    And match response.tamanho_pagina == 1000

  Scenario: tamanho_pagina=1001 retorna 422 (le=1000)
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And param tamanho_pagina = 1001
    When method GET
    Then status 422

  Scenario: Response de dados contém todos os campos obrigatórios
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And params { pagina: 1, tamanho_pagina: 5 }
    When method GET
    Then status 200
    And match response.tabela == 'crimes_letais_gold'
    And match response.total_linhas == '#number'
    And match response.pagina == '#number'
    And match response.tamanho_pagina == '#number'
    And match response.total_paginas == '#number'
    And match response.registros == '#array'
