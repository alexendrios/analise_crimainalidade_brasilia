Feature: Consulta paginada de dados das tabelas gold

  Background:
    * url baseUrl

  Scenario: paginação retorna a quantidade correta de registros
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And params { pagina: 1, tamanho_pagina: 10 }
    When method GET
    Then status 200
    And match response.tabela == 'crimes_letais_gold'
    And match response.pagina == 1
    And match response.tamanho_pagina == 10
    And match response.total_linhas == '#? _ > 0'
    And def total_paginas_esperado = Math.ceil(response.total_linhas / 10)
    And match response.total_paginas == total_paginas_esperado
    And match karate.sizeOf(response.registros) == 10

  Scenario: a última página devolve o restante dos registros
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And params { pagina: 1, tamanho_pagina: 10 }
    When method GET
    Then status 200
    And def total_paginas = response.total_paginas
    And def total_linhas = response.total_linhas
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And params { pagina: '#(total_paginas)', tamanho_pagina: 10 }
    When method GET
    Then status 200
    And match response.pagina == total_paginas
    And match response.total_linhas == total_linhas
    And match karate.sizeOf(response.registros) == '#? _ > 0'

  Scenario: filtro por região administrativa é case-insensitive
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And params { tamanho_pagina: 1000, regiao_administrativa: 'taguatinga' }
    When method GET
    Then status 200
    And match response.total_linhas == '#? _ > 0'
    And match each response.registros[*].regiao_administrativa == 'TAGUATINGA'

  Scenario: filtro por intervalo de anos
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And params { tamanho_pagina: 1000, ano_min: 2020, ano_max: 2024 }
    When method GET
    Then status 200
    And match response.total_linhas == '#? _ > 0'
    And match each response.registros[*].ano == '#? _ >= 2020'
    And match each response.registros[*].ano == '#? _ <= 2024'

  Scenario: tamanho de página acima do limite permitido é rejeitado
    Given path 'gold', 'crimes_letais_gold', 'dados'
    And param tamanho_pagina = 1001
    When method GET
    Then status 422

  Scenario: dados de tabela desconhecida retornam 404
    Given path 'gold', 'tabela_que_nao_existe', 'dados'
    When method GET
    Then status 404
    And match response.detail != null
