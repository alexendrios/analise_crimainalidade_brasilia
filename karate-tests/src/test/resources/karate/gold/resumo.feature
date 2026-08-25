Feature: Resumo estatístico de uma tabela gold

  Background:
    * url baseUrl

  Scenario Outline: resumo de uma tabela materializada
    Given path 'gold', '<tabela>', 'resumo'
    When method GET
    Then status 200
    And match response.tabela == '<tabela>'
    And match response.linhas == '#? _ > 0'
    And match response.colunas == '#? _ > 0'
    And match response.colunas_com_nulos == '#? _ >= 0'
    And match response.nulos_total == '#? _ >= 0'
    And match response.tempo_execucao_s == '#? _ >= 0'
    And match response.linhas == '#number'
    And match response.colunas == '#number'

    Examples:
      | tabela |
      | crimes_letais_gold |
      | violencia_contra_mulher_gold |
      | identificacao_crimes_contra_mulher_gold |
      | desaparecidos_regiao_gold |
      | violencia_idosos_gold |

  Scenario: resumo de tabela desconhecida retorna 404
    Given path 'gold', 'tabela_que_nao_existe', 'resumo'
    When method GET
    Then status 404
    And match response.detail != null

  Scenario: resumo indica colunas_com_nulos <= colunas
    Given path 'gold', 'crimes_letais_gold', 'resumo'
    When method GET
    Then status 200
    And def valido = response.colunas_com_nulos <= response.colunas
    And match valido == true

  Scenario: resumo indica nulos_total consistente com colunas_com_nulos
    Given path 'gold', 'crimes_letais_gold', 'resumo'
    When method GET
    Then status 200
    # se não há colunas com nulos, o total deve ser zero
    And def consistente = response.colunas_com_nulos > 0 || response.nulos_total == 0
    And match consistente == true

  Scenario: resumo contém o nome exato da tabela
    Given path 'gold', 'crimes_letais_gold', 'resumo'
    When method GET
    Then status 200
    And match response.tabela == 'crimes_letais_gold'

  Scenario: resumo de tabela gold sem registros no banco retorna 404 (tabela não encontrada)
    Given path 'gold', 'tabela_valida_mas_nao_existente', 'resumo'
    When method GET
    Then status 404
