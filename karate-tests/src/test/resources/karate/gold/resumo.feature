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

    Examples:
      | tabela |
      | crimes_letais_gold |
      | violencia_contra_mulher_gold |
      | identificacao_crimes_contra_mulher_gold |

  Scenario: resumo de tabela desconhecida retorna 404
    Given path 'gold', 'tabela_que_nao_existe', 'resumo'
    When method GET
    Then status 404
    And match response.detail != null
