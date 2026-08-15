Feature: Modelos treinados e persistidos

  Background:
    * url baseUrl

  Scenario: GET /previsao/modelos lista os modelos com métricas
    Given path 'previsao', 'modelos'
    When method GET
    Then status 200
    And match response.total == '#? _ > 0'
    And match karate.sizeOf(response.modelos) == response.total
    And match each response.modelos[*].arquivo != null
    And match each response.modelos[*].metricas != null
    And match response.modelos[*].formato_artefato contains 'bundle'
