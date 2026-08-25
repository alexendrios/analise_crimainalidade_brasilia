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

  Scenario: cada modelo contém campo arquivo com extensão .pkl
    Given path 'previsao', 'modelos'
    When method GET
    Then status 200
    And def arquivosValidos = response.modelos.every(function(it){ return it.arquivo.endsWith('.pkl'); })
    And match arquivosValidos == true

  Scenario: cada modelo com formato_artefato='bundle' tem dataset_info
    Given path 'previsao', 'modelos'
    When method GET
    Then status 200
    And def bundles = response.modelos.filter(function(it){ return it.formato_artefato == 'bundle'; })
    And def comDatasetInfo = bundles.every(function(it){ return it.dataset_info != null; })
    And match comDatasetInfo == true

  Scenario: métricas de cada modelo são numéricas não-negativas
    Given path 'previsao', 'modelos'
    When method GET
    Then status 200
    And def metricasValidas = response.modelos.every(function(it){ return it.metricas != null && (it.metricas.mae == null || it.metricas.mae >= 0) && (it.metricas.rmse == null || it.metricas.rmse >= 0); })
    And match metricasValidas == true
