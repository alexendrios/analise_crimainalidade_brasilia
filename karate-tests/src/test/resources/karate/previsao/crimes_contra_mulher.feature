Feature: Previsão de crimes contra a mulher

  Background:
    * url baseUrl

  Scenario: GET /previsao/crimes-contra-mulher retorna a previsão do horizonte pedido
    Given path 'previsao', 'crimes-contra-mulher'
    And params { horizonte_anos: 2, usar_cache: true }
    When method GET
    Then status 200
    And match response.tabela_origem == 'violencia_contra_mulher_gold'
    And match response.coluna_alvo == 'crimes_contra_mulher'
    And match response.horizonte_anos == 2
    And match karate.sizeOf(response.previsao) == 2
    And def primeiro_ano = response.previsao[0].ano
    And def segundo_ano = response.previsao[1].ano
    And def diferenca_anos = segundo_ano - primeiro_ano
    And match diferenca_anos == 1
    And match each response.previsao[*].valor_previsto == '#? _ >= 0'
    And match each response.previsao[*].componente_prophet != null
    And match each response.previsao[*].residual_log_aplicado != null
    And match response.metricas_residual.mae == '#? _ > 0'
    And match response.metricas_residual.rmse == '#? _ > 0'
    And match response.fonte_modelo == '#? _ == "artefato" || _ == "retreino"'
    And match response.modelo_arquivo != null
    And match response.gerado_em != null

  Scenario: horizonte fora do intervalo permitido é rejeitado
    Given path 'previsao', 'crimes-contra-mulher'
    And param horizonte_anos = 11
    When method GET
    Then status 422

  @retreino
  Scenario: POST /previsao/retrain persiste um novo bundle e devolve previsão
    Given path 'previsao', 'retrain'
    And param horizonte_anos = 1
    When method POST
    Then status 200
    And match response.fonte_modelo == 'retreino'
    And match response.modelo_arquivo != null
    And match response.horizonte_anos == 1
    And match karate.sizeOf(response.previsao) == 1
