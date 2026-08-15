Feature: Health check da API

  Endpoint de infraestrutura que indica se a API está no ar e se o banco
  de dados está acessível.

  Background:
    * url baseUrl

  Scenario: GET /health informa status ok e banco ok
    Given path 'health'
    When method GET
    Then status 200
    And match response.status == 'ok'
    And match response.database == 'ok'
    And match response.timestamp != null
