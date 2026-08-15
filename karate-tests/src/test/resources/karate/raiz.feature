Feature: Raiz da API

  Background:
    * url baseUrl

  Scenario: GET / responde com a mensagem de boas-vindas
    Given path '/'
    When method GET
    Then status 200
    And match response.mensagem != null
