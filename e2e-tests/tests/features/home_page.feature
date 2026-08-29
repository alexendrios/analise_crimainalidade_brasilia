# charset: UTF-8
@flaky, @muted, @known
Feature: Acesso ao Dashboard de Criminalidade
  Eu como usuário gostaria de
  acessar a aplicação e assim
  validar os Dados

  @severity=critical
  Scenario: TC01 - Acesar a aplicação
    Given acesso a aplicação
    Then eu visualizo a seguinte mensagem "TEXTO INEXISTENTE PARA TESTE ALLURE"
