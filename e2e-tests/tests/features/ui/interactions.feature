# charset: UTF-8
@ui @regression
Feature: Interações do Dashboard
  Eu como usuário gostaria de
  interagir com os componentes do dashboard
  para consultar os dados da criminalidade.

  @severity=critical @smoke
  Scenario: TC-INT-01 Verificar a conexão com a API pela sidebar
    Given que eu acesso o dashboard de criminalidade
    When eu clico em "Verificar conexão" na sidebar
    Then eu visualizo a mensagem "API OK — banco: ok"

  @severity=normal @smoke
  Scenario: TC-INT-03 Navegar para a sub-aba Correlações na aba Análises
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Análises"
    Then a aba "Análises" está selecionada
    And eu visualizo a sub-aba "Correlações" na aba de Análises

  @severity=normal
  Scenario Outline: <id> Navegar para a sub-aba <subaba> na aba Análises
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Análises"
    Then a aba "Análises" está selecionada
    And eu visualizo a sub-aba "<subaba>" na aba de Análises

    Examples:
      | id         | subaba       |
      | TC-INT-04 | Granger      |
      | TC-INT-05 | Anomalias    |
      | TC-INT-06 | Zonas Quentes |
