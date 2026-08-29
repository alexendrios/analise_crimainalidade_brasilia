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
  Scenario: TC-INT-04 Navegar para a sub-aba Granger na aba Análises
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Análises"
    Then a aba "Análises" está selecionada
    And eu visualizo a sub-aba "Granger" na aba de Análises

  @severity=normal
  Scenario: TC-INT-05 Navegar para a sub-aba Anomalias na aba Análises
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Análises"
    Then a aba "Análises" está selecionada
    And eu visualizo a sub-aba "Anomalias" na aba de Análises

  @severity=normal
  Scenario: TC-INT-06 Navegar para a sub-aba Zonas Quentes na aba Análises
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Análises"
    Then a aba "Análises" está selecionada
    And eu visualizo a sub-aba "Zonas Quentes" na aba de Análises
