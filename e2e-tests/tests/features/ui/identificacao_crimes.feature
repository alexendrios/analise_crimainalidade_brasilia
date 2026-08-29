# charset: UTF-8
@ui @regression
Feature: Aba Identificação crimes do Dashboard
  Eu como usuário gostaria de
  explorar a identificação dos crimes contra a mulher
  do dashboard para comparar a distribuição
  de idades entre vítima e autor (suspeito).

  Background:
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Identificação crimes"

  @severity=normal
  Scenario: TC-IDC-01 Visualizar os controles da identificação
    Then eu visualizo o seletor "Largura dos bins (anos)" na identificação
    And eu visualizo o gráfico da identificação
    And eu visualizo o resumo da identificação

  @severity=normal
  Scenario Outline: <id> Alterar a largura dos bins para "<bins>" na identificação
    When eu altero a largura dos bins para "<bins>" na identificação
    Then o valor da largura dos bins é "<bins>" na identificação
    And eu visualizo o gráfico da identificação
    And eu visualizo o resumo da identificação

    Examples:
      | id       | bins |
      | TC-IDC-02 | 2    |
      | TC-IDC-03 | 3    |
      | TC-IDC-04 | 8    |
      | TC-IDC-05 | 10   |