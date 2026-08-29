# charset: UTF-8
@ui @regression
Feature: Aba Previsões do Dashboard
  Eu como usuário gostaria de
  explorar a previsão de crimes contra a mulher
  do dashboard para ajustar o horizonte de projeção
  e comparar valor previsto, componente Prophet,
  resíduo e métricas do modelo.

  Background:
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Previsões"

  @severity=normal
  Scenario: TC-PRV-01 Visualizar os controles da previsão
    Then eu visualizo o seletor "Horizonte (anos)" na base de previsões
    And eu visualizo as métricas do modelo na previsão
    And eu visualizo o gráfico da previsão com o horizonte "5"
    And eu visualizo a tabela da previsão
    And eu visualizo os modelos persistidos na previsão

  @severity=normal
  Scenario Outline: <id> Alterar o horizonte para "<horiz>" anos na previsão
    When eu altero o horizonte para "<horiz>" anos na previsão
    Then o valor do horizonte é "<horiz>" anos na previsão
    And eu visualizo o gráfico da previsão com o horizonte "<horiz>"
    And eu visualizo a tabela da previsão
    And eu visualizo os modelos persistidos na previsão

    Examples:
      | id        | horiz |
      | TC-PRV-02 | 2     |
      | TC-PRV-03 | 3     |
      | TC-PRV-04 | 8     |
      | TC-PRV-05 | 10    |