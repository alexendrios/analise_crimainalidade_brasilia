# charset: UTF-8
@ui @regression
Feature: Widgets do Dashboard
  Eu como usuário gostaria de
  validar a presença dos controles
  (selectboxes, sliders, checkboxes e métricas)
  de cada aba do dashboard.

  @severity=normal
  Scenario Outline: <id> Validar os widgets da aba <aba>
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "<aba>"
    Then eu visualizo os widgets de "<tipo>"

    Examples:
      | id         | aba                   | tipo                    |
      | TC-WDGT-01 | Visão Geral           | seleção Visão Geral     |
      | TC-WDGT-02 | Visão Geral           | métricas Visão Geral    |
      | TC-WDGT-03 | Previsões             | Previsões               |
      | TC-WDGT-04 | Classificação         | Classificação           |
      | TC-WDGT-05 | Análises              | sub-abas Análises       |
      | TC-WDGT-06 | Análises              | Correlações             |
      | TC-WDGT-07 | Análises              | Granger                 |
      | TC-WDGT-08 | Análises              | Zonas Quentes           |
      | TC-WDGT-09 | Resumo Geral          | Resumo Geral IA         |
      | TC-WDGT-10 | Tabelas               | Tabelas                 |
