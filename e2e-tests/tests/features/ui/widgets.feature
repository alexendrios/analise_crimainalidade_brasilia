# charset: UTF-8
@ui @regression
Feature: Widgets do Dashboard
  Eu como usuário gostaria de
  validar a presença dos controles
  (selectboxes, sliders, checkboxes e métricas)
  de cada aba do dashboard.

  @severity=normal
  Scenario: TC-WDGT-01 Validar selects da Visão Geral
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Visão Geral"
    Then eu visualizo os widgets de seleção da Visão Geral

  @severity=normal
  Scenario: TC-WDGT-02 Validar métricas descritivas da Visão Geral
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Visão Geral"
    Then eu visualizo as métricas descritivas da Visão Geral

  @severity=normal
  Scenario: TC-WDGT-03 Validar controles e métricas de Previsões
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Previsões"
    Then eu visualizo o controle de horizonte e as métricas de Previsões

  @severity=normal
  Scenario: TC-WDGT-04 Validar seletor de ano e métricas de Classificação
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Classificação"
    Then eu visualizo o seletor de ano e as métricas de Classificação

  @severity=normal
  Scenario: TC-WDGT-05 Validar sub-abas de Análises
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Análises"
    Then eu visualizo as sub-abas e os controles de Análises

  @severity=normal
  Scenario: TC-WDGT-06 Validar controles da sub-aba Correlações
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Análises"
    Then eu visualizo os controles da sub-aba Correlações

  @severity=normal
  Scenario: TC-WDGT-07 Validar controles da sub-aba Granger
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Análises"
    Then eu visualizo os controles da sub-aba Granger

  @severity=normal
  Scenario: TC-WDGT-08 Validar controles da sub-aba Zonas Quentes
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Análises"
    Then eu visualizo os controles da sub-aba Zonas Quentes

  @severity=normal
  Scenario: TC-WDGT-09 Validar controles de IA do Resumo Geral
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Resumo Geral"
    Then eu visualizo os controles de IA do Resumo Geral
    And eu visualizo o botão de gerar resumo com IA

  @severity=normal
  Scenario: TC-WDGT-10 Validar widgets de filtro da aba Tabelas
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Tabelas"
    Then eu visualizo os widgets de filtro da aba Tabelas
