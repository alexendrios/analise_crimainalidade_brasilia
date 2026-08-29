# charset: UTF-8
@ui @regression
Feature: Navegação entre as abas do Dashboard
  Eu como usuário gostaria de
  navegar pelas abas do dashboard
  para consultar as análises disponíveis.

  @severity=normal @smoke
  Scenario: TC-TABS-01 Navegar para a aba Visão Geral
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Visão Geral"
    Then a aba "Visão Geral" está selecionada
    And eu visualizo o conteúdo da "Visão Geral"

  @severity=normal
  Scenario: TC-TABS-02 Navegar para a aba Séries Temporais
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Séries Temporais"
    Then a aba "Séries Temporais" está selecionada
    And eu visualizo o conteúdo da "Séries Temporais"

  @severity=normal
  Scenario: TC-TABS-03 Navegar para a aba Mapa de Calor
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Mapa de Calor"
    Then a aba "Mapa de Calor" está selecionada
    And eu visualizo o conteúdo da "Mapa de Calor"

  @severity=normal
  Scenario: TC-TABS-04 Navegar para a aba Mancha Criminal
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Mancha Criminal"
    Then a aba "Mancha Criminal" está selecionada
    And eu visualizo o conteúdo da "Mancha Criminal"

  @severity=normal
  Scenario: TC-TABS-05 Navegar para a aba Identificação crimes
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Identificação crimes"
    Then a aba "Identificação crimes" está selecionada
    And eu visualizo o conteúdo da "Identificação crimes"

  @severity=normal
  Scenario: TC-TABS-06 Navegar para a aba Desaparecidos
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Desaparecidos"
    Then a aba "Desaparecidos" está selecionada
    And eu visualizo o conteúdo da "Desaparecidos"

  @severity=normal
  Scenario: TC-TABS-07 Navegar para a aba Violência contra idosos
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Violência contra idosos"
    Then a aba "Violência contra idosos" está selecionada
    And eu visualizo o conteúdo da "Violência contra idosos"

  @severity=normal
  Scenario: TC-TABS-08 Navegar para a aba Previsões
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Previsões"
    Then a aba "Previsões" está selecionada
    And eu visualizo o conteúdo da "Previsões"

  @severity=normal
  Scenario: TC-TABS-09 Navegar para a aba Classificação
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Classificação"
    Then a aba "Classificação" está selecionada
    And eu visualizo o conteúdo da "Classificação"

  @severity=normal
  Scenario: TC-TABS-10 Navegar para a aba Análises
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Análises"
    Then a aba "Análises" está selecionada
    And eu visualizo o conteúdo da "Análises"

  @severity=normal
  Scenario: TC-TABS-11 Navegar para a aba Resumo Geral
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Resumo Geral"
    Then a aba "Resumo Geral" está selecionada
    And eu visualizo o conteúdo da "Resumo Geral"

  @severity=normal
  Scenario: TC-TABS-12 Navegar para a aba Tabelas
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Tabelas"
    Then a aba "Tabelas" está selecionada
    And eu visualizo o conteúdo da "Tabelas"
