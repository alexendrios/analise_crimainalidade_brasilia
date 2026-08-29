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
  Scenario Outline: <id> Navegar para a aba <aba>
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "<aba>"
    Then a aba "<aba>" está selecionada
    And eu visualizo o conteúdo da "<aba>"

    Examples:
      | id         | aba                   |
      | TC-TABS-02 | Séries Temporais      |
      | TC-TABS-03 | Mapa de Calor         |
      | TC-TABS-04 | Mancha Criminal       |
      | TC-TABS-05 | Identificação crimes  |
      | TC-TABS-06 | Desaparecidos         |
      | TC-TABS-07 | Violência contra idosos |
      | TC-TABS-08 | Previsões             |
      | TC-TABS-09 | Classificação         |
      | TC-TABS-10 | Análises              |
      | TC-TABS-11 | Resumo Geral          |
      | TC-TABS-12 | Tabelas               |
