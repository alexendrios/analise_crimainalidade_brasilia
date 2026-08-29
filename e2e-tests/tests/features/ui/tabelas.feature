# charset: UTF-8
@ui @regression
Feature: Aba Tabelas do Dashboard
  Eu como usuário gostaria de
  explorar a base de tabelas gold do
  dashboard para acompanhar as métricas de
  resumo, os filtros de intervalo de anos
  e a seleção por região administrativa.

  Background:
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Tabelas"

  @severity=normal
  Scenario: TC-TBL-01 Visualizar os controles da aba Tabelas
    Then eu visualizo o seletor "Crimes" na base de tabelas
    And eu visualizo as métricas de resumo na base de tabelas
    And a métrica "Linhas" da tabela selecionada é "347"
    And eu visualizo o seletor "Intervalo de anos" na base de tabelas
    And eu visualizo o seletor "Região Administrativa" na base de tabelas
    And eu visualizo o dataframe na base de tabelas

  @severity=normal
  Scenario: TC-TBL-02 Selecionar a tabela de crimes letais
    When eu seleciono a tabela "Crimes letais" na base de tabelas
    Then a métrica "Linhas" da tabela selecionada é "340"
    And a métrica "Colunas" da tabela selecionada é "6"
    And eu visualizo o dataframe na base de tabelas

  @severity=normal
  Scenario: TC-TBL-03 Selecionar a tabela de desaparecidos por região administrativa
    When eu seleciono a tabela "Desaparecidos — por RA" na base de tabelas
    Then a métrica "Linhas" da tabela selecionada é "33"
    And a métrica "Colunas" da tabela selecionada é "4"
    And eu não visualizo o seletor "Intervalo de anos" na base de tabelas
    And eu visualizo o seletor "Região Administrativa" na base de tabelas
    And eu visualizo o dataframe na base de tabelas

  @severity=normal
  Scenario: TC-TBL-04 Filtrar o início do intervalo de anos
    When eu altero o início do intervalo de anos para "2016" na base de tabelas
    Then o início do intervalo de anos é "2016" na base de tabelas
    And eu visualizo o dataframe na base de tabelas

  @severity=normal
  Scenario: TC-TBL-05 Filtrar por região administrativa
    When eu seleciono a região administrativa "CEILANDIA" na base de tabelas
    Then eu visualizo a região administrativa "CEILANDIA" selecionada na base de tabelas
    And eu visualizo o dataframe na base de tabelas