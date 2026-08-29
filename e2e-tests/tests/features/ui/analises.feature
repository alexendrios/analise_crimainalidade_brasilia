# charset: UTF-8
@ui @regression
Feature: Aba Análises do Dashboard
  Eu como usuário gostaria de
  explorar as análises executivas do dashboard
  para avaliar correlações entre indicadores,
  causalidade de Granger e zonas quentes
  de ocorrências patrimoniais.

  Background:
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Análises"

  @severity=normal
  Scenario: TC-ANA-01 Visualizar os controles das correlações
    When eu ativo a sub-aba "Correlações" na análise
    Then eu visualizo o seletor "Método" na base de análises
    And eu visualizo o seletor "Pares destaque" na base de análises
    And eu visualizo as métricas das correlações
    And eu visualizo o mapa de calor das correlações com o método "pearson"
    And eu visualizo os pares mais correlacionados

  @severity=normal
  Scenario: TC-ANA-02 Alterar o método de correlação para spearman
    When eu ativo a sub-aba "Correlações" na análise
    When eu seleciono o método "spearman" nas correlações
    Then eu visualizo o mapa de calor das correlações com o método "spearman"
    And eu visualizo os pares mais correlacionados
    And eu visualizo as métricas das correlações

  @severity=normal
  Scenario: TC-ANA-03 Visualizar os controles da causalidade de Granger
    When eu ativo a sub-aba "Granger" na análise
    Then eu visualizo o seletor "Defasagem máxima (anos)" na base de análises
    And eu visualizo o seletor "Somente pares significantes (p < 0,05)" na base de análises
    And eu visualizo as métricas da causalidade de Granger
    And eu visualizo o gráfico da causalidade de Granger com a defasagem "1"

  @severity=normal
  Scenario: TC-ANA-04 Alterar a defasagem máxima da causalidade de Granger
    When eu ativo a sub-aba "Granger" na análise
    When eu altero a defasagem máxima para "2" anos na causalidade de Granger
    Then o gráfico da causalidade de Granger reflete a defasagem "2"
    And eu visualizo as métricas da causalidade de Granger

  @severity=normal
  Scenario: TC-ANA-05 Desmarcar o filtro de significância na causalidade de Granger
    When eu ativo a sub-aba "Granger" na análise
    When eu desmarco o filtro de significância na causalidade de Granger
    Then eu visualizo o gráfico da causalidade de Granger com a defasagem "1"
    And eu visualizo as métricas da causalidade de Granger

  @severity=normal
  Scenario: TC-ANA-06 Visualizar os controles das zonas quentes
    When eu ativo a sub-aba "Zonas Quentes" na análise
    Then eu visualizo o seletor "Tamanho da célula (km)" na base de análises
    And eu visualizo o seletor "Células no ranking" na base de análises
    And eu visualizo as métricas das zonas quentes
    And eu visualizo o gráfico das zonas quentes
    And eu visualizo o dataframe das zonas quentes

  @severity=normal
  Scenario: TC-ANA-07 Alterar as células no ranking das zonas quentes
    When eu ativo a sub-aba "Zonas Quentes" na análise
    When eu altero as células no ranking para "10" nas zonas quentes
    Then o valor das células no ranking é "10" nas zonas quentes
    And eu visualizo o gráfico das zonas quentes
    And eu visualizo o dataframe das zonas quentes