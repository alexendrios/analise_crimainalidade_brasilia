# charset: UTF-8
@ui @regression
Feature: Aba Classificação do Dashboard
  Eu como usuário gostaria de
  explorar a classificação da criminalidade letal por RA
  do dashboard para comparar as probabilidades previstas
  por ano, o mapa de calor RA × ano
  e a avaliação do modelo.

  Background:
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Classificação"

  @severity=normal
  Scenario: TC-CLA-01 Visualizar os controles da classificação
    Then eu visualizo o seletor "Ano do ranking" na base de classificação
    And eu visualizo as métricas do modelo na classificação
    And eu visualizo o ranking de criminalidade letal do ano "2024"
    And eu visualizo o mapa de calor na classificação
    And eu visualizo as classificações por RA e ano na classificação
    And eu visualizo a avaliação do modelo na classificação
    And eu visualizo os odds ratios na classificação
    And eu visualizo a matriz de confusão na classificação

  @severity=normal
  Scenario Outline: <id> Selecionar o ano "<ano>" no ranking da classificação
    When eu seleciono o ano "<ano>" no ranking da classificação
    Then o ranking reflete o ano "<ano>" na classificação
    And eu visualizo o mapa de calor na classificação
    And eu visualizo as classificações por RA e ano na classificação
    And eu visualizo a avaliação do modelo na classificação

    Examples:
      | id        | ano  |
      | TC-CLA-02 | 2015 |
      | TC-CLA-03 | 2018 |
      | TC-CLA-04 | 2021 |
      | TC-CLA-05 | 2023 |