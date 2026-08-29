# charset: UTF-8
@ui @regression
Feature: Aba Violência contra Idosos do Dashboard
  Eu como usuário gostaria de
  explorar a violência contra idosos no Distrito Federal
  do dashboard para acompanhar ocorrências por RA,
  por ano, a série mensal e as vítimas por sexo.

  Background:
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Violência contra idosos"

  @severity=normal
  Scenario: TC-VID-01 Visualizar o bloco e os gráficos de violência contra idosos
    Then eu visualizo o subcabeçalho "Violência contra Idosos" na base de violência contra idosos
    And eu visualizo o gráfico "Violência contra idosos — ocorrências por RA (jan–ago)" na base de violência contra idosos
    And eu visualizo o gráfico "Violência contra idosos — ocorrências por ano" na base de violência contra idosos
    And eu visualizo o gráfico "Violência contra idosos — série mensal" na base de violência contra idosos
    And eu visualizo o gráfico "Violência contra idosos — vítimas por sexo" na base de violência contra idosos

  @severity=normal
  Scenario: TC-VID-02 Visualizar o gráfico de ocorrências por RA
    Then eu visualizo o gráfico "Violência contra idosos — ocorrências por RA (jan–ago)" na base de violência contra idosos
    And eu visualizo a categoria "CEILANDIA" no gráfico "Violência contra idosos — ocorrências por RA (jan–ago)" na base de violência contra idosos
    And eu visualizo a categoria "TAGUATINGA" no gráfico "Violência contra idosos — ocorrências por RA (jan–ago)" na base de violência contra idosos

  @severity=normal
  Scenario: TC-VID-03 Visualizar o gráfico de ocorrências por ano
    Then eu visualizo o gráfico "Violência contra idosos — ocorrências por ano" na base de violência contra idosos
    And eu visualizo a categoria "Ocorrências" no gráfico "Violência contra idosos — ocorrências por ano" na base de violência contra idosos
    And eu visualizo a categoria "Violência dentro de casa" no gráfico "Violência contra idosos — ocorrências por ano" na base de violência contra idosos

  @severity=normal
  Scenario: TC-VID-04 Visualizar o gráfico da série mensal
    Then eu visualizo o gráfico "Violência contra idosos — série mensal" na base de violência contra idosos
    And eu visualizo a categoria "Jan/2016" no gráfico "Violência contra idosos — série mensal" na base de violência contra idosos
    And eu visualizo a categoria "Jul/2017" no gráfico "Violência contra idosos — série mensal" na base de violência contra idosos

  @severity=normal
  Scenario: TC-VID-05 Visualizar o gráfico de vítimas por sexo
    Then eu visualizo o gráfico "Violência contra idosos — vítimas por sexo" na base de violência contra idosos
    And eu visualizo a categoria "Masculino" no gráfico "Violência contra idosos — vítimas por sexo" na base de violência contra idosos
    And eu visualizo a categoria "Feminino" no gráfico "Violência contra idosos — vítimas por sexo" na base de violência contra idosos