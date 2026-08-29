# charset: UTF-8
@ui @regression
Feature: Aba Desaparecidos do Dashboard
  Eu como usuário gostaria de
  explorar os desaparecidos no Distrito Federal
  do dashboard para acompanhar sexo, faixa etária,
  status (localizados × ainda desaparecidos)
  e distribuição regional.

  Background:
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Desaparecidos"

  @severity=normal
  Scenario: TC-DES-01 Visualizar o bloco e os gráficos de desaparecidos
    Then eu visualizo o subcabeçalho "Desaparecidos" na base de desaparecidos
    And eu visualizo o gráfico "Desaparecidos por sexo" na base de desaparecidos
    And eu visualizo o gráfico "Desaparecidos por faixa etária" na base de desaparecidos
    And eu visualizo o gráfico "Localizados × ainda desaparecidos" na base de desaparecidos
    And eu visualizo o gráfico "Desaparecimentos por RA — 2020 × 2021" na base de desaparecidos

  @severity=normal
  Scenario: TC-DES-02 Visualizar o gráfico de desaparecidos por sexo
    Then eu visualizo o gráfico "Desaparecidos por sexo" na base de desaparecidos
    And eu visualizo a categoria "MASCULINO" no gráfico "Desaparecidos por sexo" na base de desaparecidos
    And eu visualizo a categoria "FEMININO" no gráfico "Desaparecidos por sexo" na base de desaparecidos

  @severity=normal
  Scenario: TC-DES-03 Visualizar o gráfico de desaparecidos por faixa etária
    Then eu visualizo o gráfico "Desaparecidos por faixa etária" na base de desaparecidos
    And eu visualizo a categoria "DE 12 A 17 ANOS" no gráfico "Desaparecidos por faixa etária" na base de desaparecidos
    And eu visualizo a categoria "MAIS DE 50 ANOS" no gráfico "Desaparecidos por faixa etária" na base de desaparecidos

  @severity=normal
  Scenario: TC-DES-04 Visualizar o gráfico de localizados e ainda desaparecidos
    Then eu visualizo o gráfico "Localizados × ainda desaparecidos" na base de desaparecidos
    And eu visualizo a categoria "LOCALIZADOS" no gráfico "Localizados × ainda desaparecidos" na base de desaparecidos
    And eu visualizo a categoria "AINDA DESAPARECIDOS" no gráfico "Localizados × ainda desaparecidos" na base de desaparecidos

  @severity=normal
  Scenario: TC-DES-05 Visualizar o gráfico de desaparecimentos por RA
    Then eu visualizo o gráfico "Desaparecimentos por RA — 2020 × 2021" na base de desaparecidos
    And eu visualizo a categoria "CEILANDIA" no gráfico "Desaparecimentos por RA — 2020 × 2021" na base de desaparecidos
    And eu visualizo a categoria "PLANALTINA" no gráfico "Desaparecimentos por RA — 2020 × 2021" na base de desaparecidos