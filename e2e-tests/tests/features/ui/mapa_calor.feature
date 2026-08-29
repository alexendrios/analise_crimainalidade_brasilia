# charset: UTF-8
@ui @regression
Feature: Aba Mapa de Calor do Dashboard
  Eu como usuário gostaria de
  explorar o mapa de calor por Região Administrativa
  do dashboard para visualizar a intensidade
  da criminalidade ao longo dos anos.

  Background:
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Mapa de Calor"

  @severity=normal
  Scenario: TC-MC-01 Visualizar os controles do mapa de calor
    Then eu visualizo o seletor "Coluna (indicador)" no mapa de calor
    And eu visualizo o seletor "Ano para o ranking" no mapa de calor
    And eu visualizo o gráfico do mapa de calor

  @severity=normal
  Scenario Outline: <id> Visualizar o mapa de calor da tabela "<tabela>"
    When eu escolho a opção "<tabela>" no seletor "Crimes" do mapa de calor
    Then eu visualizo o seletor "Ano para o ranking" no mapa de calor
    And eu visualizo o gráfico do mapa de calor

    Examples:
      | id       | tabela                            |
      | TC-MC-02 | Crimes letais                     |
      | TC-MC-03 | Crimes discriminatórios           |
      | TC-MC-04 | Crimes patrimoniais (roubo/furto) |

  @severity=normal
  Scenario Outline: <id> Visualizar o mapa de calor do indicador "<indicador>" da tabela "<tabela>"
    When eu escolho a opção "<tabela>" no seletor "Crimes" do mapa de calor
    And eu escolho a opção "<indicador>" no seletor "Coluna (indicador)" do mapa de calor
    Then eu visualizo o gráfico do mapa de calor

    Examples:
      | id       | tabela                            | indicador                     |
      | TC-MC-05 | Crimes letais                     | Ocorrencia homicidio          |
      | TC-MC-06 | Crimes patrimoniais (roubo/furto) | Ocorrencia furto em veiculo   |
      | TC-MC-07 | Violência contra mulher           | Casos de feminicídio          |

  @severity=normal
  Scenario Outline: <id> Visualizar o ranking do ano "<ano>" no mapa de calor
    When eu escolho a opção "<ano>" no seletor "Ano para o ranking" do mapa de calor
    Then eu visualizo o gráfico do mapa de calor

    Examples:
      | id       | ano            |
      | TC-MC-08 | 2020           |
      | TC-MC-09 | 2015           |
      | TC-MC-10 | None           |