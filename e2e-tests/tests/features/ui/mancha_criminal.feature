# charset: UTF-8
@ui @regression
Feature: Aba Mancha Criminal do Dashboard
  Eu como usuário gostaria de
  explorar a mancha criminal por Região Administrativa
  do dashboard para visualizar a concentração
  da criminalidade ao longo dos anos.

  Background:
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Mancha Criminal"

  @severity=normal
  Scenario: TC-MCA-01 Visualizar os controles da mancha criminal
    Then eu visualizo o seletor "Coluna (indicador)" na mancha criminal
    And eu visualizo o seletor "Recorte temporal" na mancha criminal
    And eu visualizo o gráfico da mancha criminal
    And eu visualizo as RAs mais críticas da mancha criminal

  @severity=normal
  Scenario Outline: <id> Visualizar a mancha criminal da tabela "<tabela>"
    When eu escolho a opção "<tabela>" no seletor "Crimes" da mancha criminal
    Then eu visualizo o seletor "Recorte temporal" na mancha criminal
    And eu visualizo o gráfico da mancha criminal

    Examples:
      | id       | tabela                            |
      | TC-MCA-02 | Crimes patrimoniais (roubo/furto) |
      | TC-MCA-03 | Crimes letais                     |
      | TC-MCA-04 | Crimes discriminatórios           |
      | TC-MCA-05 | Violência contra mulher           |

  @severity=normal
  Scenario Outline: <id> Visualizar a mancha criminal do indicador "<indicador>" da tabela "<tabela>"
    When eu escolho a opção "<tabela>" no seletor "Crimes" da mancha criminal
    And eu escolho a opção "<indicador>" no seletor "Coluna (indicador)" da mancha criminal
    Then eu visualizo o gráfico da mancha criminal

    Examples:
      | id       | tabela                            | indicador                    |
      | TC-MCA-06 | Crimes letais                     | Ocorrencia latrocinio        |
      | TC-MCA-07 | Crimes patrimoniais (roubo/furto) | Ocorrencia roubo comercio    |
      | TC-MCA-08 | Crimes discriminatórios           | Ocorrencia injuria           |
      | TC-MCA-09 | Violência contra mulher           | Crimes contra a mulher       |

  @severity=normal
  Scenario Outline: <id> Visualizar a mancha criminal do recorte temporal "<recorte>"
    When eu escolho a opção "<recorte>" no seletor "Recorte temporal" da mancha criminal
    Then eu visualizo o gráfico da mancha criminal
    And eu visualizo as RAs mais críticas da mancha criminal

    Examples:
      | id        | recorte          |
      | TC-MCA-10 | Todo o período   |
      | TC-MCA-11 | 2018             |
      | TC-MCA-12 | 2020             |