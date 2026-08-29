# charset: UTF-8
@ui @regression
Feature: Aba Séries Temporais do Dashboard
  Eu como usuário gostaria de
  explorar as séries temporais do dashboard
  para acompanhar a evolução dos indicadores
  e categorias de criminalidade ao longo dos anos.

  Background:
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Séries Temporais"

  @severity=normal
  Scenario: TC-ST-01 Visualizar os controles da série temporal
    Then eu visualizo o seletor "Modo de análise"
    And eu visualizo o seletor "Coluna (indicador)"
    And eu visualizo o seletor "Comparar RAs"
    And eu visualizo o seletor "Média móvel (janela, 1 = desativada)"

  @severity=normal
  Scenario Outline: <id> Visualizar a série temporal da tabela "<tabela>"
    When eu escolho a opção "<tabela>" no seletor "Crimes" da série temporal
    Then eu visualizo o seletor "Modo de análise"
    And eu visualizo o gráfico da série temporal

    Examples:
      | id       | tabela                            |
      | TC-ST-02 | Crimes patrimoniais (roubo/furto) |
      | TC-ST-03 | Crimes letais                     |
      | TC-ST-04 | Crimes discriminatórios           |

  @severity=normal
  Scenario Outline: <id> Selecionar o indicador "<indicador>" da tabela "<tabela>"
    When eu escolho a opção "<tabela>" no seletor "Crimes" da série temporal
    And eu escolho a opção "<indicador>" no seletor "Coluna (indicador)" da série temporal
    Then eu visualizo o gráfico da série temporal

    Examples:
      | id       | tabela                            | indicador                 |
      | TC-ST-05 | Crimes patrimoniais (roubo/furto) | Ocorrencia roubo pedestre |
      | TC-ST-06 | Crimes letais                     | Ocorrencia homicidio      |
      | TC-ST-07 | Violência contra mulher           | Casos de feminicídio      |

  @severity=normal
  Scenario Outline: <id> Alterar o modo de análise para "<modo>"
    When eu escolho a opção "<modo>" no seletor "Modo de análise" da série temporal
    Then eu visualizo o seletor "<seletor>"

    Examples:
      | id       | modo                    | seletor            |
      | TC-ST-08 | Contagem por categoria  | Categoria          |
      | TC-ST-09 | Indicador numérico      | Coluna (indicador) |

  @severity=normal
  Scenario Outline: <id> Visualizar a série temporal por categoria "<categoria>"
    When eu escolho a opção "Identificação crimes contra mulher" no seletor "Crimes" da série temporal
    And eu escolho a opção "Contagem por categoria" no seletor "Modo de análise" da série temporal
    And eu escolho a opção "<categoria>" no seletor "Categoria" da série temporal
    Then eu visualizo o gráfico da série temporal

    Examples:
      | id       | categoria      |
      | TC-ST-10 | Meio utilizado |
      | TC-ST-11 | Motivação      |

  @severity=normal
  Scenario Outline: <id> Comparar a RA "<ra>" na série temporal
    When eu escolho a opção "<ra>" no seletor "Comparar RAs" da série temporal
    Then eu visualizo o gráfico da série temporal

    Examples:
      | id       | ra          |
      | TC-ST-12 | AGUAS CLARAS |