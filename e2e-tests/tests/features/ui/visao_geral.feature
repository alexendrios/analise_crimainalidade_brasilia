# charset: UTF-8
@ui @regression
Feature: Aba Visão Geral do Dashboard
  Eu como usuário gostaria de
  explorar os indicadores e estatísticas da aba
  Visão Geral para consultar a criminalidade consolidada.

  Background:
    Given que eu acesso o dashboard de criminalidade
    When eu ativo a aba "Visão Geral"

  @severity=normal
  Scenario Outline: <id> Visualizar a métrica "<métrica>" da Visão Geral
    Then eu visualizo a métrica "<métrica>"

    Examples:
      | id       | métrica         |
      | TC-VG-01 | Período coberto |
      | TC-VG-02 | RA mais crítica |
      | TC-VG-03 | RAs monitoradas |

  @severity=normal
  Scenario Outline: <id> Visualizar a legenda "<fragmento>" na Visão Geral
    Then eu visualizo a legenda "<fragmento>"

    Examples:
      | id       | fragmento  |
      | TC-VG-04 | Tabela:    |
      | TC-VG-05 | Indicador: |

  @severity=normal
  Scenario Outline: <id> Visualizar a métrica descritiva "<métrica>" da Visão Geral
    Then eu visualizo o título da seção "Estatísticas descritivas"
    And eu visualizo a métrica "<métrica>"

    Examples:
      | id       | métrica       |
      | TC-VG-06 | Média         |
      | TC-VG-07 | Mediana       |
      | TC-VG-08 | Mínimo        |
      | TC-VG-09 | Máximo        |
      | TC-VG-10 | Desvio padrão |

  @severity=normal
  Scenario Outline: <id> Alterar a tabela de crimes para "<tabela>"
    When eu seleciono a opção "<tabela>" no seletor "Crimes"
    Then eu visualizo a legenda "Tabela: <tabela>"

    Examples:
      | id       | tabela                            |
      | TC-VG-11 | Crimes patrimoniais (roubo/furto) |
      | TC-VG-12 | Crimes letais                     |