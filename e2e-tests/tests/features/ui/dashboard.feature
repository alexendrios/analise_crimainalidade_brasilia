# charset: UTF-8
@ui @regression
Feature: Dashboard de Criminalidade
  Eu como usuário gostaria de
  acessar o dashboard e assim
  validar o carregamento da aplicação.

  @severity=critical @smoke
  Scenario: TC-DASH-01 Carregar o dashboard e validar o título
    Given que eu acesso o dashboard de criminalidade
    Then eu visualizo o título "Criminalidade em Brasília/DF — Dashboard Analítico"

  @severity=normal @smoke
  Scenario: TC-DASH-02 Visualizar a configuração da API na sidebar
    Given que eu acesso o dashboard de criminalidade
    Then eu visualizo o título "Configuração" na sidebar
    And eu visualizo o campo "URL da API" na sidebar
    And eu visualizo o botão "Verificar conexão" na sidebar

  @severity=normal
  Scenario: TC-DASH-03 Visualizar a aba padrão do dashboard
    Given que eu acesso o dashboard de criminalidade
    Then a aba "Visão Geral" está selecionada
