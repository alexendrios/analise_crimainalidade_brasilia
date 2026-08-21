Feature: Classificação de criminalidade letal por Regressão Logística

  Background:
    * url baseUrl

  Scenario: GET /classificacao/criminalidade-letal classifica cada RA/ano com métricas e interpretação
    Given path 'classificacao', 'criminalidade-letal'
    And params { usar_cache: true }
    When method GET
    Then status 200
    And match response.tabelas_origem contains 'crimes_letais_gold'
    And match response.tabelas_origem contains 'populacao_regiao_administrativa'
    And match response.total_registros == '#? _ > 0'
    And match response.total_ras == '#? _ > 0'
    And def inicio = response.periodo[0]
    And def fim = response.periodo[1]
    And match karate.sizeOf(response.periodo) == 2
    And def periodo_valido = fim >= inicio
    And match periodo_valido == true
    And match response.limiar_taxa_mediana == '#? _ > 0'
    And def soma_distribuicao = response.distribuicao_real.alta + response.distribuicao_real.baixa
    And match soma_distribuicao == response.total_registros

    And def m = response.metricas
    And match m.cv_roc_auc_media == '#? _ >= 0 && _ <= 1'
    And match m.cv_roc_auc_std == '#? _ >= 0 && _ <= 1'
    And match m.holdout_accuracy == '#? _ >= 0 && _ <= 1'
    And match m.holdout_precision == '#? _ >= 0 && _ <= 1'
    And match m.holdout_recall == '#? _ >= 0 && _ <= 1'
    And match m.holdout_f1 == '#? _ >= 0 && _ <= 1'
    And match m.holdout_roc_auc == '#? _ >= 0 && _ <= 1'

    And def ors = response.odds_ratios
    And match karate.sizeOf(karate.keysOf(ors)) == 5
    And match ors.taxa_homicidio == '#? _ > 0'
    And match ors.taxa_latrocinio == '#? _ > 0'
    And match ors.taxa_lesao_morte == '#? _ > 0'
    And match ors.log_populacao == '#? _ > 0'
    And match ors.ano_num == '#? _ > 0'

    And def mc = response.matriz_confusao
    And match karate.sizeOf(mc) == 2
    And match karate.sizeOf(mc[0]) == 2
    And match karate.sizeOf(mc[1]) == 2
    And def total_reg = response.total_registros
    And def soma_mc = mc[0][0] + mc[0][1] + mc[1][0] + mc[1][1]
    And match soma_mc == '#? _ > 0 && _ < total_reg'

    And def classes = response.classificacoes
    And match karate.sizeOf(classes) == total_reg
    And match each classes[*].regiao_administrativa != null
    And match each classes[*].ano == '#number'
    And match each classes[*].classe_prevista == '#? _ == 0 || _ == 1'
    And match each classes[*].probabilidade_alta == '#? _ >= 0 && _ <= 1'
    And def consistente = classes.every(function(it){ var esperado = it.classe_prevista == 1 ? 'alta' : 'baixa'; return it.rotulo_previsto == esperado; })
    And match consistente == true
    And def ordenadoDesc = classes.every(function(it, i, arr){ return i == 0 || arr[i-1].probabilidade_alta >= it.probabilidade_alta; })
    And match ordenadoDesc == true
    And match response.fonte_modelo == '#? _ == "artefato" || _ == "retreino"'
    And match response.modelo_arquivo != null
    And match response.gerado_em != null

  Scenario: GET sem cache reprocessa e mantém o contrato da resposta
    Given path 'classificacao', 'criminalidade-letal'
    When method GET
    Then status 200
    And def total_esperado = response.total_registros
    Given path 'classificacao', 'criminalidade-letal'
    And param usar_cache = false
    When method GET
    Then status 200
    And match response.total_registros == total_esperado
    And match karate.sizeOf(response.classificacoes) == total_esperado

  @retreino
  Scenario: POST /classificacao/retrain treina, persiste um novo pipeline e devolve a classificação
    Given path 'classificacao', 'retrain'
    When method POST
    Then status 200
    And match response.fonte_modelo == 'retreino'
    And match response.modelo_arquivo != null
    And match response.modelo_arquivo contains 'logreg_criminalidade_letal_'
    And match response.metricas.holdout_roc_auc == '#? _ >= 0 && _ <= 1'
    And match response.limiar_taxa_mediana == '#? _ > 0'
    And match karate.sizeOf(response.classificacoes) == '#? _ > 0'
