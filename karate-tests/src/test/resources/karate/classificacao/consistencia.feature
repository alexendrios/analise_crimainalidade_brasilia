Feature: Consistência e campos obrigatórios da classificação de criminalidade

  Background:
    * url baseUrl

  Scenario: classificação devolve campos obrigatórios com tipos corretos
    Given path 'classificacao', 'criminalidade-letal'
    And param usar_cache = true
    When method GET
    Then status 200
    And match response.gerado_em != null
    And match response.cache_ate != null
    And match response.modelo_arquivo != null
    And match response.modelo_arquivo contains 'logreg'
    And match response.fonte_modelo != null

  Scenario: distribuição real contém apenas chaves alta e baixa
    Given path 'classificacao', 'criminalidade-letal'
    And param usar_cache = true
    When method GET
    Then status 200
    And def chaves = karate.keysOf(response.distribuicao_real)
    And match chaves contains 'alta'
    And match chaves contains 'baixa'
    And match karate.sizeOf(chaves) == 2

  Scenario: odds_ratios contém exatamente 5 coeficientes
    Given path 'classificacao', 'criminalidade-letal'
    And param usar_cache = true
    When method GET
    Then status 200
    And def chavesOr = karate.keysOf(response.odds_ratios)
    And match karate.sizeOf(chavesOr) == 5
    And match chavesOr contains 'taxa_homicidio'
    And match chavesOr contains 'taxa_latrocinio'
    And match chavesOr contains 'taxa_lesao_morte'
    And match chavesOr contains 'log_populacao'
    And match chavesOr contains 'ano_num'

  Scenario: todas as classificações são ordenadas por probabilidade decrescente
    Given path 'classificacao', 'criminalidade-letal'
    And param usar_cache = true
    When method GET
    Then status 200
    And def classes = response.classificacoes
    And def total = classes.length
    And def ordenado = classes.every(function(it, i, arr){ return i == 0 || arr[i-1].probabilidade_alta >= it.probabilidade_alta; })
    And match ordenado == true

  Scenario: cada classificação tem campo rotulo_previsto consistente com classe_prevista
    Given path 'classificacao', 'criminalidade-letal'
    And param usar_cache = true
    When method GET
    Then status 200
    And def classes = response.classificacoes
    # classe_prevista=1 → rotulo 'alta', classe_prevista=0 → rotulo 'baixa'
    And def consistente = classes.every(function(it){ return (it.classe_prevista == 1 && it.rotulo_previsto == 'alta') || (it.classe_prevista == 0 && it.rotulo_previsto == 'baixa'); })
    And match consistente == true

  Scenario: total_registros == total_ras * anos_no_período
    Given path 'classificacao', 'criminalidade-letal'
    And param usar_cache = true
    When method GET
    Then status 200
    And def anos = response.periodo[1] - response.periodo[0] + 1
    And def esperado = response.total_ras * anos
    And match response.total_registros == esperado

  Scenario: matriz_confusão tem pelo menos 1 registro classificado
    Given path 'classificacao', 'criminalidade-letal'
    And param usar_cache = true
    When method GET
    Then status 200
    And def mc = response.matriz_confusao
    And def somaMc = mc[0][0] + mc[0][1] + mc[1][0] + mc[1][1]
    # holdout é um subconjunto — deve ter ao menos 1 registro
    And match somaMc == '#? _ >= 1'
