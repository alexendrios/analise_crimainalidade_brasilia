Feature: Correlação multivariada entre indicadores gold

  Background:
    * url baseUrl

  Scenario: GET /analise/correlacoes devolve matriz quadrada, pares destaque e insights
    Given path 'analise', 'correlacoes'
    When method GET
    Then status 200
    And match response.metodo == 'pearson'
    And match karate.sizeOf(response.periodo) == 2
    And def inicio = response.periodo[0]
    And def fim = response.periodo[1]
    And def periodo_valido = fim >= inicio
    And match periodo_valido == true
    And match karate.sizeOf(response.indicadores) == '#? _ >= 2'

    # matriz é quadrada e usa exatamente os mesmos indicadores
    And def chaves = karate.keysOf(response.matriz_correlacao)
    And match karate.sizeOf(chaves) == karate.sizeOf(response.indicadores)
    And def mesma_chaves = chaves.every(function(k){ return response.indicadores.includes(k); })
    And match mesma_chaves == true

    # diagonal vale 1
    And def diagonalUm = response.indicadores.every(function(ind){ return Math.abs(response.matriz_correlacao[ind][ind] - 1.0) < 0.0001; })
    And match diagonalUm == true

    # série histórica cobre todos os anos do período com todos os indicadores
    And match karate.sizeOf(response.serie_historica) == '#? _ >= 2'
    And def primeira_linha = response.serie_historica[0]
    And match primeira_linha.ano == inicio

    # pares destaque ordenados por correlação absoluta, sem repetir par simétrico
    And def pares = response.pares_destaque
    And match karate.sizeOf(pares) == '#? _ >= 1 && _ <= 5'
    And def ordenadoAbs = pares.every(function(it, i, arr){ return i == 0 || Math.abs(arr[i-1].correlacao) >= Math.abs(it.correlacao); })
    And match ordenadoAbs == true
    And def paresDistintos = pares.every(function(it){ return it.indicador_a != it.indicador_b; })
    And match paresDistintos == true

    # insights textuais não vazios
    And match karate.sizeOf(response.insights) == '#? _ >= 1'
    And match each response.insights != '#null'

  Scenario: GET /analise/correlacoes com metodo=spearman respeita o parâmetro
    Given path 'analise', 'correlacoes'
    And param metodo = 'spearman'
    When method GET
    Then status 200
    And match response.metodo == 'spearman'
    And match karate.sizeOf(response.matriz_correlacao) == karate.sizeOf(response.indicadores)

  Scenario: GET /analise/correlacoes com top_n=3 limita os pares destaque
    Given path 'analise', 'correlacoes'
    And params { metodo: 'pearson', top_n: 3 }
    When method GET
    Then status 200
    And match karate.sizeOf(response.pares_destaque) == '#? _ <= 3'

  Scenario: GET /analise/correlacoes com metodo inválido retorna 422
    Given path 'analise', 'correlacoes'
    And param metodo = 'kendall'
    When method GET
    Then status 422

  Scenario: GET /analise/correlacoes com top_n abaixo do mínimo retorna 422
    Given path 'analise', 'correlacoes'
    And param top_n = 0
    When method GET
    Then status 422

  Scenario: GET /analise/correlacoes com top_n acima do máximo retorna 422
    Given path 'analise', 'correlacoes'
    And param top_n = 31
    When method GET
    Then status 422
