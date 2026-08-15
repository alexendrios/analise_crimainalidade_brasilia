package br.com.criminalidade.api;

import com.intuit.karate.junit5.Karate;

/**
 * Runner JUnit5 da suite de testes E2E da API.
 *
 * <p>Executa todos os arquivos .feature em src/test/resources/karate, exceto
 * cenários marcados com @retreino (mais lentos e com efeito colateral: gravam
 * um novo bundle do modelo em models/). Para incluí-los, remova o filtro
 * abaixo ou rode isoladamente:
 *
 * <pre>
 *   mvn test -Dkarate.options="--tags @retreino"
 * </pre>
 */
class RunnerTest {

    @Karate.Test
    Karate executarSuiteE2E() {
        return Karate.run("classpath:karate").tags("~@retreino");
    }
}
