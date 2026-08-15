function fn() {
    var config = {
        // API local (FastAPI/uvicorn). Ajuste se o servidor subir em outra porta.
        baseUrl: 'http://localhost:8000'
    };

    // Sobrescrita por ambiente, ex.: mvn test -Dkarate.env=hml
    var env = karate.env;
    if (env && env === 'hml') {
        config.baseUrl = 'http://localhost:8001';
    }

    karate.log('Base URL da API: ' + config.baseUrl);
    return config;
}
