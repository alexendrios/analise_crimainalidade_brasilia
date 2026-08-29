const { I, analisesTab } = inject();

When('eu ativo a sub-aba {string} na análise', async (nome) => {

  await analisesTab.clicarSubAba(nome);
});

Then('eu visualizo o seletor {string} na base de análises', (titulo) => {

  analisesTab.verSeletor(titulo);
});

Then('eu visualizo as métricas das correlações', () => {

  analisesTab.verMetricasCorrelacoes();
});

Then(
  'eu visualizo o mapa de calor das correlações com o método {string}',
  (metodo) => {

    analisesTab.verHeatmapCorrelacoes(metodo);
  }
);

Then('eu visualizo os pares mais correlacionados', () => {

  analisesTab.verParesCorrelacionados();
});

When('eu seleciono o método {string} nas correlações', async (metodo) => {

  await analisesTab.selecionarMetodoCorrelacao(metodo);
});

Then('eu visualizo as métricas da causalidade de Granger', () => {

  analisesTab.verMetricasGranger();
});

Then(
  'eu visualizo o gráfico da causalidade de Granger com a defasagem {string}',
  (lag) => {

    analisesTab.verGraficoGranger(lag);
  }
);

When(
  'eu altero a defasagem máxima para {string} anos na causalidade de Granger',
  async (lag) => {

    await analisesTab.alterarDefasagemMaxima(lag);
  }
);

Then(
  'o gráfico da causalidade de Granger reflete a defasagem {string}',
  (lag) => {

    analisesTab.verGraficoGranger(lag);
  }
);

When('eu desmarco o filtro de significância na causalidade de Granger', async () => {

  await analisesTab.desmarcarFiltroSignificancia();
});

Then('eu visualizo as métricas das zonas quentes', () => {

  analisesTab.verMetricasZonasQuentes();
});

Then('eu visualizo o gráfico das zonas quentes', () => {

  analisesTab.verGraficoZonas();
});

Then('eu visualizo o dataframe das zonas quentes', () => {

  analisesTab.verDataframeZonas();
});

When('eu altero as células no ranking para {string} nas zonas quentes', async (valor) => {

  await analisesTab.alterarCelulasRanking(valor);
});

Then(
  'o valor das células no ranking é {string} nas zonas quentes',
  async (valor) => {

    await analisesTab.verCelulasRanking(valor);
  }
);