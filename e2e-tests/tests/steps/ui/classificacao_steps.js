const { I, classificacaoTab } = inject();

Then('eu visualizo o seletor {string} na base de classificação', (titulo) => {

  classificacaoTab.verSeletor(titulo);
});

Then('eu visualizo as métricas do modelo na classificação', () => {

  classificacaoTab.verMetricasModelo();
});

Then(
  'eu visualizo o ranking de criminalidade letal do ano {string}',
  (ano) => {

    classificacaoTab.verGraficoRanking(ano);
  }
);

Then('eu visualizo o mapa de calor na classificação', () => {

  classificacaoTab.verHeatmap();
});

Then('eu visualizo as classificações por RA e ano na classificação', () => {

  classificacaoTab.verClassificacoes();
});

Then('eu visualizo a avaliação do modelo na classificação', () => {

  classificacaoTab.verAvaliacaoModelo();
});

Then('eu visualizo os odds ratios na classificação', () => {

  classificacaoTab.verOddsRatios();
});

Then('eu visualizo a matriz de confusão na classificação', () => {

  classificacaoTab.verMatrizConfusao();
});

When('eu seleciono o ano {string} no ranking da classificação', async (ano) => {

  await classificacaoTab.selecionarAnoRanking(ano);
});

Then('o ranking reflete o ano {string} na classificação', (ano) => {

  classificacaoTab.verGraficoRanking(ano);
});