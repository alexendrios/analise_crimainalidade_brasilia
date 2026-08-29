const { I, previsoesTab } = inject();

Then('eu visualizo o seletor {string} na base de previsões', (titulo) => {

  previsoesTab.verSeletor(titulo);
});

Then('eu visualizo as métricas do modelo na previsão', () => {

  previsoesTab.verMetricasModelo();
});

Then(
  'eu visualizo o gráfico da previsão com o horizonte {string}',
  (horizonte) => {

    previsoesTab.verGraficoPrevisao(horizonte);
  }
);

Then('eu visualizo a tabela da previsão', () => {

  previsoesTab.verTabelaPrevisao();
});

Then('eu visualizo os modelos persistidos na previsão', () => {

  previsoesTab.verModelosPersistidos();
});

When('eu altero o horizonte para {string} anos na previsão', async (horiz) => {

  await previsoesTab.alterarHorizonte(horiz);
});

Then(
  'o valor do horizonte é {string} anos na previsão',
  async (horiz) => {

    await previsoesTab.verHorizonte(horiz);
  }
);