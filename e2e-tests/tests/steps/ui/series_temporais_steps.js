const { I, seriesTemporaisTab } = inject();

When(
  'eu escolho a opção {string} no seletor {string} da série temporal',
  async (opcao, seletor) => {

    await seriesTemporaisTab.selecionarOpcao(seletor, opcao);
  }
);

Then('eu visualizo o seletor {string}', (titulo) => {

  seriesTemporaisTab.verSeletor(titulo);
});

Then('eu visualizo o gráfico da série temporal', () => {

  seriesTemporaisTab.verGraficoSerieTemporal();
});