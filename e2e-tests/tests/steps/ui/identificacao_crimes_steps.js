const { I, identificacaoCrimesTab } = inject();

Then('eu visualizo o seletor {string} na identificação', (titulo) => {

  identificacaoCrimesTab.verSeletor(titulo);
});

Then('eu visualizo o gráfico da identificação', () => {

  identificacaoCrimesTab.verGraficoIdentificacao();
});

Then('eu visualizo o resumo da identificação', () => {

  identificacaoCrimesTab.verResumo();
});

When(
  'eu altero a largura dos bins para {string} na identificação',
  async (bins) => {

    await identificacaoCrimesTab.alterarLarguraBins(bins);
  }
);

Then(
  'o valor da largura dos bins é {string} na identificação',
  async (bins) => {

    await identificacaoCrimesTab.verLarguraBins(bins);
  }
);