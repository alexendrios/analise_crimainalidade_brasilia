const { I, manchaCriminalTab } = inject();

When(
  'eu escolho a opção {string} no seletor {string} da mancha criminal',
  async (opcao, seletor) => {

    await manchaCriminalTab.selecionarOpcao(seletor, opcao);
  }
);

Then('eu visualizo o seletor {string} na mancha criminal', (titulo) => {

  manchaCriminalTab.verSeletor(titulo);
});

Then('eu visualizo o gráfico da mancha criminal', () => {

  manchaCriminalTab.verGraficoManchaCriminal();
});

Then('eu visualizo as RAs mais críticas da mancha criminal', () => {

  manchaCriminalTab.verRasMaisCriticas();
});