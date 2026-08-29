const { I, mapaCalorTab } = inject();

When(
  'eu escolho a opção {string} no seletor {string} do mapa de calor',
  async (opcao, seletor) => {

    await mapaCalorTab.selecionarOpcao(seletor, opcao);
  }
);

Then('eu visualizo o seletor {string} no mapa de calor', (titulo) => {

  mapaCalorTab.verSeletor(titulo);
});

Then('eu visualizo o gráfico do mapa de calor', () => {

  mapaCalorTab.verGraficoMapaCalor();
});