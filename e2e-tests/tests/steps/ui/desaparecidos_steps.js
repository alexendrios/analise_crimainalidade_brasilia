const { I, desaparecidosTab } = inject();

Then('eu visualizo o subcabeçalho {string} na base de desaparecidos', (texto) => {

  desaparecidosTab.verSubcabecalho(texto);
});

Then('eu visualizo o gráfico {string} na base de desaparecidos', (titulo) => {

  desaparecidosTab.verGrafico(titulo);
});

Then(
  'eu visualizo a categoria {string} no gráfico {string} na base de desaparecidos',
  (categoria, titulo) => {

    desaparecidosTab.verCategoria(categoria, titulo);
  }
);