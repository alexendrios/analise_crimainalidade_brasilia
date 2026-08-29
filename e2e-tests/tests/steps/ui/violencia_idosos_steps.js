const { I, violenciaIdososTab } = inject();

Then('eu visualizo o subcabeçalho {string} na base de violência contra idosos', (texto) => {

  violenciaIdososTab.verSubcabecalho(texto);
});

Then('eu visualizo o gráfico {string} na base de violência contra idosos', (titulo) => {

  violenciaIdososTab.verGrafico(titulo);
});

Then(
  'eu visualizo a categoria {string} no gráfico {string} na base de violência contra idosos',
  (categoria, titulo) => {

    violenciaIdososTab.verCategoria(categoria, titulo);
  }
);