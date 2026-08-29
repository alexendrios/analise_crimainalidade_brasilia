const { I } = inject();

const TabsBase = require('./TabsBase.js');

class ViolenciaIdososTab extends TabsBase {

  constructor() {

    super('Violência contra idosos', [
      'Violência contra idosos — ocorrências por RA (jan–ago)',
      'Violência contra idosos — ocorrências por ano',
      'Violência contra idosos — série mensal',
      'Violência contra idosos — vítimas por sexo'
    ]);
  }

  verSubcabecalho(texto) {

    I.waitForText(texto, 45);
  }

  verGrafico(titulo) {

    // O Streamlit mantém no DOM o conteúdo de todas as abas (painéis
    // ocultos); o filtro :visible garante que só os gráficos da aba ativa
    // sejam considerados e o título (plotly, dentro do SVG) confirma o
    // render do gráfico com dados reais.
    I.waitForElement(
      '[data-testid="stPlotlyChart"]:visible', 45
    );

    I.waitForText(titulo, 45);
  }

  verCategoria(categoria, titulo) {

    // As categorias são renderizadas como rótulos (ticks) ou entradas da
    // legenda dos gráficos de barra da aba ativa.
    I.waitForText(categoria, 45);
  }

}

module.exports = new ViolenciaIdososTab();