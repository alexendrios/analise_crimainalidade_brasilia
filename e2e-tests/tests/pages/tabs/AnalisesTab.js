const { I } = inject();

const TabsBase = require('./TabsBase.js');

class AnalisesTab extends TabsBase {

  constructor() {

    super('Análises', [
      'Análises Executivas',
      'Insights'
    ]);

    this.subAbas = [
      'Correlações',
      'Granger',
      'Anomalias',
      'Zonas Quentes'
    ];

    this.correlacoesSelectMetodo = 'Método';

    this.correlacoesSliderPares = 'Pares destaque';

    this.grangerSliderLag = 'Defasagem máxima (anos)';

    this.grangerCheckboxSignificancia =
      'Somente pares significantes (p < 0,05)';

    this.zonasSliderCelula = 'Tamanho da célula (km)';

    this.zonasSliderTopn = 'Células no ranking';

    this.metricasCorrelacoes = [
      'Período consolidado',
      'Indicadores'
    ];

    this.metricasGranger = [
      'Pares retornados',
      'Pares significantes (total testado)'
    ];
  }

  verSubAbas() {

    for (const subaba of this.subAbas) {

      I.waitForText(subaba, 45);
    }
  }

  async clicarSubAba(nome) {

    // As sub-abas (Correlações, Granger, Anomalias, Zonas Quentes) são abas
    // aninhadas dentro da aba Análises; para que o conteúdo de cada uma seja
    // carregado é necessário ativá-la explicitamente. Aguarda o rótulo da
    // sub-aba existir antes de interagir.
    const subaba = locate('[role="tab"]').withText(nome);

    await I.waitForElement(subaba, 45);

    await I.click(subaba);
  }

  verWidgetsCorrelacoes() {

    I.waitForText(this.correlacoesSelectMetodo, 45);
    I.waitForText(this.correlacoesSliderPares, 45);

    for (const metrica of this.metricasCorrelacoes) {

      I.waitForText(metrica, 45);
    }
  }

  verWidgetsGranger() {

    I.waitForText(this.grangerSliderLag, 45);
    I.waitForText(this.grangerCheckboxSignificancia, 45);

    for (const metrica of this.metricasGranger) {

      I.waitForText(metrica, 45);
    }
  }

  verWidgetsZonasQuentes() {

    I.waitForText(this.zonasSliderCelula, 45);
    I.waitForText(this.zonasSliderTopn, 45);
  }

}

module.exports = new AnalisesTab();
