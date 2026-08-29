const { I } = inject();

const TabsBase = require('./TabsBase.js');

class VisaoGeralTab extends TabsBase {

  constructor() {

    super('Visão Geral', [
      'Visão Geral',
      'Estatísticas descritivas'
    ]);

    this.selectCriminalidade = 'Crimes';

    this.selectIndicador = 'Indicador';

    this.metricasDescritivas = [
      'Média',
      'Mediana',
      'Mínimo',
      'Máximo',
      'Desvio padrão'
    ];
  }

  verSelectboxCrimes() {

    I.waitForText(this.selectCriminalidade, 45);
  }

  verSelectboxIndicador() {

    I.waitForText(this.selectIndicador, 45);
  }

  verMetricasDescritivas() {

    for (const metrica of this.metricasDescritivas) {

      I.waitForText(metrica, 45);
    }
  }

}

module.exports = new VisaoGeralTab();
