const { I } = inject();

const TabsBase = require('./TabsBase.js');

class TabelasTab extends TabsBase {

  constructor() {

    super('Tabelas', [
      'Explorar Tabelas Gold'
    ]);

    this.selectCrimes = 'Crimes';

    this.selectRegiao =
      'Região Administrativa';

    this.sliderIntervaloAnos =
      'Intervalo de anos';

    this.metricasResumo = [
      'Linhas',
      'Colunas',
      'Valores nulos'
    ];
  }

  verSelectboxCrimes() {

    I.waitForText(this.selectCrimes, 45);
  }

  verWidgetsFiltro() {

    I.waitForText(this.selectRegiao, 45);
    I.waitForText(this.sliderIntervaloAnos, 45);

    for (const metrica of this.metricasResumo) {

      I.waitForText(metrica, 45);
    }
  }

}

module.exports = new TabelasTab();
