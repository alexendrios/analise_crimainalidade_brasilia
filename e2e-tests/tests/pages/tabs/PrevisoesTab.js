const { I } = inject();

const TabsBase = require('./TabsBase.js');

class PrevisoesTab extends TabsBase {

  constructor() {

    super('Previsões', [
      'Previsão — Crimes contra a Mulher (Prophet + XGBoost)',
      'Modelos persistidos'
    ]);

    this.sliderHorizonte = 'Horizonte (anos)';

    this.metricas = [
      'Origem',
      'Fonte do modelo',
      'MAE',
      'RMSE'
    ];
  }

  verSliderHorizonte() {

    I.waitForText(this.sliderHorizonte, 45);
  }

  verMetricasModelo() {

    for (const metrica of this.metricas) {

      I.waitForText(metrica, 45);
    }
  }

}

module.exports = new PrevisoesTab();
