const { I } = inject();

const TabsBase = require('./TabsBase.js');

class ClassificacaoTab extends TabsBase {

  constructor() {

    super('Classificação', [
      'Classificação — Criminalidade Letal por RA (Regressão Logística)',
      'Avaliação do modelo'
    ]);

    this.selectAnoRanking = 'Ano do ranking';

    this.metricas = [
      'Fonte do modelo',
      'Registros analisados',
      'Regiões administrativas'
    ];
  }

  verSelectboxAnoRanking() {

    I.waitForText(this.selectAnoRanking, 45);
  }

  verMetricasModelo() {

    for (const metrica of this.metricas) {

      I.waitForText(metrica, 45);
    }
  }

}

module.exports = new ClassificacaoTab();
