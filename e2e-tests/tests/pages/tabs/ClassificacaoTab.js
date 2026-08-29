const TabsBase = require('./TabsBase.js');

class ClassificacaoTab extends TabsBase {

  constructor() {

    super('Classificação', [
      'Classificação — Criminalidade Letal por RA (Regressão Logística)',
      'Avaliação do modelo'
    ]);
  }

}

module.exports = new ClassificacaoTab();
