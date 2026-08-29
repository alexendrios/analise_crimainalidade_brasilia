const TabsBase = require('./TabsBase.js');

class PrevisoesTab extends TabsBase {

  constructor() {

    super('Previsões', [
      'Previsão — Crimes contra a Mulher (Prophet + XGBoost)',
      'Modelos persistidos'
    ]);
  }

}

module.exports = new PrevisoesTab();
