const TabsBase = require('./TabsBase.js');

class ResumoGeralTab extends TabsBase {

  constructor() {

    super('Resumo Geral', [
      'Resumo Geral (IA)'
    ]);
  }

}

module.exports = new ResumoGeralTab();
