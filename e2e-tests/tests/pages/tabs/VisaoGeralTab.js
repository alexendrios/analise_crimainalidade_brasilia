const TabsBase = require('./TabsBase.js');

class VisaoGeralTab extends TabsBase {

  constructor() {

    super('Visão Geral', [
      'Visão Geral',
      'Estatísticas descritivas'
    ]);
  }

}

module.exports = new VisaoGeralTab();
