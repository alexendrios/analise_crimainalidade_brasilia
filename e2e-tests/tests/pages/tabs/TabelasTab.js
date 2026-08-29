const TabsBase = require('./TabsBase.js');

class TabelasTab extends TabsBase {

  constructor() {

    super('Tabelas', [
      'Explorar Tabelas Gold'
    ]);
  }

}

module.exports = new TabelasTab();
