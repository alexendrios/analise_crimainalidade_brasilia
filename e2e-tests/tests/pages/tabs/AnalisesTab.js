const TabsBase = require('./TabsBase.js');

class AnalisesTab extends TabsBase {

  constructor() {

    super('Análises', [
      'Análises Executivas',
      'Insights'
    ]);
  }

}

module.exports = new AnalisesTab();
