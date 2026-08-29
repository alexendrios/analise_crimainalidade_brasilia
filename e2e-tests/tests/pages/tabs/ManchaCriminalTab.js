const TabsBase = require('./TabsBase.js');

class ManchaCriminalTab extends TabsBase {

  constructor() {

    super('Mancha Criminal', [
      'Mancha Criminal por RA',
      'RAs mais críticas'
    ]);
  }

}

module.exports = new ManchaCriminalTab();
