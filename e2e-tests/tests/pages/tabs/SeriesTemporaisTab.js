const TabsBase = require('./TabsBase.js');

class SeriesTemporaisTab extends TabsBase {

  constructor() {

    super('Séries Temporais', [
      'Séries Temporais'
    ]);
  }

}

module.exports = new SeriesTemporaisTab();
