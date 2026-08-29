const TabsBase = require('./TabsBase.js');

class DesaparecidosTab extends TabsBase {

  constructor() {

    super('Desaparecidos', [
      'Desaparecidos'
    ]);
  }

}

module.exports = new DesaparecidosTab();
