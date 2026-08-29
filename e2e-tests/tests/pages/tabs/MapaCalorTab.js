const TabsBase = require('./TabsBase.js');

class MapaCalorTab extends TabsBase {

  constructor() {

    super('Mapa de Calor', [
      'Mapa de Calor por RA'
    ]);
  }

}

module.exports = new MapaCalorTab();
