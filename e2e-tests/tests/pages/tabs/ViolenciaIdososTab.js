const TabsBase = require('./TabsBase.js');

class ViolenciaIdososTab extends TabsBase {

  constructor() {

    super('Violência contra idosos', [
      'Violência contra Idosos'
    ]);
  }

}

module.exports = new ViolenciaIdososTab();
