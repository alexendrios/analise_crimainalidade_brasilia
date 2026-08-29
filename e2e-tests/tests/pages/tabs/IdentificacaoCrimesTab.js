const TabsBase = require('./TabsBase.js');

class IdentificacaoCrimesTab extends TabsBase {

  constructor() {

    super('Identificação crimes', [
      'Idades — Vítima × Autor (suspeito)',
      'Resumo'
    ]);
  }

}

module.exports = new IdentificacaoCrimesTab();
