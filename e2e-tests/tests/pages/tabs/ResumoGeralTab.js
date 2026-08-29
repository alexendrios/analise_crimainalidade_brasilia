const { I } = inject();

const TabsBase = require('./TabsBase.js');

class ResumoGeralTab extends TabsBase {

  constructor() {

    super('Resumo Geral', [
      'Resumo Geral (IA)'
    ]);

    this.textUrlOllama = 'URL do Ollama';

    this.selectModelo = 'Modelo';

    this.botaoGerarResumo = 'Gerar resumo com IA';
  }

  verTextUrlOllama() {

    I.waitForText(this.textUrlOllama, 45);
  }

  verSelectboxModelo() {

    I.waitForText(this.selectModelo, 45);
  }

  verBotaoGerarResumo() {

    I.waitForElement(locate('button').withText(this.botaoGerarResumo), 45);
  }

}

module.exports = new ResumoGeralTab();
