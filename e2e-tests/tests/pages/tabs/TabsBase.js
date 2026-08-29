const { I } = inject();

const BasePage = require('../BasePage.js').BasePage;

class TabsBase extends BasePage {

  constructor(nomeAba, subheaders) {

    super();

    this.nomeAba = nomeAba;

    this.subheaders = subheaders || [];
  }

  ativar() {

    this.ativarAba(this.nomeAba);
  }

  verAbaSelecionada() {

    this.verificarAbaSelecionada(this.nomeAba);
  }

  verAbaVisivel() {

    this.verificarAbaVisivel(this.nomeAba);
  }

  verSubheaders() {

    for (const cabecalho of this.subheaders) {

      I.see(cabecalho);
    }
  }

}

module.exports = TabsBase;
