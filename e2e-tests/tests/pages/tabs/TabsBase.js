const { I } = inject();

const BasePage = require('../BasePage.js').BasePage;

const aguardar = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

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

async aguardarFimProcessamento() {

    // Após uma interação o Streamlit re-executa o script da aba; interagir
    // com os comboboxes nesse ínterim abre dropdowns obsoletos ou parciais.
    // Uma pausa curta reduz essa janela (a robustez fica no retry do
    // selecionarOpcao).
    await aguardar(1500);

    await I.waitForElement('[data-testid="stApp"]');
  }

}

module.exports = TabsBase;
