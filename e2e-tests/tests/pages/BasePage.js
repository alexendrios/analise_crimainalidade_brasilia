const { I } = inject();

class BasePage {

  constructor() {
    this.tituloDashboard =
      'Criminalidade em Brasília/DF — Dashboard Analítico';

    this.sidebar =
      locate('section[data-testid="stSidebar"]');

    this.aba =
      (nome) => locate('[role="tab"]').withText(nome);
  }

  async abrirAplicacao() {
    await I.amOnPage('/');
    await I.waitForElement('h1');
  }

  verTituloDashboard() {
    I.see(this.tituloDashboard, 'h1');
  }

  verTituloSub(cabecalho) {
    I.see(cabecalho);
  }

  // ==========================================
  // ABAS DO DASHBOARD
  // ==========================================

  async ativarAba(nome) {
    await I.click(this.aba(nome));
    // A renderização do conteúdo da aba dispara chamadas à API e pode
    // demorar; aguarda o "estado limpo" do widget antes de prosseguir.
    await I.waitForElement('[data-testid="stApp"]');
  }

  async verificarAbaSelecionada(nome) {
    await I.seeAttributesOnElements(
      this.aba(nome),
      { 'aria-selected': 'true' }
    );
  }

  async verificarAbaVisivel(nome) {
    await I.see(nome, '[role="tab"]');
  }

}

module.exports = new BasePage();

module.exports.BasePage = BasePage;
