const { I } = inject();

const BasePage = require('./BasePage.js').BasePage;

class SidebarPage extends BasePage {

  constructor() {
    super();

    this.tituloSidebar =
      'Configuração';

    this.rotuloUrlApi =
      'URL da API';

    this.botaoVerificarConexao =
      locate('button').withText('Verificar conexão');

    this.mensagemConexaoOk =
      'API OK — banco: ok';
  }

  verTituloSidebar() {
    I.see(this.tituloSidebar, this.sidebar);
  }

  verCampoUrlApi() {
    I.see(this.rotuloUrlApi, this.sidebar);
  }

  verBotaoVerificarConexao() {
    I.seeElement(this.botaoVerificarConexao);
  }

  async verificarConexaoComApi() {
    // O health check é uma operação assíncrona (chamada à API); espera o
    // botão ficar clicável antes de acioná-lo.
    await I.waitForClickable(this.botaoVerificarConexao, 15);
    await I.click(this.botaoVerificarConexao);
  }

  async verMensagemConexaoOk() {
    // A resposta de saudação da API chega de forma assíncrona após o clique.
    await I.waitForText(this.mensagemConexaoOk, 15);
  }

}

module.exports = new SidebarPage();
