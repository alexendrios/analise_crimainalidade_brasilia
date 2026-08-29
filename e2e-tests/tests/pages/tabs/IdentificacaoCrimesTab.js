const { I } = inject();

const TabsBase = require('./TabsBase.js');

const aguardar = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

class IdentificacaoCrimesTab extends TabsBase {

  constructor() {

    super('Identificação crimes', [
      'Idades — Vítima × Autor (suspeito)',
      'Resumo'
    ]);

    this.sliderBins = 'Largura dos bins (anos)';
  }

  verSeletor(titulo) {

    I.waitForText(titulo, 45);
  }

  verGraficoIdentificacao() {

    // O Streamlit mantém no DOM o conteúdo de todas as abas (painéis
    // ocultos); o filtro :visible garante que só os gráficos da aba ativa
    // (visível) sejam considerados.
    I.waitForElement(
      '[data-testid="stPlotlyChart"]:visible', 45
    );

    I.seeElement(
      '[data-testid="stPlotlyChart"]:visible'
    );
  }

  verResumo() {

    I.see('Resumo');

    I.waitForElement(
      '[data-testid="stDataFrame"]:visible', 45
    );
  }

  async _lerValorBins() {

    // O polegar do slider é um input[type="range"] genuíno (aria-label =
    // rótulo do widget); lê o valor atual pelo atributo, sem depender de
    // clique/arraste (que são interceptados por overlays do react-aria).
    return I.executeScript((label) => {
      const inputs =
        [...document.querySelectorAll('input[type="range"]')];

      const alvo = inputs.find(
        (input) => input.getAttribute('aria-label') === label
      );

      return alvo ? parseInt(alvo.value, 10) : null;
    }, this.sliderBins);
  }

  async _focarBins() {

    await I.executeScript((label) => {
      const inputs =
        [...document.querySelectorAll('input[type="range"]')];

      const alvo = inputs.find(
        (input) => input.getAttribute('aria-label') === label
      );

      if (alvo) alvo.focus();
    }, this.sliderBins);
  }

  async alterarLarguraBins(valor) {

    await this.aguardarFimProcessamento();

    await I.waitForElement(
      `[data-testid="stSlider"]:visible` +
      `:has-text("${this.sliderBins}")`,
      45
    );

    // Foca o polegar e move com as setas do teclado (passos de 1 unidade;
    // o default do widget é 5).
    await this._focarBins();

    await aguardar(500);

    const atual = await this._lerValorBins();

    if (atual === null) {
      throw new Error(
        `Slider "${this.sliderBins}" não encontrado na aba ativa.`
      );
    }

    const alvo = parseInt(valor, 10);

    const tecla =
      alvo > atual ? 'ArrowRight' : 'ArrowLeft';

    for (let i = 0; i < Math.abs(alvo - atual); i++) {

      await I.pressKey(tecla);

      await aguardar(80);
    }

    await this.aguardarFimProcessamento();
  }

  async verLarguraBins(valor) {

    const alvo = parseInt(valor, 10);

    // Poll sem-throw: o Streamlit re-executa o script após a interação com
    // o slider; aguarda o valor refletir o alvo.
    let atual = null;
    const inicio = Date.now();

    while (Date.now() - inicio < 15000) {

      atual = await this._lerValorBins();

      if (atual === alvo) return;

      await aguardar(400);
    }

    throw new Error(
      `Largura dos bins não atingiu o valor ${alvo} (obtido: ${atual})`
    );
  }

}

module.exports = new IdentificacaoCrimesTab();