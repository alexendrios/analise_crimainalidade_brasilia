const { I } = inject();

const TabsBase = require('./TabsBase.js');

const aguardar = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

class PrevisoesTab extends TabsBase {

  constructor() {

    super('Previsões', [
      'Previsão — Crimes contra a Mulher (Prophet + XGBoost)',
      'Modelos persistidos'
    ]);

    this.sliderHorizonte = 'Horizonte (anos)';

    this.colunaAlvo = 'crimes_contra_mulher';

    this.metricas = [
      'Origem',
      'Fonte do modelo',
      'MAE',
      'RMSE'
    ];
  }

  verSeletor(titulo) {

    I.waitForText(titulo, 45);
  }

  verSliderHorizonte() {

    I.waitForText(this.sliderHorizonte, 45);
  }

  verMetricasModelo() {

    for (const metrica of this.metricas) {

      I.waitForText(metrica, 45);
    }
  }

  verGraficoPrevisao(horizonte) {

    // O título do gráfico embute o horizonte usado na reexecução;
    // aguardar o título confirma que a API respondeu o novo horizonte.
    const titulo =
      `Previsão de ${this.colunaAlvo} (${horizonte} anos à frente)`;

    I.waitForElement(
      '[data-testid="stPlotlyChart"]:visible', 45
    );

    I.waitForText(titulo, 45);
  }

  verTabelaPrevisao() {

    // Dataframe com os pontos previstos (ano, valor, componente e resíduo).
    I.waitForElement(
      '[data-testid="stDataFrame"]:visible', 45
    );
  }

  verModelosPersistidos() {

    I.see('Modelos persistidos');
  }

  async _lerValorHorizonte() {

    return I.executeScript((label) => {
      const inputs =
        [...document.querySelectorAll('input[type="range"]')];

      const alvo = inputs.find(
        (input) => input.getAttribute('aria-label') === label
      );

      return alvo ? parseInt(alvo.value, 10) : null;
    }, this.sliderHorizonte);
  }

  async _focarHorizonte() {

    await I.executeScript((label) => {
      const inputs =
        [...document.querySelectorAll('input[type="range"]')];

      const alvo = inputs.find(
        (input) => input.getAttribute('aria-label') === label
      );

      if (alvo) alvo.focus();
    }, this.sliderHorizonte);
  }

  async alterarHorizonte(valor) {

    await this.aguardarFimProcessamento();

    await I.waitForElement(
      `[data-testid="stSlider"]:visible` +
      `:has-text("${this.sliderHorizonte}")`,
      45
    );

    // Foca o polegar e move com as setas do teclado (passos de 1 unidade;
    // o default do widget é 5).
    await this._focarHorizonte();

    await aguardar(500);

    const atual = await this._lerValorHorizonte();

    if (atual === null) {
      throw new Error(
        `Slider "${this.sliderHorizonte}" não encontrado na aba ativa.`
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

  async verHorizonte(valor) {

    const alvo = parseInt(valor, 10);

    // Poll sem-throw: o Streamlit re-executa o script após a interação com
    // o slider e consulta a API; aguarda o valor refletir o alvo.
    let atual = null;
    const inicio = Date.now();

    while (Date.now() - inicio < 20000) {

      atual = await this._lerValorHorizonte();

      if (atual === alvo) return;

      await aguardar(400);
    }

    throw new Error(
      `Horizonte não atingiu o valor ${alvo} (obtido: ${atual})`
    );
  }

}

module.exports = new PrevisoesTab();
