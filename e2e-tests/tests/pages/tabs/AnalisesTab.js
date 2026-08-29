const { I } = inject();

const TabsBase = require('./TabsBase.js');

const aguardar = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

class AnalisesTab extends TabsBase {

  constructor() {

    super('Análises', [
      'Análises Executivas',
      'Insights'
    ]);

    this.subAbas = [
      'Correlações',
      'Granger',
      'Anomalias',
      'Zonas Quentes'
    ];

    this.correlacoesSelectMetodo = 'Método';

    this.correlacoesSliderPares = 'Pares destaque';

    this.grangerSliderLag = 'Defasagem máxima (anos)';

    this.grangerCheckboxSignificancia =
      'Somente pares significantes (p < 0,05)';

    this.zonasSliderCelula = 'Tamanho da célula (km)';

    this.zonasSliderTopn = 'Células no ranking';

    this.metricasCorrelacoes = [
      'Período consolidado',
      'Indicadores'
    ];

    this.metricasGranger = [
      'Pares retornados',
      'Pares significantes (total testado)'
    ];

    this.metricasZonasQuentes = [
      'Ano de referência',
      'Células com ocorrências'
    ];
  }

  verSubAbas() {

    for (const subaba of this.subAbas) {

      I.waitForText(subaba, 45);
    }
  }

  async clicarSubAba(nome) {

    // As sub-abas (Correlações, Granger, Anomalias, Zonas Quentes) são abas
    // aninhadas dentro da aba Análises; para que o conteúdo de cada uma seja
    // carregado é necessário ativá-la explicitamente. Aguarda o rótulo da
    // sub-aba existir antes de interagir.
    const subaba = locate('[role="tab"]').withText(nome);

    await I.waitForElement(subaba, 45);

    await I.click(subaba);
  }

  verSeletor(titulo) {

    I.waitForText(titulo, 45);
  }

  verWidgetsCorrelacoes() {

    I.waitForText(this.correlacoesSelectMetodo, 45);
    I.waitForText(this.correlacoesSliderPares, 45);

    for (const metrica of this.metricasCorrelacoes) {

      I.waitForText(metrica, 45);
    }
  }

  verWidgetsGranger() {

    I.waitForText(this.grangerSliderLag, 45);
    I.waitForText(this.grangerCheckboxSignificancia, 45);

    for (const metrica of this.metricasGranger) {

      I.waitForText(metrica, 45);
    }
  }

  verWidgetsZonasQuentes() {

    I.waitForText(this.zonasSliderCelula, 45);
    I.waitForText(this.zonasSliderTopn, 45);
  }

  verMetricasCorrelacoes() {

    for (const metrica of this.metricasCorrelacoes) {

      I.waitForText(metrica, 45);
    }
  }

  verHeatmapCorrelacoes(metodo) {

    // O título do mapa de calor embute o método selecionado.
    I.waitForText(
      `Correlação entre indicadores — ${metodo}`, 45
    );
  }

  verParesCorrelacionados() {

    I.waitForText(
      'Pares de indicadores mais correlacionados', 45
    );
  }

  verMetricasGranger() {

    for (const metrica of this.metricasGranger) {

      I.waitForText(metrica, 45);
    }
  }

  verGraficoGranger(lag) {

    // O título do gráfico de Granger embute a defasagem selecionada.
    I.waitForText(
      `Força da causalidade de Granger (max_lag = ${lag})`, 45
    );
  }

  verMetricasZonasQuentes() {

    for (const metrica of this.metricasZonasQuentes) {

      I.waitForText(metrica, 45);
    }
  }

  verGraficoZonas() {

    I.waitForText(
      'Zonas quentes — células com mais ocorrências patrimoniais', 45
    );
  }

  verDataframeZonas() {

    I.waitForElement(
      '[data-testid="stDataFrame"]:visible', 45
    );
  }

  async _selecionarOpcaoCombobox(seletor, opcao) {

    // Combobox do Streamlit (react-aria): clicar no <input> abre o dropdown
    // e as opções aparecem como li[role="option"]. O filtro :visible isola
    // os controles da sub-aba ativa.
    const combobox =
      `[data-testid="stSelectbox"]:visible` +
      `:has-text("${seletor}") input`;

    await this.aguardarFimProcessamento();

    await I.waitForElement(combobox, 45);

    await I.click(combobox);

    const opcaoItem =
      `[role="option"]:visible:has-text("${opcao}")`;

    for (let tentativa = 0; tentativa < 5; tentativa++) {

      await I.click(combobox);

      // Poll sem-throw: um helper que lança envenena o recorder do
      // CodeceptJS; grabNumberOfVisibleElements permite o retry de verdade.
      let achou = false;
      const inicio = Date.now();
      while (Date.now() - inicio < 12000) {
        const visiveis =
          await I.grabNumberOfVisibleElements(opcaoItem);
        if (visiveis > 0) {
          achou = true;
          break;
        }
        await aguardar(500);
      }
      if (achou) break;

      await I.pressKey('Escape');
      await aguardar(3000);
    }

    await I.click(opcaoItem);

    await I.waitForElement('[data-testid="stApp"]');
  }

  async selecionarMetodoCorrelacao(metodo) {

    await this._selecionarOpcaoCombobox(
      this.correlacoesSelectMetodo, metodo
    );
  }

  async _lerValorRange(label) {

    return I.executeScript((rotulo) => {
      const inputs =
        [...document.querySelectorAll('input[type="range"]')];

      const alvo = inputs.find(
        (input) => input.getAttribute('aria-label') === rotulo
      );

      return alvo ? parseInt(alvo.value, 10) : null;
    }, label);
  }

  async _focarRange(label) {

    await I.executeScript((rotulo) => {
      const inputs =
        [...document.querySelectorAll('input[type="range"]')];

      const alvo = inputs.find(
        (input) => input.getAttribute('aria-label') === rotulo
      );

      if (alvo) alvo.focus();
    }, label);
  }

  async alterarRange(label, valor) {

    await this.aguardarFimProcessamento();

    await I.waitForElement(
      `[data-testid="stSlider"]:visible` +
      `:has-text("${label}")`,
      45
    );

    await this._focarRange(label);

    await aguardar(500);

    const atual = await this._lerValorRange(label);

    if (atual === null) {
      throw new Error(
        `Slider "${label}" não encontrado na sub-aba ativa.`
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

  async verValorRange(label, valor) {

    const alvo = parseInt(valor, 10);

    let atual = null;
    const inicio = Date.now();

    while (Date.now() - inicio < 20000) {

      atual = await this._lerValorRange(label);

      if (atual === alvo) return;

      await aguardar(400);
    }

    throw new Error(
      `Slider "${label}" não atingiu o valor ${alvo} (obtido: ${atual})`
    );
  }

  async alterarDefasagemMaxima(lag) {

    await this.alterarRange(this.grangerSliderLag, lag);
  }

  async verDefasagemMaxima(lag) {

    await this.verValorRange(this.grangerSliderLag, lag);
  }

  async alterarCelulasRanking(valor) {

    await this.alterarRange(this.zonasSliderTopn, valor);
  }

  async verCelulasRanking(valor) {

    await this.verValorRange(this.zonasSliderTopn, valor);
  }

  async desmarcarFiltroSignificancia() {

    await this.aguardarFimProcessamento();

    // O checkbox do Streamlit é um contêiner clicável com um input
    // escondido; clicar no contêiner alterna o estado (marcado por padrão).
    const checkbox =
      `[data-testid="stCheckbox"]:visible` +
      `:has-text("Somente pares significantes")`;

    await I.waitForElement(checkbox, 45);

    await I.click(checkbox);

    await this.aguardarFimProcessamento();
  }

}

module.exports = new AnalisesTab();