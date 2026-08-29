const { I } = inject();

const TabsBase = require('./TabsBase.js');

const aguardar = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

class SeriesTemporaisTab extends TabsBase {

  constructor() {

    super('Séries Temporais', [
      'Séries Temporais'
    ]);

    this.selectCrimes = 'Crimes';

    this.selectModoAnalise = 'Modo de análise';

    this.selectColuna = 'Coluna (indicador)';

    this.selectCategoria = 'Categoria';

    this.multiselectRas = 'Comparar RAs';

    this.sliderMediaMovel =
      'Média móvel (janela, 1 = desativada)';
  }

  verSeletor(titulo) {

    I.waitForText(titulo, 45);
  }

  verWidgets() {

    I.waitForText(this.selectModoAnalise, 45);
    I.waitForText(this.selectColuna, 45);
    I.waitForText(this.multiselectRas, 45);
    I.waitForText(this.sliderMediaMovel, 45);
  }

  verGraficoSerieTemporal() {

    // O Streamlit mantém no DOM o conteúdo de todas as abas (painéis
    // ocultos); o filtro :visible garante que só o gráfico da aba ativa
    // (visível) seja considerado.
    I.waitForElement(
      '[data-testid="stPlotlyChart"]:visible', 45
    );

    I.seeElement(
      '[data-testid="stPlotlyChart"]:visible'
    );
  }

  async selecionarOpcao(seletor, opcao) {

    // Os seletores da aba Séries Temporais (selectboxes e o multiselect de
    // RAs) são comboboxes do Streamlit (react-aria): clicar no <input> abre
    // o dropdown e as opções aparecem como li[role="option"]. Como o DOM
    // mantém os painéis das abas inativas, o filtro :visible restringe a
    // interação aos controles da aba ativa.
    const testid =
      seletor === this.multiselectRas
        ? 'stMultiSelect'
        : 'stSelectbox';

    const combobox =
      `[data-testid="${testid}"]:visible` +
      `:has-text("${seletor}") input`;

    await this.aguardarFimProcessamento();

    await I.waitForElement(combobox, 45);

    await I.click(combobox);

    const opcaoItem =
      `[role="option"]:visible:has-text("${opcao}")`;

    for (let tentativa = 0; tentativa < 5; tentativa++) {

      await I.click(combobox);

      // Poll sem-throw: um helper que lança (ex.: waitForElement) envenena
      // o recorder do CodeceptJS e aborta o cenário mesmo que a opção
      // apareça numa tentativa posterior. grabNumberOfVisibleElements
      // retorna uma contagem e permite o retry de verdade.
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

    // A seleção dispara nova execução do script da aba; aguarda o
    // "estado limpo" do widget antes de prosseguir.
    await I.waitForElement('[data-testid="stApp"]');
  }

}

module.exports = new SeriesTemporaisTab();