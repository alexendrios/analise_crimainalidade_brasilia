const { I } = inject();

const TabsBase = require('./TabsBase.js');

const aguardar = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

class ClassificacaoTab extends TabsBase {

  constructor() {

    super('Classificação', [
      'Classificação — Criminalidade Letal por RA (Regressão Logística)',
      'Avaliação do modelo'
    ]);

    this.selectAnoRanking = 'Ano do ranking';

    this.metricas = [
      'Fonte do modelo',
      'Registros analisados',
      'Regiões administrativas',
      'Limiar (taxa/100 mil)'
    ];
  }

  verSeletor(titulo) {

    I.waitForText(titulo, 45);
  }

  verSelectboxAnoRanking() {

    I.waitForText(this.selectAnoRanking, 45);
  }

  verMetricasModelo() {

    for (const metrica of this.metricas) {

      I.waitForText(metrica, 45);
    }
  }

  verGraficoRanking(ano) {

    // O título do ranking embute o ano selecionado; aguardar o título
    // confirma que a seleção foi aplicada.
    const titulo =
      `P(alta criminalidade letal) por RA — ano ${ano}`;

    I.waitForElement(
      '[data-testid="stPlotlyChart"]:visible', 45
    );

    I.waitForText(titulo, 45);
  }

  verHeatmap() {

    I.waitForText(
      'Probabilidade de alta criminalidade letal por RA e ano', 45
    );
  }

  verClassificacoes() {

    I.waitForText('Classificações por RA e ano', 45);

    I.waitForElement(
      '[data-testid="stDataFrame"]:visible', 45
    );
  }

  verAvaliacaoModelo() {

    for (const rotulo of [
      'Avaliação do modelo',
      'CV ROC-AUC',
      'Holdout ROC-AUC',
      'Holdout F1'
    ]) {

      I.waitForText(rotulo, 45);
    }
  }

  verOddsRatios() {

    I.waitForText('Odds ratios (exp coeficientes)', 45);
  }

  verMatrizConfusao() {

    I.waitForText('Matriz de confusão', 45);
  }

  async selecionarOpcao(seletor, opcao) {

    // O seletor "Ano do ranking" é um combobox do Streamlit (react-aria):
    // clicar no <input> abre o dropdown e as opções aparecem como
    // li[role="option"]. Como o DOM mantém os painéis das abas inativas,
    // o filtro :visible restringe a interação aos controles da aba ativa.
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

  async selecionarAnoRanking(ano) {

    await this.selecionarOpcao(this.selectAnoRanking, ano);
  }

}

module.exports = new ClassificacaoTab();