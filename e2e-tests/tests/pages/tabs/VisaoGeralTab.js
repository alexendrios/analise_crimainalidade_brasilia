const { I } = inject();

const TabsBase = require('./TabsBase.js');

class VisaoGeralTab extends TabsBase {

  constructor() {

    super('Visão Geral', [
      'Visão Geral',
      'Estatísticas descritivas'
    ]);

    this.selectCriminalidade = 'Crimes';

    this.selectIndicador = 'Indicador';

    this.metricasDescritivas = [
      'Média',
      'Mediana',
      'Mínimo',
      'Máximo',
      'Desvio padrão'
    ];

    this.metricasResumo = [
      'Período coberto',
      'RA mais crítica',
      'RAs monitoradas'
    ];

    this.legendaTabela = 'Tabela:';

    this.legendaIndicador = 'Indicador:';

    this.captionContainer =
      '[data-testid="stCaptionContainer"]';
  }

  verSelectboxCrimes() {

    I.waitForText(this.selectCriminalidade, 45);
  }

  verSelectboxIndicador() {

    I.waitForText(this.selectIndicador, 45);
  }

  verMetricasDescritivas() {

    for (const metrica of this.metricasDescritivas) {

      I.waitForText(metrica, 45);
    }
  }

  verMetricasResumo() {

    for (const metrica of this.metricasResumo) {

      I.waitForText(metrica, 45);
    }
  }

  verMetrica(nome) {

    I.waitForText(nome, 45);
  }

  verSecao(nome) {

    I.waitForText(nome, 45);
  }

  verLegenda(texto) {

    I.waitForText(texto, 45);
  }

  async selecionarTabela(rotulo) {

    // O selectbox "Crimes" é renderizado pelo Streamlit como um combobox
    // (react-aria): o gatilho é o <input> do seletor e as opções aparecem em
    // um dropdown virtual (li[role="option"]). Clicar no input abre o menu;
    // depois escolhe-se a opção pelo rótulo amigável (format_func de
    // rotulo_tabela).
    const seletor =
      locate('[data-testid="stSelectbox"]')
        .withText(this.selectCriminalidade)
        .find('input');

    await I.waitForElement(seletor, 45);

    await I.click(seletor);

    const opcao =
      locate('[role="option"]').withText(rotulo);

    await I.waitForElement(opcao, 15);

    await I.click(opcao);

    // A troca de tabela dispara nova chamada à API e re-renderiza a aba;
    // aguarda o "estado limpo" do widget antes de prosseguir.
    await I.waitForElement('[data-testid="stApp"]');
  }

}

module.exports = new VisaoGeralTab();