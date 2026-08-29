const { I } = inject();

const TabsBase = require('./TabsBase.js');

const aguardar = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

class ManchaCriminalTab extends TabsBase {

  constructor() {

    super('Mancha Criminal', [
      'Mancha Criminal por RA',
      'RAs mais críticas'
    ]);

    this.selectCrimes = 'Crimes';

    this.selectColuna = 'Coluna (indicador)';

    this.selectRecorte = 'Recorte temporal';
  }

  verSeletor(titulo) {

    I.waitForText(titulo, 45);
  }

  verGraficoManchaCriminal() {

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

  verRasMaisCriticas() {

    // A seção "RAs mais críticas" lista até 5 métricas com o total por RA
    // da tabela/recorte selecionados.
    I.see('RAs mais críticas');

    I.waitForElement(
      '[data-testid="stMetric"]:visible', 45
    );
  }

  async selecionarOpcao(seletor, opcao) {

    // Os seletores da aba Mancha Criminal são comboboxes do Streamlit
    // (react-aria): clicar no <input> abre o dropdown e as opções aparecem
    // como li[role="option"]. Como o DOM mantém os painéis das abas
    // inativas, o filtro :visible restringe a interação aos controles da
    // aba ativa.
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

}

module.exports = new ManchaCriminalTab();