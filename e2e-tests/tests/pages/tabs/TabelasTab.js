const { I } = inject();

const TabsBase = require('./TabsBase.js');

const aguardar = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

class TabelasTab extends TabsBase {

  constructor() {

    super('Tabelas', [
      'Explorar Tabelas Gold'
    ]);

    this.selectCrimes = 'Crimes';

    this.selectRegiao =
      'Região Administrativa';

    this.sliderIntervaloAnos =
      'Intervalo de anos';

    this.rangeAnosStart =
      'Intervalo de anos — start';

    this.metricasResumo = [
      'Linhas',
      'Colunas',
      'Valores nulos'
    ];
  }

  verSeletor(titulo) {

    I.waitForText(titulo, 45);
  }

  verMetricasResumo() {

    for (const metrica of this.metricasResumo) {

      I.waitForText(metrica, 45);
    }
  }

  verSelectboxCrimes() {

    I.waitForText(this.selectCrimes, 45);
  }

  verWidgetsFiltro() {

    I.waitForText(this.selectRegiao, 45);
    I.waitForText(this.sliderIntervaloAnos, 45);

    for (const metrica of this.metricasResumo) {

      I.waitForText(metrica, 45);
    }
  }

  verDataframe() {

    // A carga da tabela completa via API é demorada (30s+); usa uma janela
    // folgada para o st.dataframe aparecer com as linhas já materializadas.
    I.waitForElement(
      '[data-testid="stDataFrame"]:visible', 90
    );
  }

  async _lerMetricaValor(rotulo) {

    return I.executeScript((label) => {

      const metricas = [
        ...document.querySelectorAll('[data-testid="stMetric"]')
      ];

      const metrica = metricas.find(
        (m) => (m.innerText || '').startsWith(label)
      );

      const valor = metrica &&
        metrica.querySelector('[data-testid="stMetricValue"]');

      return valor
        ? valor.innerText.trim()
        : null;
    }, rotulo);
  }

  async verMetricaValor(rotulo, valor) {

    // O resumo é buscado junto com a (re)execução do script; faz poll
    // sem-throw até a métrica refletir o valor esperado.
    const inicio = Date.now();

    while (Date.now() - inicio < 60000) {

      const atual = await this._lerMetricaValor(rotulo);

      if (atual === valor) return;

      await aguardar(500);
    }

    throw new Error(
      `Métrica "${rotulo}" não atingiu o valor ${valor} ` +
      `(obtido: ${await this._lerMetricaValor(rotulo)})`
    );
  }

  async verSemSeletor(titulo) {

    // Ao trocar para uma tabela sem coluna "ano" o slider de intervalo some;
    // a (re)execução no Streamlit é lenta, então aguarda o texto desaparecer.
    const seletor =
      `[data-testid="stMarkdownContainer"]:visible` +
      `:has-text("${titulo}")`;

    const inicio = Date.now();

    while (Date.now() - inicio < 60000) {

      const visiveis =
        await I.grabNumberOfVisibleElements(seletor);

      if (visiveis === 0) return;

      await aguardar(500);
    }

    throw new Error(
      `Seletor "${titulo}" ainda visível após a troca de tabela.`
    );
  }

  async _selecionarOpcaoCombobox(seletor, opcao) {

    const combobox =
      `[data-testid="stSelectbox"]:visible` +
      `:has-text("${seletor}") input`;

    await this.aguardarFimProcessamento();

    await I.waitForElement(combobox, 45);

    await I.click(combobox);

    const opcaoItem =
      `[role="option"]:visible:has-text("${opcao}")`;

    // Poll sem-throw até a opção aparecer. As listas são virtualizadas e as
    // últimas opções podem não ser renderizadas sem rolar o dropdown.
    let achou = false;
    const inicio = Date.now();

    while (Date.now() - inicio < 10000 && !achou) {

      achou =
        (await I.grabNumberOfVisibleElements(opcaoItem)) > 0;

      if (!achou) await aguardar(400);
    }

    if (!achou) {

      // Opção fora da janela virtualizada: digita o texto para filtrar a
      // lista do combobox e expor o item desejado (react-aria filtra a lista).
      await I.fillField(combobox, opcao);

      const inicioFiltro = Date.now();

      while (Date.now() - inicioFiltro < 10000 && !achou) {

        achou =
          (await I.grabNumberOfVisibleElements(opcaoItem)) > 0;

        if (!achou) await aguardar(400);
      }
    }

    if (!achou) {
      throw new Error(
        `Opção "${opcao}" não encontrada no combobox "${seletor}".`
      );
    }

    await I.click(opcaoItem);

    await I.waitForElement('[data-testid="stApp"]');
  }

  async selecionarTabela(tabela) {

    await this._selecionarOpcaoCombobox(
      this.selectCrimes, tabela
    );
  }

  async selecionarRegiao(regiao) {

    await this._selecionarOpcaoCombobox(
      this.selectRegiao, regiao
    );
  }

  async _lerSelecaoCombobox(rotulo) {

    return I.executeScript((label) => {

      const selects = [
        ...document.querySelectorAll('[data-testid="stSelectbox"]')
      ];

      const seletor = selects.find(
        (s) => (s.innerText || '').includes(label)
      );

      const input = seletor &&
        seletor.querySelector('input[role="combobox"]');

      return input ? input.value : null;
    }, rotulo);
  }

  async verSelecaoRegiao(regiao) {

    const inicio = Date.now();

    while (Date.now() - inicio < 60000) {

      const atual = await this._lerSelecaoCombobox(
        this.selectRegiao
      );

      if (atual === regiao) return;

      await aguardar(500);
    }

    throw new Error(
      `Região Administrativa não refletiu o valor ${regiao} ` +
      `(obtido: ${await this._lerSelecaoCombobox(this.selectRegiao)})`
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

  async alterarInicioIntervaloAnos(valor) {

    await this.aguardarFimProcessamento();

    await I.waitForElement(
      `[data-testid="stSlider"]:visible` +
      `:has-text("${this.sliderIntervaloAnos}")`,
      90
    );

    await this._focarRange(this.rangeAnosStart);

    await aguardar(500);

    const atual = await this._lerValorRange(this.rangeAnosStart);

    if (atual === null) {
      throw new Error(
        'Range "Intervalo de anos — start" não encontrado.'
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

  async verInicioIntervaloAnos(valor) {

    const alvo = parseInt(valor, 10);

    const inicio = Date.now();

    while (Date.now() - inicio < 60000) {

      const atual = await this._lerValorRange(this.rangeAnosStart);

      if (atual === alvo) return;

      await aguardar(500);
    }

    throw new Error(
      'Range "Intervalo de anos — start" não atingiu o valor ' +
      `${alvo} (obtido: ${await this._lerValorRange(this.rangeAnosStart)})`
    );
  }

}

module.exports = new TabelasTab();