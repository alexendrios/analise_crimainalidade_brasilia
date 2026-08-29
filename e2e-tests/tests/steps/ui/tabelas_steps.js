const { I, tabelasTab } = inject();

Then('eu visualizo o seletor {string} na base de tabelas', (titulo) => {

  tabelasTab.verSeletor(titulo);
});

Then('eu visualizo as métricas de resumo na base de tabelas', () => {

  tabelasTab.verMetricasResumo();
});

Then(
  'a métrica {string} da tabela selecionada é {string}',
  async (rotulo, valor) => {

    await tabelasTab.verMetricaValor(rotulo, valor);
  }
);

Then('eu visualizo o dataframe na base de tabelas', () => {

  tabelasTab.verDataframe();
});

When(
  'eu seleciono a tabela {string} na base de tabelas',
  async (tabela) => {

    await tabelasTab.selecionarTabela(tabela);
  }
);

Then(
  'eu não visualizo o seletor {string} na base de tabelas',
  async (titulo) => {

    await tabelasTab.verSemSeletor(titulo);
  }
);

When(
  'eu altero o início do intervalo de anos para {string} na base de tabelas',
  async (valor) => {

    await tabelasTab.alterarInicioIntervaloAnos(valor);
  }
);

Then(
  'o início do intervalo de anos é {string} na base de tabelas',
  async (valor) => {

    await tabelasTab.verInicioIntervaloAnos(valor);
  }
);

When(
  'eu seleciono a região administrativa {string} na base de tabelas',
  async (regiao) => {

    await tabelasTab.selecionarRegiao(regiao);
  }
);

Then(
  'eu visualizo a região administrativa {string} selecionada na base de tabelas',
  async (regiao) => {

    await tabelasTab.verSelecaoRegiao(regiao);
  }
);