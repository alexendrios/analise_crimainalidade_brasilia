const { I, visaoGeralTab } = inject();

Then('eu visualizo a métrica {string}', (nome) => {

  visaoGeralTab.verMetrica(nome);
});

Then('eu visualizo o título da seção {string}', (nome) => {

  visaoGeralTab.verSecao(nome);
});

Then('eu visualizo a legenda {string}', (texto) => {

  visaoGeralTab.verLegenda(texto);
});

When('eu seleciono a opção {string} no seletor {string}', async (opcao, seletor) => {

  if (seletor === 'Crimes') {

    await visaoGeralTab.selecionarTabela(opcao);

    return;
  }

  throw new Error(`Seletor não suportado pela aba Visão Geral: "${seletor}"`);
});