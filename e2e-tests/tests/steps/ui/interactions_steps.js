const { I, sidebarPage, basePage } = inject();

When('eu clico em {string} na sidebar', async () => {

  await sidebarPage.verificarConexaoComApi();
});

Then('eu visualizo a mensagem {string}', async () => {

  await sidebarPage.verMensagemConexaoOk();
});

Then('eu visualizo a sub-aba {string} na aba de Análises', async (subaba) => {

  // As sub-abas (Correlações, Granger, Anomalias, Zonas Quentes) são
  // renderizadas como abas aninhadas dentro da aba Análises. A aba
  // Análises dispara as chamadas mais pesadas da API (correlações,
  // Granger, anomalias, zonas quentes), então tolera-se um retry maior.
  await I.waitForText(subaba, 45);
});
