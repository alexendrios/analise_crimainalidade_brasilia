const { I, basePage, sidebarPage } = inject();

Given('que eu acesso o dashboard de criminalidade', async () => {

  await basePage.abrirAplicacao();
});

Then('eu visualizo o título {string}', (titulo) => {

  I.see(titulo, 'h1');
});

Then('eu visualizo o título {string} na sidebar', () => {

  sidebarPage.verTituloSidebar();
});

Then('eu visualizo o campo {string} na sidebar', () => {

  sidebarPage.verCampoUrlApi();
});

Then('eu visualizo o botão {string} na sidebar', () => {

  sidebarPage.verBotaoVerificarConexao();
});

Then('a aba {string} está selecionada', async (nome) => {

  await basePage.verificarAbaSelecionada(nome);
});
