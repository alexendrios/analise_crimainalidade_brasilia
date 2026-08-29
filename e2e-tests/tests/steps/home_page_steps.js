const { I, homePage } = inject();

Given('acesso a aplicação', () => {
  homePage.abrirAplicacao(I);
});

Then('eu visualizo a seguinte mensagem {string}', (mensagem) => {
  I.see(mensagem);
});
