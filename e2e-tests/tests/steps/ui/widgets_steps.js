const {
  I,
  visaoGeralTab,
  previsoesTab,
  classificacaoTab,
  analisesTab,
  resumoGeralTab,
  tabelasTab
} = inject();

Then('eu visualizo os widgets de seleção da Visão Geral', async () => {

  await visaoGeralTab.verAbaSelecionada();

  await visaoGeralTab.verSelectboxCrimes();

  await visaoGeralTab.verSelectboxIndicador();
});

Then('eu visualizo as métricas descritivas da Visão Geral', async () => {

  await visaoGeralTab.verAbaSelecionada();

  await visaoGeralTab.verMetricasDescritivas();
});

Then('eu visualizo o controle de horizonte e as métricas de Previsões', async () => {

  await previsoesTab.verAbaSelecionada();

  await previsoesTab.verSliderHorizonte();

  await previsoesTab.verMetricasModelo();
});

Then('eu visualizo o seletor de ano e as métricas de Classificação', async () => {

  await classificacaoTab.verAbaSelecionada();

  await classificacaoTab.verSelectboxAnoRanking();

  await classificacaoTab.verMetricasModelo();
});

Then('eu visualizo as sub-abas e os controles de Análises', async () => {

  await analisesTab.verAbaSelecionada();

  await analisesTab.verSubAbas();
});

Then('eu visualizo os controles da sub-aba Correlações', async () => {

  // "Correlações" é a sub-aba padrão da aba Análises; seu conteúdo é
  // carregado automaticamente ao ativar a aba, sem necessidade de clique.
  await analisesTab.verWidgetsCorrelacoes();
});

Then('eu visualizo os controles da sub-aba Granger', async () => {

  await analisesTab.clicarSubAba('Granger');

  await analisesTab.verWidgetsGranger();
});

Then('eu visualizo os controles da sub-aba Zonas Quentes', async () => {

  await analisesTab.clicarSubAba('Zonas Quentes');

  await analisesTab.verWidgetsZonasQuentes();
});

Then('eu visualizo os controles de IA do Resumo Geral', async () => {

  await resumoGeralTab.verAbaSelecionada();

  await resumoGeralTab.verTextUrlOllama();

  await resumoGeralTab.verSelectboxModelo();
});

Then('eu visualizo o botão de gerar resumo com IA', async () => {

  await resumoGeralTab.verBotaoGerarResumo();
});

Then('eu visualizo os widgets de filtro da aba Tabelas', async () => {

  await tabelasTab.verAbaSelecionada();

  await tabelasTab.verSelectboxCrimes();

  await tabelasTab.verWidgetsFiltro();
});
