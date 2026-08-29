const {
  I,
  visaoGeralTab,
  previsoesTab,
  classificacaoTab,
  analisesTab,
  resumoGeralTab,
  tabelasTab
} = inject();

const CHECKS = {

  async 'seleção Visão Geral'() {

    visaoGeralTab.verSelectboxCrimes();
    visaoGeralTab.verSelectboxIndicador();
  },

  async 'métricas Visão Geral'() {

    visaoGeralTab.verMetricasDescritivas();
  },

  async 'Previsões'() {

    previsoesTab.verSliderHorizonte();
    previsoesTab.verMetricasModelo();
  },

  async 'Classificação'() {

    classificacaoTab.verSelectboxAnoRanking();
    classificacaoTab.verMetricasModelo();
  },

  async 'sub-abas Análises'() {

    analisesTab.verSubAbas();
  },

  async 'Correlações'() {

    // "Correlações" é a sub-aba padrão da aba Análises; seu conteúdo é
    // carregado automaticamente ao ativar a aba, sem necessidade de clique.
    analisesTab.verWidgetsCorrelacoes();
  },

  async 'Granger'() {

    await analisesTab.clicarSubAba('Granger');

    analisesTab.verWidgetsGranger();
  },

  async 'Zonas Quentes'() {

    await analisesTab.clicarSubAba('Zonas Quentes');

    analisesTab.verWidgetsZonasQuentes();
  },

  async 'Resumo Geral IA'() {

    resumoGeralTab.verTextUrlOllama();
    resumoGeralTab.verSelectboxModelo();
    resumoGeralTab.verBotaoGerarResumo();
  },

  async 'Tabelas'() {

    tabelasTab.verSelectboxCrimes();
    tabelasTab.verWidgetsFiltro();
  }
};

Then('eu visualizo os widgets de {string}', async (tipo) => {

  const check = CHECKS[tipo];

  if (!check) {

    throw new Error(`Tipo de verificação de widgets desconhecido: "${tipo}"`);
  }

  await check();
});
