const { I, basePage } = inject();

const SUBHEADERS = {
  'Visão Geral': 'Visão Geral',
  'Séries Temporais': 'Séries Temporais',
  'Mapa de Calor': 'Mapa de Calor por RA',
  'Mancha Criminal': 'Mancha Criminal por RA',
  'Identificação crimes': 'Idades — Vítima × Autor (suspeito)',
  'Desaparecidos': 'Desaparecidos',
  'Violência contra idosos': 'Violência contra Idosos',
  'Previsões': 'Previsão — Crimes contra a Mulher (Prophet + XGBoost)',
  'Classificação': 'Classificação — Criminalidade Letal por RA (Regressão Logística)',
  'Análises': 'Análises Executivas',
  'Resumo Geral': null,
  'Tabelas': null
};

When('eu ativo a aba {string}', async (nome) => {

  await basePage.ativarAba(nome);
});

Then('eu visualizo o conteúdo da {string}', async (nome) => {

  await basePage.verificarAbaSelecionada(nome);

  await basePage.verificarAbaVisivel(nome);

  const subheader = SUBHEADERS[nome];

  if (subheader) {

    await I.waitForText(subheader, 45);
  }
});
