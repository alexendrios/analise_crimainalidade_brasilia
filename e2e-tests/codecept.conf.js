const UI_URL =
  process.env.UI_URL ||
  'http://localhost:8501';

const custom =
  require('./helpers/custom.js');

const attachAllureEvidence =
  require('./utils/attach-allure-evidence.js');

const {
  setHeadlessWhen
} = require('@codeceptjs/configure');

setHeadlessWhen(process.env.HEADLESS);

/** @type {CodeceptJS.MainConfig} */
exports.config = {

  // ==========================================
  // OUTPUT
  // ==========================================

  output: './output',

  // ==========================================
  // HELPERS
  // ==========================================

  helpers: {

    // ========================================
    // PLAYWRIGHT
    // ========================================

    Playwright: {

      url: UI_URL,

      show: false,

      browser: 'chromium',

      restart: true,

      // ======================================
      // VÍDEO
      // ======================================

      video: true,

      keepVideoForPassedTests: true,

      // ======================================
      // TRACE
      // ======================================

      trace: true,

      // ======================================
      // CONFIGURAÇÃO
      // ======================================

      colorScheme: 'dark',

      waitForAction: 1000,

      waitForTimeout: 1000,

      manualStart: false,

      pressKeyDelay: 1
    },

    // ========================================
    // ALLURE EVIDENCE
    // ========================================

    AllureEvidenceHelper: {

      require:
        './helpers/allure-evidence-helper.js'
    }
  },

  // ==========================================
  // PAGE OBJECTS
  // ==========================================

  include: {

    basePage:
      './tests/pages/BasePage.js',

    sidebarPage:
      './tests/pages/SidebarPage.js',

    visaoGeralTab:
      './tests/pages/tabs/VisaoGeralTab.js',

    seriesTemporaisTab:
      './tests/pages/tabs/SeriesTemporaisTab.js',

    mapaCalorTab:
      './tests/pages/tabs/MapaCalorTab.js',

    manchaCriminalTab:
      './tests/pages/tabs/ManchaCriminalTab.js',

    identificacaoCrimesTab:
      './tests/pages/tabs/IdentificacaoCrimesTab.js',

    desaparecidosTab:
      './tests/pages/tabs/DesaparecidosTab.js',

    violenciaIdososTab:
      './tests/pages/tabs/ViolenciaIdososTab.js',

    previsoesTab:
      './tests/pages/tabs/PrevisoesTab.js',

    classificacaoTab:
      './tests/pages/tabs/ClassificacaoTab.js',

    analisesTab:
      './tests/pages/tabs/AnalisesTab.js',

    resumoGeralTab:
      './tests/pages/tabs/ResumoGeralTab.js',

    tabelasTab:
      './tests/pages/tabs/TabelasTab.js'
  },

  // ==========================================
  // BOOTSTRAP
  // ==========================================

  bootstrap:
    custom.init(),

  timeout:
    null,

  teardown:
    attachAllureEvidence.attachEvidence,

  // ==========================================
  // GHERKIN
  // ==========================================

  gherkin: {

    features:
      './tests/features/**/*.feature',

    steps: [
      './tests/steps/ui/dashboard_steps.js',
      './tests/steps/ui/tabs_steps.js',
      './tests/steps/ui/interactions_steps.js',
      './tests/steps/ui/widgets_steps.js',
      './tests/steps/ui/visao_geral_steps.js',
      './tests/steps/ui/series_temporais_steps.js',
      './tests/steps/ui/mapa_calor_steps.js'
    ]
  },

  // ==========================================
  // PLUGINS
  // ==========================================

  plugins: {

    // ========================================
    // SCREENSHOT
    // ========================================

    screenshot: {

      enabled: true,

      uniqueScreenshotNames: true,

      disableScreenshotOnFail: false
    },

    // ========================================
    // ALLURE
    // ========================================

    allure: {

      enabled: true,

      require:
        'allure-codeceptjs',

      outputDir:
        './allure-results'
    },

    // ========================================
    // PAUSE ON FAIL
    // ========================================

    pauseOnFail: {

      enabled: false
    }
  },

  // ==========================================
  // TIMEOUT
  // ==========================================

  stepTimeout:
    0,

  stepTimeoutOverride: [

    {
      pattern:
        'wait.*',

      timeout:
        0
    },

    {
      pattern:
        'amOnPage',

      timeout:
        0
    }
  ],

  // ==========================================
  // SUÍTE
  // ==========================================

  _beforeSuite() {

    console.log(
      '\x1b[34m\n' +
      '--------------------------- ' +
      'Suíte de Testes ' +
      '--------------------------------\x1b[0m\n'
    );

    console.log(
      '\x1b[34m\t\tExecutando os Cenários:\x1b[0m\n'
    );
  },

  _afterSuite() {

    console.log(
      '\x1b[34m\n' +
      '--------------------------- ' +
      'Suíte Finalizada ' +
      '-------------------------------\x1b[0m\n'
    );
  },

  // ==========================================
  // NOME
  // ==========================================

  name:
    'tests-Criminalidade-Brasilia-DF-web'
};