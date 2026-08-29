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

      show: true,

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

    homePage:
      './tests/pages/HomePage.js'
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
      './tests/features/*.feature',

    steps: [
      './tests/steps/home_page_steps.js'
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