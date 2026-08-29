/// <reference types='codeceptjs' />
type homePage = typeof import('./tests/pages/HomePage.js');
type Hooks = import('./helpers/hooks.js');

declare namespace CodeceptJS {
  interface SupportObject { I: I, current: any, homePage: homePage }
  interface Methods extends Playwright, Hooks {}
  interface I extends WithTranslation<Methods> {}
  namespace Translation {
    interface Actions {}
  }
}
