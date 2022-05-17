import '@applitools/eyes-cypress/commands';

import './commands';

afterEach(() => {
  cy.visit('#/403');
});

Cypress.on('uncaught:exception', (err: Error) => {
  return !err.message.includes('ResizeObserver loop limit exceeded');
});
