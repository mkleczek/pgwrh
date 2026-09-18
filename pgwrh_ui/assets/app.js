/* Small transport helpers; application state and rendering stay in SQL. */
document.addEventListener('htmx:beforeRequest', function (event) {
  if (document.hidden && event.detail.elt.matches('[data-poll]')) {
    event.preventDefault();
  }
});
document.addEventListener('htmx:configRequest', function (event) {
  event.detail.headers.Accept = 'text/html';
  if (event.detail.verb !== 'get') {
    event.detail.headers['X-Pgwrh-UI'] = '1';
  }
});
document.addEventListener('htmx:beforeSwap', function (event) {
  // Expected validation/conflict responses contain a safe HTML message.
  if ([400, 409, 422].includes(event.detail.xhr.status) &&
      event.detail.xhr.getResponseHeader('Content-Type')?.startsWith('text/html')) {
    event.detail.shouldSwap = true;
    event.detail.isError = false;
  }
});
function connectionError(message) {
  const banner = document.getElementById('connection-error');
  banner.textContent = message;
  banner.hidden = false;
}
document.addEventListener('htmx:sendError', function () {
  connectionError('Connection to the controller is unavailable. Displayed reports may be out of date.');
});
document.addEventListener('htmx:responseError', function (event) {
  connectionError('Request failed (HTTP ' + event.detail.xhr.status + '). Refresh or check your access. Displayed reports may be out of date.');
});
document.addEventListener('htmx:afterRequest', function (event) {
  if (event.detail.successful) document.getElementById('connection-error').hidden = true;
});
document.addEventListener('htmx:afterSwap', function () {
  const commit = document.getElementById('commit-rollout');
  const status = document.getElementById('live-status');
  if (commit && status) commit.disabled = status.dataset.commitReady !== 'true';
});
