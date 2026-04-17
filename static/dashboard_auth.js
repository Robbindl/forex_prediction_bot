(function () {
  if (window.__dashboardAuthInstalled) return;
  window.__dashboardAuthInstalled = true;

  const nativeFetch = window.fetch.bind(window);
  let refreshPromise = null;
  let promptShown = false;
  const inflightJsonRequests = new Map();

  function isRequestObject(value) {
    return typeof Request !== 'undefined' && value instanceof Request;
  }

  function toUrl(input) {
    const raw = isRequestObject(input) ? input.url : String(input || '');
    try {
      return new URL(raw, window.location.origin);
    } catch (_) {
      return null;
    }
  }

  function isProtectedApiRequest(input) {
    const url = toUrl(input);
    return !!(url && url.origin === window.location.origin && url.pathname.startsWith('/api/') && url.pathname !== '/api/login');
  }

  function getStoredToken() {
    try {
      return sessionStorage.getItem('api_token') || '';
    } catch (_) {
      return '';
    }
  }

  function setStoredToken(token) {
    try {
      if (token) {
        sessionStorage.setItem('api_token', token);
      } else {
        sessionStorage.removeItem('api_token');
      }
    } catch (_) {}
  }

  function getStoredApiKey() {
    try {
      return String(localStorage.getItem('dashboard_api_key') || sessionStorage.getItem('dashboard_api_key') || '').trim();
    } catch (_) {
      return '';
    }
  }

  function clearStoredApiKey() {
    try {
      localStorage.removeItem('dashboard_api_key');
      sessionStorage.removeItem('dashboard_api_key');
    } catch (_) {}
  }

  async function loginWithApiKey(apiKey) {
    const response = await nativeFetch('/api/login', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({api_key: apiKey}),
    }).catch(() => null);

    if (!response) return '';

    const payload = await response.json().catch(() => ({}));
    if (payload && payload.success && payload.token) {
      setStoredToken(payload.token);
      try {
        localStorage.setItem('dashboard_api_key', apiKey);
      } catch (_) {}
      return payload.token;
    }
    return '';
  }

  async function ensureApiToken(forceRefresh) {
    const existing = getStoredToken();
    const storedApiKey = getStoredApiKey();
    if (!forceRefresh && existing) {
      return existing;
    }

    if (refreshPromise) {
      return refreshPromise;
    }

    refreshPromise = (async () => {
      let apiKey = storedApiKey;
      if (apiKey) {
        const token = await loginWithApiKey(apiKey);
        if (token) return token;
        clearStoredApiKey();
        setStoredToken('');
      }

      if (promptShown) {
        return '';
      }

      promptShown = true;
      apiKey = String(window.prompt('Enter dashboard API key to unlock protected dashboard data:') || '').trim();
      if (!apiKey) {
        return '';
      }

      const token = await loginWithApiKey(apiKey);
      if (!token) {
        clearStoredApiKey();
        setStoredToken('');
        console.warn('[DashboardAuth] Invalid dashboard API key');
      }
      return token;
    })().finally(() => {
      refreshPromise = null;
    });

    return refreshPromise;
  }

  async function authorizedFetch(input, init) {
    if (!isProtectedApiRequest(input)) {
      return nativeFetch(input, init);
    }

    const requestInit = Object.assign({}, init || {});
    let headers = new Headers(requestInit.headers || (isRequestObject(input) ? input.headers : undefined) || {});
    let token = getStoredToken();
    if (!token) {
      token = await ensureApiToken(false);
    }
    if (token && !headers.has('Authorization')) {
      headers.set('Authorization', `Bearer ${token}`);
    }
    requestInit.headers = headers;

    let response = await nativeFetch(input, requestInit);
    if (response.status === 401 || response.status === 403) {
      token = await ensureApiToken(true);
      headers = new Headers(requestInit.headers || {});
      if (token) {
        headers.set('Authorization', `Bearer ${token}`);
        requestInit.headers = headers;
        response = await nativeFetch(input, requestInit);
      }
    }

    return response;
  }

  async function dashboardFetchJson(input, options) {
    const opts = Object.assign({timeoutMs: 15000, init: {}}, options || {});
    const method = String((opts.init && opts.init.method) || 'GET').toUpperCase();
    const requestKey = method === 'GET' ? `${method}:${String(input || '')}` : '';

    if (requestKey && inflightJsonRequests.has(requestKey)) {
      return inflightJsonRequests.get(requestKey);
    }

    const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
    const requestInit = Object.assign({}, opts.init || {});
    let timeoutId = null;
    if (controller) {
      requestInit.signal = controller.signal;
      timeoutId = window.setTimeout(() => controller.abort(), Number(opts.timeoutMs || 15000));
    }

    const requestPromise = (async () => {
      try {
        const response = await window.fetch(input, requestInit);
        if (timeoutId) window.clearTimeout(timeoutId);
        const payload = await response.json().catch(() => ({}));
        if (!response.ok && (!payload || typeof payload !== 'object')) {
          return {success: false, error: `HTTP ${response.status}`};
        }
        if (payload && typeof payload === 'object' && !payload.success && !payload.error && !response.ok) {
          payload.error = `HTTP ${response.status}`;
        }
        return payload;
      } catch (error) {
        if (timeoutId) window.clearTimeout(timeoutId);
        if (error && error.name === 'AbortError') {
          return {success: false, error: 'timeout'};
        }
        return {success: false, error: error && error.message ? error.message : 'request_failed'};
      } finally {
        if (requestKey) {
          inflightJsonRequests.delete(requestKey);
        }
      }
    })();

    if (requestKey) {
      inflightJsonRequests.set(requestKey, requestPromise);
    }
    return requestPromise;
  }

  window.fetch = authorizedFetch;
  window.dashboardAuthReady = ensureApiToken(false).catch(() => '');
  window.dashboardFetchJson = dashboardFetchJson;
  window.dashboardGetApiToken = async function(forceRefresh) {
    return ensureApiToken(!!forceRefresh);
  };
})();
