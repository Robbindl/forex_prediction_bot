(function () {
  if (window.__dashboardAuthInstalled) return;
  window.__dashboardAuthInstalled = true;

  const nativeFetch = window.fetch.bind(window);
  let refreshPromise = null;
  let promptShown = false;
  const inflightJsonRequests = new Map();
  const jsonCachePrefix = 'robbindl:dashboard-json:last-good:';
  const jsonCacheIndexKey = 'robbindl:dashboard-json:last-good:index';
  const maxJsonCacheValueBytes = 900000;
  const maxJsonCacheTotalBytes = 3500000;
  const maxJsonCacheEntries = 24;
  const maxJsonCacheAgeMs = 6 * 60 * 60 * 1000;

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

  function jsonCacheRequestKey(input, method) {
    if (String(method || 'GET').toUpperCase() !== 'GET') return '';
    const url = toUrl(input);
    if (!url || url.origin !== window.location.origin || !url.pathname.startsWith('/api/')) return '';
    if (url.pathname === '/api/login' || url.pathname.endsWith('/stream') || url.pathname.includes('/stream/')) return '';
    if (url.pathname === '/api/live-book' || url.pathname === '/api/chart/quote') return '';
    url.searchParams.delete('token');
    url.searchParams.delete('_');
    return `${url.pathname}${url.search}`;
  }

  function byteSize(value) {
    try {
      if (typeof TextEncoder !== 'undefined') return new TextEncoder().encode(String(value || '')).length;
    } catch (_) {}
    return String(value || '').length * 2;
  }

  function readJsonCacheIndex() {
    try {
      const parsed = JSON.parse(localStorage.getItem(jsonCacheIndexKey) || '[]');
      return Array.isArray(parsed) ? parsed.filter(item => item && item.key) : [];
    } catch (_) {
      return [];
    }
  }

  function writeJsonCacheIndex(index) {
    try {
      localStorage.setItem(jsonCacheIndexKey, JSON.stringify(index.slice(0, maxJsonCacheEntries)));
    } catch (_) {}
  }

  function pruneJsonCache(index) {
    let total = 0;
    const kept = [];
    const sorted = (Array.isArray(index) ? index : []).slice().sort((a, b) => Number(b.stored || 0) - Number(a.stored || 0));
    sorted.forEach(item => {
      const size = Number(item.bytes || 0);
      if (kept.length >= maxJsonCacheEntries || total + size > maxJsonCacheTotalBytes) {
        try { localStorage.removeItem(jsonCachePrefix + item.key); } catch (_) {}
        return;
      }
      total += size;
      kept.push(item);
    });
    writeJsonCacheIndex(kept);
  }

  function shouldStoreJsonPayload(payload) {
    if (!payload || typeof payload !== 'object' || payload.success !== true) return false;
    const reason = String(payload.degraded_reason || payload.reason || '').toLowerCase();
    if (payload.degraded && /refreshing|warming|unavailable|build_failed|timeout|timed out/.test(reason)) return false;
    return true;
  }

  function rememberJsonPayload(cacheKey, payload) {
    if (!cacheKey || !shouldStoreJsonPayload(payload)) return;
    try {
      const raw = JSON.stringify({stored: Date.now(), payload});
      const bytes = byteSize(raw);
      if (bytes > maxJsonCacheValueBytes) return;
      localStorage.setItem(jsonCachePrefix + cacheKey, raw);
      const index = readJsonCacheIndex().filter(item => item.key !== cacheKey);
      index.unshift({key: cacheKey, stored: Date.now(), bytes});
      pruneJsonCache(index);
    } catch (_) {}
  }

  function readRememberedJsonPayload(cacheKey) {
    if (!cacheKey) return null;
    try {
      const raw = localStorage.getItem(jsonCachePrefix + cacheKey);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      const ageMs = Math.max(0, Date.now() - Number(parsed.stored || Date.now()));
      if (ageMs > maxJsonCacheAgeMs) return null;
      const payload = parsed && parsed.payload && typeof parsed.payload === 'object' ? Object.assign({}, parsed.payload) : null;
      if (!payload || payload.success !== true) return null;
      payload.stale = true;
      payload.degraded = true;
      payload.degraded_reason = 'browser_last_good';
      payload.browser_cache_age_ms = ageMs;
      return payload;
    } catch (_) {
      return null;
    }
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
    const cacheKey = jsonCacheRequestKey(input, method);

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
          return readRememberedJsonPayload(cacheKey) || {success: false, error: `HTTP ${response.status}`};
        }
        if (payload && typeof payload === 'object' && !payload.success && !payload.error && !response.ok) {
          payload.error = `HTTP ${response.status}`;
        }
        if (!response.ok || (payload && typeof payload === 'object' && payload.success === false)) {
          return readRememberedJsonPayload(cacheKey) || payload;
        }
        rememberJsonPayload(cacheKey, payload);
        return payload;
      } catch (error) {
        if (timeoutId) window.clearTimeout(timeoutId);
        const remembered = readRememberedJsonPayload(cacheKey);
        if (remembered) return remembered;
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
  window.dashboardBuildAuthedUrl = async function(input, forceRefresh) {
    const url = toUrl(input);
    if (!url) return String(input || '');
    if (isProtectedApiRequest(url.toString())) {
      const token = await ensureApiToken(!!forceRefresh);
      if (token) {
        url.searchParams.set('token', token);
      }
    }
    return url.toString();
  };
})();
