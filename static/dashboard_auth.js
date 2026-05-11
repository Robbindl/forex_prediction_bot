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
    const useful = !!(
      payload.command_center ||
      payload.live_summary ||
      payload.why_not_traded ||
      payload.watchlist_ladder ||
      payload.session_radar ||
      payload.decision_context ||
      payload.balance != null ||
      (Array.isArray(payload.positions) && payload.positions.length)
    );
    if (payload.degraded && /refreshing|warming|unavailable|build_failed|timeout|timed out/.test(reason) && !useful) return false;
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

  function dashboardList(value) {
    return Array.isArray(value) ? value : [];
  }

  function dashboardObject(value) {
    return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  }

  function dashboardMergeCommandCenterWhale(payload) {
    const root = dashboardObject(payload);
    const commandCenter = dashboardObject(root.command_center);
    const nested = dashboardObject(commandCenter.command_center);
    const whaleSources = [
      dashboardObject(root.whale),
      dashboardObject(nested.whale),
      dashboardObject(commandCenter.whale),
    ].filter(source => Object.keys(source).length);
    const whale = Object.assign({}, ...whaleSources);
    const recent = []
      .concat(dashboardList(root.recent))
      .concat(dashboardList(commandCenter.recent))
      .concat(dashboardList(nested.recent))
      .concat(...whaleSources.map(source => dashboardList(source.recent)))
      .concat(...whaleSources.map(source => dashboardList(source.alerts)));
    const alerts = []
      .concat(dashboardList(root.alerts))
      .concat(dashboardList(commandCenter.alerts))
      .concat(dashboardList(nested.alerts))
      .concat(...whaleSources.map(source => dashboardList(source.alerts)))
      .concat(...whaleSources.map(source => dashboardList(source.recent)));
    const sourceCounts = [
      root.alert_count_24h,
      root.whale_alerts_24h,
      commandCenter.alert_count_24h,
      commandCenter.whale_alerts_24h,
      nested.alert_count_24h,
      nested.whale_alerts_24h,
      ...whaleSources.flatMap(source => [source.alert_count_24h, source.whale_alerts_24h]),
    ].map(Number).filter(Number.isFinite);
    const count = Math.max(...sourceCounts, recent.length, alerts.length, 0);
    return {
      whale,
      recent,
      alerts,
      alert_count_24h: Number.isFinite(count) ? count : 0,
      whale_alerts_24h: Number.isFinite(count) ? count : 0,
    };
  }

  function dashboardNormalizeCommandCenter(payload) {
    const root = dashboardObject(payload);
    const commandCenter = dashboardObject(root.command_center);
    const nested = dashboardObject(commandCenter.command_center);
    const normalized = Object.assign({}, nested, commandCenter);
    const whale = dashboardMergeCommandCenterWhale(root);
    normalized.whale = Object.assign({}, whale.whale, dashboardObject(normalized.whale));
    normalized.recent = dashboardList(normalized.recent).concat(whale.recent);
    normalized.alerts = dashboardList(normalized.alerts).concat(whale.alerts);
    normalized.alert_count_24h = Math.max(Number(normalized.alert_count_24h || 0), whale.alert_count_24h || 0);
    normalized.whale_alerts_24h = Math.max(Number(normalized.whale_alerts_24h || 0), whale.whale_alerts_24h || 0);
    if (!normalized.sentiment_context && root.sentiment_context) normalized.sentiment_context = root.sentiment_context;
    if (!normalized.live_summary) {
      normalized.live_summary = root.live_summary || dashboardObject(root.status).live_summary || dashboardObject(root.risk).live_summary || {};
    }
    return normalized;
  }

  function dashboardDefaultPublishedSignal(signal) {
    if (!signal || typeof signal !== 'object') return false;
    const meta = dashboardObject(signal.metadata);
    const value = key => meta[key] !== undefined ? meta[key] : signal[key];
    const text = key => String(value(key) == null ? '' : value(key)).trim();
    const kind = text('decision_kind') || text('kind');
    const state = text('decision_state') || text('state');
    if (/session_watch/i.test(kind) || /watching/i.test(state)) return false;
    if (/candidate|blocked|accepted|killed|signal/i.test(kind)) return true;
    if (text('direction') || text('signal') || text('side') || text('action')) return true;
    return Boolean(
      text('exact_kill_reason') ||
      text('execution_kill_reason') ||
      text('blocked_reason') ||
      text('kill_reason') ||
      value('entry_confirmation_ready') === true ||
      Number(value('entry_confirmation_bars_required') || 0) > 0 ||
      Number(value('entry_confirmation_count') || 0) > 0
    );
  }

  function dashboardCollectDecisionRows(commandCenter, options) {
    const opts = Object.assign({includeContext: true}, options || {});
    const cc = dashboardNormalizeCommandCenter({command_center: commandCenter});
    const ladder = dashboardObject(cc.watchlist_ladder);
    const decisionContext = dashboardObject(cc.decision_context);
    const radar = dashboardObject(cc.session_radar);
    const sources = [
      cc.latest_signals,
      cc.top_opportunities,
      cc.near_misses,
      ladder.hot,
      ladder.almost_ready,
      ladder.blocked,
      decisionContext.rows,
      radar.rows,
      ladder.inactive,
    ];
    const isPublished = typeof opts.isPublished === 'function' ? opts.isPublished : dashboardDefaultPublishedSignal;
    const seen = new Set();
    const executionRows = [];
    const contextRows = [];
    sources.forEach(source => {
      dashboardList(source).forEach(item => {
        if (!item || typeof item !== 'object') return;
        const asset = item.asset || item.symbol;
        if (!asset) return;
        const key = [
          asset,
          item.decision_kind || item.kind || '',
          item.decision_state || item.state || '',
          item.direction || item.signal || item.side || item.action || '',
          item.decision_reason || item.reason || item.exact_kill_reason || '',
        ].join('|');
        if (seen.has(key)) return;
        seen.add(key);
        if (isPublished(item)) executionRows.push(item);
        else contextRows.push(item);
      });
    });
    if (executionRows.length) return executionRows;
    return opts.includeContext === false ? [] : contextRows;
  }

  function dashboardAccountSummary(payload) {
    const root = dashboardObject(payload);
    const commandCenter = dashboardNormalizeCommandCenter(root);
    const tradeHistory = dashboardObject(root.trade_history);
    const tradeHistorySummary = dashboardObject(root.trade_history_summary || tradeHistory.summary);
    if (!Number.isFinite(Number(tradeHistorySummary.closed_trades)) && Number.isFinite(Number(tradeHistory.count))) {
      tradeHistorySummary.closed_trades = Number(tradeHistory.count);
    }
    if (!Number.isFinite(Number(tradeHistorySummary.total_trades)) && Number.isFinite(Number(tradeHistory.count))) {
      tradeHistorySummary.total_trades = Number(tradeHistory.count);
    }
    if (Array.isArray(tradeHistory.trades) && tradeHistory.trades.length && !Number.isFinite(Number(tradeHistorySummary.total_pnl))) {
      const total = tradeHistory.trades.reduce((sum, row) => sum + Number((row && row.pnl) || 0), 0);
      const initial = Number(tradeHistorySummary.initial_balance ?? root.initial_balance ?? commandCenter.initial_balance ?? 10000);
      tradeHistorySummary.total_pnl = Math.round(total * 100) / 100;
      tradeHistorySummary.realized_total_pnl = Math.round(total * 100) / 100;
      if (!Number.isFinite(Number(tradeHistorySummary.balance))) {
        tradeHistorySummary.balance = Math.round((initial + total) * 100) / 100;
      }
      if (!Number.isFinite(Number(tradeHistorySummary.realized_balance))) {
        tradeHistorySummary.realized_balance = tradeHistorySummary.balance;
      }
      if (!Number.isFinite(Number(tradeHistorySummary.balance_delta))) {
        tradeHistorySummary.balance_delta = Math.round(total * 100) / 100;
      }
      tradeHistorySummary.closed_trades = tradeHistorySummary.closed_trades ?? tradeHistory.trades.length;
      tradeHistorySummary.total_trades = tradeHistorySummary.total_trades ?? tradeHistory.trades.length;
    }
    const liveCandidates = [
      root.live_summary,
      commandCenter.live_summary,
      dashboardObject(root.status).live_summary,
      dashboardObject(root.risk).live_summary,
      root,
      commandCenter,
      dashboardObject(root.status),
      dashboardObject(root.risk),
    ].map(dashboardObject).filter(item =>
      Number.isFinite(Number(item.balance)) ||
      Number.isFinite(Number(item.realized_balance)) ||
      Number.isFinite(Number(item.open_pnl)) ||
      Number.isFinite(Number(item.total_pnl)) ||
      Number.isFinite(Number(item.closed_trades)) ||
      Number.isFinite(Number(item.open_positions))
    );
    const scoreSummary = item => {
      const balance = Number(item.balance ?? item.realized_balance);
      const initial = Number(item.initial_balance);
      const totalPnl = Number(item.total_pnl ?? item.realized_total_pnl ?? item.balance_delta);
      const closed = Number(item.closed_trades ?? item.total_trades);
      const openPnl = Number(item.open_pnl);
      const openPositions = Number(item.open_positions);
      let score = 0;
      if (Number.isFinite(balance)) score += 1;
      if (Number.isFinite(initial)) score += 1;
      if (Number.isFinite(totalPnl) && Math.abs(totalPnl) >= 0.01) score += 8;
      if (Number.isFinite(closed) && closed > 0) score += 6;
      if (Number.isFinite(openPnl) && Math.abs(openPnl) >= 0.01) score += 6;
      if (Number.isFinite(openPositions) && openPositions > 0) score += 4;
      if (Number.isFinite(balance) && Number.isFinite(initial) && Math.abs(balance - initial) >= 0.01) score += 10;
      if (Number.isFinite(item.balance_delta) && Math.abs(Number(item.balance_delta)) >= 0.01) score += 5;
      return score;
    };
    const liveSummary = liveCandidates.sort((a, b) => scoreSummary(b) - scoreSummary(a))[0] || {};
    const tradeClosed = Number(tradeHistorySummary.closed_trades ?? tradeHistorySummary.total_trades ?? 0);
    const tradeRealizedPnl = Number(tradeHistorySummary.total_pnl ?? tradeHistorySummary.realized_total_pnl ?? tradeHistorySummary.balance_delta);
    const tradeBalance = Number(tradeHistorySummary.realized_balance ?? tradeHistorySummary.balance);
    const tradeInitial = Number(tradeHistorySummary.initial_balance);
    const tradeHasAuthority =
      tradeClosed > 0 && (
        Number.isFinite(tradeBalance) ||
        Number.isFinite(tradeRealizedPnl) ||
        Number.isFinite(tradeInitial)
      );
    const realizedSummary = tradeHasAuthority ? tradeHistorySummary : liveSummary;
    const realizedPnl = Number(
      realizedSummary.total_pnl ??
      realizedSummary.realized_total_pnl ??
      realizedSummary.balance_delta ??
      root.total_pnl ??
      commandCenter.total_pnl
    );
    const rawRealizedBalance = Number(
      realizedSummary.realized_balance ??
      realizedSummary.balance ??
      root.realized_balance ??
      commandCenter.realized_balance ??
      root.balance ??
      commandCenter.balance
    );
    const rawInitial = Number(
      realizedSummary.initial_balance ??
      liveSummary.initial_balance ??
      root.initial_balance ??
      commandCenter.initial_balance
    );
    const initial = Number.isFinite(rawInitial)
      ? rawInitial
      : (Number.isFinite(rawRealizedBalance) && Number.isFinite(realizedPnl) ? rawRealizedBalance - realizedPnl : NaN);
    const realized = Number.isFinite(rawRealizedBalance)
      ? rawRealizedBalance
      : (Number.isFinite(initial) && Number.isFinite(realizedPnl) ? initial + realizedPnl : NaN);
    const openPnl = Number(
      liveSummary.open_pnl ??
      root.open_pnl ??
      commandCenter.open_pnl ??
      realizedSummary.open_pnl ??
      0
    );
    const balance = Number.isFinite(realized) ? realized + openPnl : Number(
      liveSummary.balance ??
      realizedSummary.balance ??
      root.balance ??
      commandCenter.balance
    );
    const totalPnl = Number.isFinite(realizedPnl) ? realizedPnl + openPnl : Number(
      liveSummary.total_pnl ??
      liveSummary.balance_delta ??
      realizedSummary.total_pnl ??
      realizedSummary.realized_total_pnl ??
      0
    );
    const balanceDelta = Number.isFinite(balance) && Number.isFinite(initial)
      ? balance - initial
      : Number(
          liveSummary.balance_delta ??
          realizedSummary.balance_delta ??
          totalPnl
        );
    const closedTrades = Math.max(
      Number(realizedSummary.closed_trades ?? realizedSummary.total_trades ?? 0),
      Number(liveSummary.closed_trades ?? liveSummary.total_trades ?? 0),
      0
    );
    const totalTrades = Math.max(
      Number(realizedSummary.total_trades ?? realizedSummary.closed_trades ?? 0),
      Number(liveSummary.total_trades ?? liveSummary.closed_trades ?? 0),
      closedTrades
    );
    return Object.assign({}, liveSummary, realizedSummary, {
      initial_balance: Number.isFinite(initial) ? initial : realizedSummary.initial_balance,
      realized_balance: Number.isFinite(realized) ? realized : realizedSummary.realized_balance,
      balance: Number.isFinite(balance) ? balance : realizedSummary.balance,
      balance_delta: Number.isFinite(balanceDelta) ? Math.round(balanceDelta * 100) / 100 : 0,
      total_pnl: Number.isFinite(totalPnl) ? Math.round(totalPnl * 100) / 100 : 0,
      realized_total_pnl: Number.isFinite(realizedPnl) ? Math.round(realizedPnl * 100) / 100 : 0,
      open_pnl: Number.isFinite(openPnl) ? openPnl : 0,
      closed_trades: Number.isFinite(closedTrades) ? closedTrades : 0,
      total_trades: Number.isFinite(totalTrades) ? totalTrades : 0,
      open_positions: Number(liveSummary.open_positions ?? realizedSummary.open_positions ?? 0) || 0,
      buy_count: Number(liveSummary.buy_count ?? realizedSummary.buy_count ?? 0) || 0,
      sell_count: Number(liveSummary.sell_count ?? realizedSummary.sell_count ?? 0) || 0,
      account_state: balanceDelta < -0.005 ? 'loss' : balanceDelta > 0.005 ? 'gain' : 'flat',
    });
  }

  window.fetch = authorizedFetch;
  window.dashboardAuthReady = ensureApiToken(false).catch(() => '');
  window.dashboardFetchJson = dashboardFetchJson;
  window.dashboardNormalizeCommandCenter = dashboardNormalizeCommandCenter;
  window.dashboardCollectDecisionRows = dashboardCollectDecisionRows;
  window.dashboardAccountSummary = dashboardAccountSummary;
  window.dashboardMergeCommandCenterWhale = dashboardMergeCommandCenterWhale;
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
