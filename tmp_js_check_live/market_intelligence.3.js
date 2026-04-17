
let lwChart, cSeries, vSeries, sseSource;
let livePriceLine = null;
let currentAsset = 'EUR/USD', currentTf = '5m';
let candlesLoading = false;
let _chartCandles = [];
let _chartTfUsed = '5m';
let _chartBooted = false;
let _chartRetryCount = 0;
let _chartLiveOverlayAllowed = true;
let _chartLiveSource = '';
let _chartHistorySource = '';
let _chartHistorySourceClass = '';
let _chartProviderFamily = '';
let _chartProviderWarning = '';
let _chartMode = 'live';
let _historyCursor = null;
let _historyHasMore = false;
let _lastLiveReloadAt = 0;
let _currentAssetDescriptor = null;
let _heatmapItems = [];
let _chartPricePrecision = 5;
let _marketIntelligenceCache = null;
let _marketIntelligencePending = null;

function numValue(){ for(let i=0;i<arguments.length;i++){ const n = Number(arguments[i]); if(Number.isFinite(n)) return n; } return null; }
function boolValue(){ for(let i=0;i<arguments.length;i++){ const v = arguments[i]; if(v === true || v === false) return v; } return null; }
function humanizeChartToken(value, fallback='—'){ const raw = String(value || '').trim(); return raw ? raw.replace(/_/g,' ') : fallback; }
function statusChip(state,label){ return '<span class="status-chip status-'+state+'">'+label+'</span>'; }
function signalMeta(signal){ return signal && typeof signal.metadata === 'object' && signal.metadata ? signal.metadata : {}; }
function presentLabel(value, fallback){ const raw = String(value || '').trim(); return raw ? raw : fallback; }
function exactKillReason(signal){ const meta = signalMeta(signal); return presentLabel(meta.execution_kill_reason || meta.kill_reason || signal.execution_kill_reason || signal.kill_reason, ''); }
function confirmationState(signal){
  const meta = signalMeta(signal);
  const ready = boolValue(meta.entry_confirmation_ready, signal.entry_confirmation_ready);
  const count = numValue(meta.entry_confirmation_count, signal.entry_confirmation_count, 0);
  const required = numValue(meta.entry_confirmation_bars_required, signal.entry_confirmation_bars_required, 0);
  if(ready) return {state:'ready',label:'Ready',detail:count && required ? count+'/'+required+' bars' : 'confirmed'};
  if(required && count < required) return {state:'waiting',label:'Waiting',detail:count+'/'+required+' bars'};
  return {state:'neutral',label:'Unknown',detail:'confirmation n/a'};
}
function entryReadiness(signal){
  const meta = signalMeta(signal);
  const retest = boolValue(meta.breakout_retest_ready, signal.breakout_retest_ready);
  const pullback = boolValue(meta.first_pullback_ready, signal.first_pullback_ready);
  const confirm = confirmationState(signal);
  const kill = exactKillReason(signal);
  if(kill) return {state:'blocked',label:'Blocked',detail:kill};
  if((retest || pullback) && confirm.state === 'ready') return {state:'ready',label:'Ready',detail:(retest ? 'retest' : 'pullback')+' + confirmation'};
  if(retest || pullback) return {state:'waiting',label:'Waiting',detail:confirm.detail};
  return {state:'neutral',label:'Context only',detail:'structure not ready'};
}
function formatRelativeAge(ageSec){ if(!Number.isFinite(ageSec) || ageSec < 0) return '—'; if(ageSec < 60) return ageSec+'s'; if(ageSec < 3600) return Math.floor(ageSec/60)+'m'; return Math.floor(ageSec/3600)+'h'; }
function clampChartPrecision(value, fallback = 5){ const parsed = Number(value); return Number.isFinite(parsed) ? Math.max(0, Math.min(8, Math.round(parsed))) : Math.max(0, Math.min(8, Math.round(Number(fallback) || 0))); }
function assetPrecisionHint(asset, descriptor = null){
  const source = descriptor || _currentAssetDescriptor || {};
  const explicit = Number(source.price_precision);
  if (Number.isFinite(explicit)) return clampChartPrecision(explicit, 5);
  const symbol = String(source.symbol || asset || currentAsset || '').trim().toUpperCase();
  const category = String(source.category || '').trim().toLowerCase();
  if (category === 'crypto' || /(BTC|ETH|SOL|XRP|ADA|DOGE|LTC|BNB)/.test(symbol)) return 8;
  if (category === 'forex' || symbol.includes('/')) return symbol.endsWith('JPY') || symbol.includes('/JPY') ? 3 : 5;
  if (category === 'commodities' || /(XAU|XAG|WTI|BRENT|OIL|GAS)/.test(symbol)) return /(XAU|XAG)/.test(symbol) ? 3 : 2;
  if (category === 'indices' || symbol.endsWith('=F') || symbol.startsWith('^')) return 2;
  return symbol.includes('/') ? 5 : 2;
}
function decimalPlacesFromValue(value){ const parsed = Number(value); if(!Number.isFinite(parsed)) return 0; const text = parsed.toFixed(10).replace(/0+$/, '').replace(/\.$/, ''); const dot = text.indexOf('.'); return dot >= 0 ? Math.min(8, text.length - dot - 1) : 0; }
function chartPrecisionFromCandles(candles, asset = currentAsset, descriptor = _currentAssetDescriptor){
  let precision = assetPrecisionHint(asset, descriptor);
  const sample = Array.isArray(candles) ? candles.slice(-200) : [];
  for(const candle of sample){
    if(!candle) continue;
    precision = Math.max(precision, decimalPlacesFromValue(candle.open), decimalPlacesFromValue(candle.high), decimalPlacesFromValue(candle.low), decimalPlacesFromValue(candle.close));
    if(precision >= 8) return 8;
  }
  return clampChartPrecision(precision, 5);
}
function applyChartPricePrecision(precision){
  _chartPricePrecision = clampChartPrecision(precision, _chartPricePrecision || 5);
  if(!cSeries || typeof cSeries.applyOptions !== 'function') return;
  try{ cSeries.applyOptions({priceFormat:{type:'price',precision:_chartPricePrecision,minMove:Math.pow(10,-_chartPricePrecision)}}); }catch(e){}
}
function syncChartPricePrecision(candles = null, asset = currentAsset, descriptor = _currentAssetDescriptor){
  const precision = candles ? chartPrecisionFromCandles(candles, asset, descriptor) : assetPrecisionHint(asset, descriptor);
  applyChartPricePrecision(precision);
  return _chartPricePrecision;
}
function formatAssetPrice(value, asset = currentAsset, descriptor = _currentAssetDescriptor){ const parsed = Number(value); return Number.isFinite(parsed) ? parsed.toFixed(assetPrecisionHint(asset, descriptor)) : '—'; }
function formatChartPrice(value){ const parsed = Number(value); return Number.isFinite(parsed) ? parsed.toFixed(_chartPricePrecision) : '—'; }
function intervalSeconds(tf){ return {'1m':60,'5m':300,'15m':900,'30m':1800,'1h':3600,'4h':14400,'1d':86400}[tf] || null; }
function volumeBarFor(candle){ return {time:candle.time,value:candle.volume || 0,color:candle.close >= candle.open ? 'rgba(0,208,132,.25)' : 'rgba(255,69,96,.25)'}; }
function signalMeta(signal){ return signal && typeof signal.metadata === 'object' && signal.metadata ? signal.metadata : {}; }
function signalValue(signal,key){ const meta = signalMeta(signal); return meta[key] !== undefined ? meta[key] : signal ? signal[key] : undefined; }
function signalNum(signal,key,fallback=0){ const num = Number(signalValue(signal,key)); return Number.isFinite(num) ? num : fallback; }
function signalBool(signal,key){ return signalValue(signal,key) === true; }
function signalText(signal,key){ const value = signalValue(signal,key); return value == null ? '' : String(value).trim(); }
function normalizeSignalAsset(asset){
  return String(asset || '')
    .toUpperCase()
    .replace('/USD','')
    .replace('-USD','')
    .replace('USDT','')
    .replace('=F','');
}
function confirmationState(signal){
  const ready = signalBool(signal, 'entry_confirmation_ready');
  const count = signalNum(signal, 'entry_confirmation_count', 0);
  const required = signalNum(signal, 'entry_confirmation_bars_required', 0);
  if(ready) return {state:'ready', label:'Ready', detail: count && required ? count + '/' + required + ' bars' : 'confirmed'};
  if(required && count < required) return {state:'waiting', label:'Waiting', detail: count + '/' + required + ' bars'};
  return {state:'neutral', label:'Unknown', detail:'confirmation not published'};
}
function entryReadiness(signal){
  const confirm = confirmationState(signal);
  const retest = signalBool(signal, 'breakout_retest_ready');
  const pullback = signalBool(signal, 'first_pullback_ready');
  const kill = exactKillReason(signal);
  if(kill) return {state:'blocked', label:'Blocked', detail:kill};
  if((retest || pullback) && confirm.state === 'ready') return {state:'ready', label:'Ready', detail:(retest ? 'retest' : 'pullback') + ' + confirmation'};
  if(retest || pullback) return {state:'waiting', label:'Waiting', detail:confirm.detail};
  return {state:'neutral', label:'Structure only', detail:'entry path not ready'};
}
function exactKillReason(signal){
  return String(signalValue(signal, 'exact_kill_reason') || signalValue(signal, 'execution_kill_reason') || signalValue(signal, 'kill_reason') || signalValue(signal, 'killed_by') || signalValue(signal, 'reason') || '').trim();
}
function reviewNotesLine(signal){
  return [signalValue(signal, 'market_review_notes'), signalValue(signal, 'execution_review_notes')].filter(Boolean).join(' · ');
}
function regimeBits(signal){
  const bits = [];
  const regime = String(signalValue(signal, 'regime_policy_summary') || signalValue(signal, 'regime_policy') || signalValue(signal, 'regime_label') || '').trim();
  const cluster = Number(signalValue(signal, 'cluster_penalty') || 0);
  const extension = Number(signalValue(signal, 'extension_score') || 0);
  if(regime) bits.push('Regime ' + regime);
  if(cluster) bits.push('Cluster ' + cluster.toFixed(cluster >= 1 ? 0 : 2));
  if(extension) bits.push('Ext ' + extension.toFixed(extension >= 1 ? 0 : 2));
  return bits;
}
function fetchPrimarySignal(page){
  const current = normalizeSignalAsset(currentAsset);
  const sources = [
    page && page.signals,
    page && page.command_center && page.command_center.latest_signals,
    page && page.command_center && page.command_center.top_opportunities,
    page && page.command_center && page.command_center.near_misses,
    page && page.command_center && page.command_center.positions,
  ];
  for(const source of sources){
    if(Array.isArray(source) && source.length){
      const exact = source.find(s => normalizeSignalAsset(s.asset || s.symbol) === current);
      if(exact) return exact;
    }
  }
  for(const source of sources){
    if(Array.isArray(source) && source.length){
      return source[0];
    }
  }
  return null;
}
async function fetchMarketIntelligenceOverview(){
  const now = Date.now();
  if(_marketIntelligenceCache && now - _marketIntelligenceCache.fetched < 2000) return _marketIntelligenceCache.data;
  if(_marketIntelligencePending) return _marketIntelligencePending;
  _marketIntelligencePending = (async function(){
    try{
      const d = await window.dashboardFetchJson('/api/page-overview?page=market_intelligence&no_cache=1&_=' + Date.now(), {timeoutMs:10000, init:{cache:'no-store'}});
      if(d.success){ _marketIntelligenceCache = {fetched:Date.now(),data:d}; return d; }
      return {success:false};
    } finally {
      _marketIntelligencePending = null;
    }
  })();
  return _marketIntelligencePending;
}
async function fetchWithTimeout(url, ms=20000){
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try{ const r = await fetch(url, {signal:ctrl.signal}); clearTimeout(id); return r.json(); } catch(e){ clearTimeout(id); return null; }
}
function clearLivePriceLine(){ if(!cSeries || !livePriceLine) return; try{ cSeries.removePriceLine(livePriceLine); }catch(e){} livePriceLine = null; }
function setLivePriceLine(price){
  if(!cSeries || !Number.isFinite(price)) return;
  if(!livePriceLine){
    livePriceLine = cSeries.createPriceLine({price,color:'#00e5ff',lineWidth:1,lineStyle:LightweightCharts.LineStyle.Dashed,axisLabelVisible:true,title:'LIVE'});
    return;
  }
  try{ livePriceLine.applyOptions({price}); }catch(e){ clearLivePriceLine(); setLivePriceLine(price); }
}
function updateChartModeUi(){
  const liveBtn = document.getElementById('chartModeLive');
  const historyBtn = document.getElementById('chartModeHistory');
  const historyControls = document.getElementById('historyControls');
  const title = document.getElementById('chartPanelTitle');
  const livePriceEl = document.getElementById('livePrice');
  if(liveBtn) liveBtn.classList.toggle('on', _chartMode === 'live');
  if(historyBtn) historyBtn.classList.toggle('on', _chartMode === 'history');
  if(historyControls) historyControls.classList.toggle('on', _chartMode === 'history');
  if(title) title.textContent = _chartMode === 'history' ? '🗃️ Deep History' : '🕯️ Live Chart';
  if(livePriceEl) livePriceEl.style.opacity = _chartMode === 'history' ? '.5' : '1';
}
function historyEndTimeFromInput(){ const raw = String(document.getElementById('historyEndDate')?.value || '').trim(); return raw ? raw + 'T23:59:59Z' : null; }
function mergeCandles(existing, incoming){
  const merged = new Map();
  [].concat(incoming || [], existing || []).forEach(candle => { if(candle && Number.isFinite(Number(candle.time))) merged.set(Number(candle.time), candle); });
  return Array.from(merged.values()).sort((a,b) => Number(a.time) - Number(b.time));
}
function applyLiveTickToChart(price, tsSec){
  if(_chartMode !== 'live' || !Number.isFinite(Number(price))) return;
  if(!Array.isArray(_chartCandles) || !_chartCandles.length || !cSeries){
    setLivePriceLine(Number(price));
    return;
  }
  const intervalSec = intervalSeconds(_chartTfUsed || currentTf) || intervalSeconds(currentTf);
  if(!intervalSec){
    setLivePriceLine(Number(price));
    return;
  }
  const last = _chartCandles[_chartCandles.length - 1];
  const lastTime = Number(last && last.time);
  if(!Number.isFinite(lastTime)){
    setLivePriceLine(Number(price));
    return;
  }
  const tickTs = Number.isFinite(Number(tsSec)) ? Number(tsSec) : Math.floor(Date.now() / 1000);
  const bucketTime = Math.floor(tickTs / intervalSec) * intervalSec;
  const nextPrice = Number(price);
  let candle = null;
  if(bucketTime === lastTime){
    candle = {
      ...last,
      high: Math.max(Number(last.high || nextPrice), nextPrice),
      low: Math.min(Number(last.low || nextPrice), nextPrice),
      close: nextPrice
    };
    _chartCandles[_chartCandles.length - 1] = candle;
  }else if(bucketTime > lastTime){
    const open = Number(last.close || nextPrice);
    candle = {
      time: bucketTime,
      open: open,
      high: Math.max(open, nextPrice),
      low: Math.min(open, nextPrice),
      close: nextPrice,
      volume: 0
    };
    _chartCandles = _chartCandles.concat([candle]).slice(-2000);
  }else{
    setLivePriceLine(nextPrice);
    return;
  }
  clearLivePriceLine();
  try{ cSeries.update(candle); }catch(err){ try{ cSeries.setData(_chartCandles); }catch(_err){} }
  try{ vSeries.update(volumeBarFor(candle)); }catch(err){}
}
function updateHistoryButtons(){
  const loadOlderBtn = document.getElementById('historyLoadOlder');
  if(!loadOlderBtn) return;
  loadOlderBtn.disabled = !_historyHasMore || candlesLoading;
  loadOlderBtn.textContent = _historyHasMore ? 'Load older' : 'Start reached';
}
function initChart(){
  lwChart = LightweightCharts.createChart(document.getElementById('lwChart'), {layout:{background:{type:'solid',color:'#0d1117'}, textColor:'#7a8899'},grid:{vertLines:{color:'#1a2030'}, horzLines:{color:'#1a2030'}},crosshair:{mode:LightweightCharts.CrosshairMode.Normal},rightPriceScale:{borderColor:'#1e2635', autoScale:true},timeScale:{borderColor:'#1e2635', timeVisible:true}});
  cSeries = lwChart.addCandlestickSeries({upColor:'#00d084', downColor:'#ff4560', borderVisible:false, wickUpColor:'#00d084', wickDownColor:'#ff4560'});
  vSeries = lwChart.addHistogramSeries({priceScaleId:'', scaleMargins:{top:0.85,bottom:0}});
  syncChartPricePrecision(null, currentAsset, _currentAssetDescriptor);
  new ResizeObserver(() => { const c = document.getElementById('lwChart'); if(lwChart) lwChart.resize(c.clientWidth, c.clientHeight); }).observe(document.getElementById('lwChart'));
}
function renderChartIntegrity(){
  const statusEl = document.getElementById('chartIntegrityStatus');
  const summaryEl = document.getElementById('chartIntegritySummary');
  const detailEl = document.getElementById('chartIntegrityDetail');
  const tagsEl = document.getElementById('chartIntegrityTags');
  const notesEl = document.getElementById('chartIntegrityNotes');
  if(!statusEl || !summaryEl || !detailEl || !tagsEl || !notesEl) return;
  const selected = _currentAssetDescriptor;
  const intervalSec = intervalSeconds(_chartTfUsed || currentTf) || intervalSeconds(currentTf) || 0;
  const last = _chartCandles.length ? _chartCandles[_chartCandles.length - 1] : null;
  const lastAgeSec = last && Number.isFinite(Number(last.time)) ? Math.max(0, Math.floor(Date.now() / 1000 - Number(last.time))) : null;
  let integrityState = 'Waiting', integrityColor = 'var(--bl)';
  if(_chartMode === 'history'){ integrityState = 'History'; integrityColor = 'var(--pu)'; }
  else if(_chartCandles.length && lastAgeSec != null && intervalSec && lastAgeSec <= intervalSec * 2){ integrityState = _chartLiveOverlayAllowed ? 'Live merge' : 'Line only'; integrityColor = _chartLiveOverlayAllowed ? 'var(--gr)' : 'var(--am)'; }
  else if(_chartCandles.length){ integrityState = 'Lag watch'; integrityColor = 'var(--am)'; }
  statusEl.textContent = integrityState;
  statusEl.style.background = integrityColor === 'var(--gr)' ? 'rgba(0,208,132,.12)' : integrityColor === 'var(--pu)' ? 'rgba(179,136,255,.15)' : 'rgba(255,165,0,.15)';
  statusEl.style.color = integrityColor;
  summaryEl.innerHTML = [
    {label:'Mode', value:_chartMode === 'history' ? 'History' : 'Live', color:_chartMode === 'history' ? 'var(--pu)' : 'var(--cy)'},
    {label:'Bars Loaded', value:String(_chartCandles.length || 0), color:_chartCandles.length ? 'var(--tx)' : 'var(--tx2)'},
    {label:'History Spine', value:_chartHistorySource || '—', color:_chartHistorySource ? 'var(--cy)' : 'var(--tx2)'},
    {label:'Live Source', value:_chartLiveSource || '—', color:_chartLiveSource ? 'var(--gr)' : 'var(--tx2)'},
    {label:'Candle State', value:!_chartCandles.length ? 'Waiting' : lastAgeSec != null && intervalSec && lastAgeSec <= intervalSec * 2 ? 'Confirmed' : 'Stale', color:lastAgeSec != null && intervalSec && lastAgeSec <= intervalSec * 2 ? 'var(--gr)' : !_chartCandles.length ? 'var(--bl)' : 'var(--am)'},
    {label:'Last Bar Age', value:lastAgeSec == null ? '—' : formatRelativeAge(lastAgeSec), color:lastAgeSec != null && intervalSec && lastAgeSec <= intervalSec * 2 ? 'var(--gr)' : lastAgeSec == null ? 'var(--tx2)' : 'var(--am)'}
  ].map(item => '<div class="dp-stat"><div class="dp-val" style="color:'+item.color+'">'+item.value+'</div><div class="dp-lbl">'+item.label+'</div></div>').join('');
  detailEl.textContent = selected ? selected.symbol + ' is charting on ' + (_chartTfUsed || currentTf) + ' with ' + (_chartHistorySource || selected.primary_provider || 'Unknown') + ' as the history spine and ' + (_chartLiveSource || selected.primary_provider || 'Unknown') + ' as the live source.' : 'Chart status unavailable';
  tagsEl.innerHTML = [selected?.primary_provider ? '<div class="dp-tag">'+selected.primary_provider+' primary</div>' : '', selected?.secondary_provider ? '<div class="dp-tag">'+selected.secondary_provider+' fallback</div>' : '', _chartProviderFamily ? '<div class="dp-tag">'+_chartProviderFamily+' family</div>' : '', _chartTfUsed ? '<div class="dp-tag">'+_chartTfUsed+' rendered</div>' : '', _chartProviderWarning ? '<div class="dp-tag">provider constrained</div>' : ''].filter(Boolean).join('');
  notesEl.innerHTML = '<div class="event-risk-card"><div class="event-risk-title">Current state</div><div class="event-risk-text">' + (_chartMode === 'history' ? 'Deep-history mode is active.' : _chartCandles.length ? (_chartLiveOverlayAllowed ? 'The live candle is being merged into the chart.' : 'Live price is shown as a price-line only.') : 'The chart is waiting for candles.') + '</div></div>';
}
function renderMarketStatePanels(runtimeStatus=null, selectedSignal=null){
  const statusEl = document.getElementById('marketStateStatus');
  const panelEl = document.getElementById('marketStatePanel');
  const notesEl = document.getElementById('marketStateNotes');
  const levelStatusEl = document.getElementById('levelMapStatus');
  const levelPanelEl = document.getElementById('levelMapPanel');
  const levelNotesEl = document.getElementById('levelMapNotes');
  const crossStatusEl = document.getElementById('crossMarketStatus');
  const crossPanelEl = document.getElementById('crossMarketPanel');
  const crossNotesEl = document.getElementById('crossMarketNotes');
  if(!statusEl||!panelEl||!notesEl||!levelStatusEl||!levelPanelEl||!levelNotesEl||!crossStatusEl||!crossPanelEl||!crossNotesEl) return;
  const selected = _currentAssetDescriptor;
  const signal = selectedSignal || fetchPrimarySignal(runtimeStatus || {}) || null;
  const meta = signalMeta(signal);
  const candles = Array.isArray(_chartCandles) ? _chartCandles : [];
  const last = candles.length ? candles[candles.length - 1] : null;
  const lookback = candles.slice(-24);
  const highs = lookback.map(c => Number(c.high || 0)).filter(Number.isFinite);
  const lows = lookback.map(c => Number(c.low || 0)).filter(Number.isFinite);
  const high = highs.length ? Math.max.apply(null, highs) : null;
  const low = lows.length ? Math.min.apply(null, lows) : null;
  const rangeMid = high != null && low != null ? (high + low) / 2 : null;
  const session = selected ? humanizeChartToken((runtimeStatus?.provider_routing?.summary_label || selected.primary_provider || 'unknown')) : 'Unavailable';
  const readiness = signal ? entryReadiness(signal) : {state:'neutral',label:'Context only',detail:'signal pending'};
  const confirm = signal ? confirmationState(signal) : {state:'neutral',label:'Unknown',detail:'confirmation pending'};
  const sweep = boolValue(meta.liquidity_sweep_reclaim, signal?.liquidity_sweep_reclaim);
  const failedOpp = boolValue(meta.failed_opposite_move_confirmed, signal?.failed_opposite_move_confirmed);
  statusEl.textContent = selected ? 'Live' : 'Waiting';
  statusEl.style.background = selected ? 'rgba(0,208,132,.12)' : 'rgba(41,121,255,.15)';
  statusEl.style.color = selected ? 'var(--gr)' : 'var(--bl)';
  panelEl.innerHTML = [
    {label:'Asset', value:selected?.symbol || '—', color:'var(--cy)'},
    {label:'Session / Spine', value:session, color:'var(--gr)'},
    {label:'Entry status', value:readiness.label, color:readiness.state === 'ready' ? 'var(--gr)' : readiness.state === 'waiting' ? 'var(--am)' : readiness.state === 'blocked' ? 'var(--rd)' : 'var(--tx2)'},
    {label:'Confirmation', value:confirm.label, color:confirm.state === 'ready' ? 'var(--gr)' : confirm.state === 'waiting' ? 'var(--am)' : 'var(--tx2)'},
    {label:'Pattern family', value:presentLabel(meta.pattern_family || signal?.pattern_family, '—'), color:'var(--pu)'},
    {label:'Elite rank', value:presentLabel(meta.elite_pattern_rank || signal?.elite_pattern_rank, '—'), color:'var(--cy)'},
    {label:'Retest / Pullback', value:(boolValue(meta.breakout_retest_ready, signal?.breakout_retest_ready) ? 'R' : '—') + ' / ' + (boolValue(meta.first_pullback_ready, signal?.first_pullback_ready) ? 'P' : '—'), color:'var(--bl)'},
    {label:'Sweep / Reclaim', value:(sweep ? 'Sweep' : '—') + ' / ' + (failedOpp ? 'Fail-opposite' : '—'), color:sweep || failedOpp ? 'var(--gr)' : 'var(--tx2)'}
  ].map(item => '<div class="dp-stat"><div class="dp-val" style="color:'+item.color+'">'+item.value+'</div><div class="dp-lbl">'+item.label+'</div></div>').join('');
  notesEl.innerHTML = '<div class="event-risk-card"><div class="event-risk-title">Desk read</div><div class="event-risk-text">' + (signal ? 'Market context is live, but entry qualification is ' + readiness.label.toLowerCase() + '. ' + readiness.detail + '.' : 'Waiting for a selected signal to distinguish context from execution readiness.') + '</div></div>';
  levelStatusEl.textContent = candles.length ? candles.length + ' bars' : 'Waiting';
  levelStatusEl.style.background = candles.length ? 'rgba(0,208,132,.12)' : 'rgba(41,121,255,.15)';
  levelStatusEl.style.color = candles.length ? 'var(--gr)' : 'var(--bl)';
  levelPanelEl.innerHTML = [
    {label:'Range high', value:high == null ? '—' : formatAssetPrice(high), color:'var(--gr)'},
    {label:'Range low', value:low == null ? '—' : formatAssetPrice(low), color:'var(--rd)'},
    {label:'Current', value:last ? formatAssetPrice(last.close) : '—', color:'var(--tx)'},
    {label:'Mid / value', value:rangeMid == null ? '—' : formatAssetPrice(rangeMid), color:'var(--cy)'},
    {label:'Extension score', value:numValue(meta.extension_score, signal?.extension_score) == null ? '—' : Number(numValue(meta.extension_score, signal?.extension_score)).toFixed(2), color:(numValue(meta.extension_score, signal?.extension_score) || 0) > 0.75 ? 'var(--rd)' : (numValue(meta.extension_score, signal?.extension_score) || 0) > 0.45 ? 'var(--am)' : 'var(--gr)'},
    {label:'Target efficiency', value:numValue(meta.target_efficiency_score, signal?.target_efficiency_score) == null ? '—' : Number(numValue(meta.target_efficiency_score, signal?.target_efficiency_score) * 100).toFixed(0)+'%', color:(numValue(meta.target_efficiency_score, signal?.target_efficiency_score) || 0) >= 0.6 ? 'var(--gr)' : (numValue(meta.target_efficiency_score, signal?.target_efficiency_score) || 0) >= 0.4 ? 'var(--am)' : 'var(--rd)'},
    {label:'Impulse age', value:numValue(meta.impulse_age_bars, signal?.impulse_age_bars) == null ? '—' : String(Math.round(numValue(meta.impulse_age_bars, signal?.impulse_age_bars))), color:'var(--pu)'},
    {label:'Kill reason', value:exactKillReason(signal) || 'None', color:exactKillReason(signal) ? 'var(--rd)' : 'var(--gr)'}
  ].map(item => '<div class="dp-stat"><div class="dp-val" style="color:'+item.color+'">'+item.value+'</div><div class="dp-lbl">'+item.label+'</div></div>').join('');
  levelNotesEl.innerHTML = '<div class="event-risk-card"><div class="event-risk-title">Level read</div><div class="event-risk-text">' + (signal ? 'Context levels are separate from execution gates. Extension is ' + (numValue(meta.extension_score, signal.extension_score) == null ? 'not published' : Number(numValue(meta.extension_score, signal.extension_score)).toFixed(2)) + ' and target efficiency is ' + (numValue(meta.target_efficiency_score, signal.target_efficiency_score) == null ? 'not published' : Number(numValue(meta.target_efficiency_score, signal.target_efficiency_score) * 100).toFixed(0) + '%') + '.' : 'Levels will populate once recent candles and a tracked setup are available.') + '</div></div>';
  const related = (_heatmapItems || []).filter(item => item.asset !== currentAsset).slice(0,4);
  crossStatusEl.textContent = related.length ? related.length + ' peers' : 'Waiting';
  crossStatusEl.style.background = related.length ? 'rgba(0,208,132,.12)' : 'rgba(41,121,255,.15)';
  crossStatusEl.style.color = related.length ? 'var(--gr)' : 'var(--bl)';
  crossPanelEl.innerHTML = related.length ? related.map(item => '<div class="dp-stat"><div class="dp-val" style="color:'+(Number(item.change_pct || 0) >= 0 ? 'var(--gr)' : 'var(--rd)')+'">'+item.asset.replace('-USD','').replace('/USD','').replace('=F','')+'</div><div class="dp-lbl">'+(item.change_pct == null ? 'live' : (Number(item.change_pct || 0) >= 0 ? '+' : '') + Number(item.change_pct || 0).toFixed(2)+'%')+'</div></div>').join('') : '<div class="dp-stat"><div class="dp-val">—</div><div class="dp-lbl">No peers</div></div>';
  crossNotesEl.innerHTML = '<div class="event-risk-card"><div class="event-risk-title">Cross-market read</div><div class="event-risk-text">' + (signal ? 'Cross-market context can support the tape, but only retest / pullback readiness plus confirmation qualifies an entry.' : 'Cross-market peers will appear after the heatmap loads.') + '</div></div>';
}
function getAssetDescriptor(assetPayload, symbol){
  const items = Array.isArray(assetPayload?.assets) ? assetPayload.assets : [];
  return items.find(a => a.symbol === symbol) || null;
}
function formatQuoteMode(value){ return String(value || '').toLowerCase() === 'polling' ? 'Polling' : 'Stream'; }
async function loadAssets(){
  const page = await fetchMarketIntelligenceOverview();
  const d = page.success ? (page.assets || {assets:[]}) : await fetchWithTimeout('/api/chart/assets', 8000) || {assets:[]};
  const sel = document.getElementById('chartAsset');
  if(!sel) return;
  const grouped = {};
  (d.assets || []).forEach(a => { (grouped[a.category] || (grouped[a.category] = [])).push(a); });
  const groupEntries = Object.entries(grouped);
  if(!groupEntries.length){ sel.innerHTML = '<option value="">Asset list unavailable</option>'; return; }
  sel.innerHTML = groupEntries.map(([cat, list]) => '<optgroup label="'+cat+'">' + list.map(a => '<option value="'+a.symbol+'" '+(a.symbol === currentAsset ? 'selected' : '')+'>'+a.symbol+'</option>').join('') + '</optgroup>').join('');
  const hasCurrent = (d.assets || []).some(a => a.symbol === currentAsset);
  if(!hasCurrent && d.assets && d.assets.length){ currentAsset = d.assets[0].symbol; sel.value = currentAsset; }
}
async function loadDataPlane(){
  const status = document.getElementById('dataPlaneStatus');
  const summaryEl = document.getElementById('dataPlaneSummary');
  const detailEl = document.getElementById('dataPlaneDetail');
  const tagsEl = document.getElementById('dataPlaneTags');
  const eventEl = document.getElementById('eventRiskPanel');
  const signalStatusEl = document.getElementById('signalContextStatus');
  const signalSummaryEl = document.getElementById('signalContextSummary');
  const signalDetailEl = document.getElementById('signalContextDetail');
  const signalTagsEl = document.getElementById('signalContextTags');
  const signalNotesEl = document.getElementById('signalContextNotes');
  status.textContent = 'Loading…'; status.style.background = 'rgba(41,121,255,.15)'; status.style.color = 'var(--bl)';
  signalStatusEl.textContent = 'Loading…'; signalStatusEl.style.background = 'rgba(41,121,255,.15)'; signalStatusEl.style.color = 'var(--bl)';
  const page = await fetchMarketIntelligenceOverview();
  const assetPayload = page.success ? (page.assets || {assets:[]}) : await fetchWithTimeout('/api/chart/assets', 8000) || {assets:[]};
  const eventPayload = page.success ? (page.events || {success:false, events:[], risk_outlook:{}}) : await fetchWithTimeout('/api/market/events', 15000) || {success:false, events:[], risk_outlook:{}};
  const runtimeStatus = page.success ? (page.status || {}) : {};
  const commandCenter = page.success ? (page.command_center || {}) : {};
  const signal = fetchPrimarySignal(page);
  const meta = signalMeta(signal);
  const assets = assetPayload.assets || [];
  const selected = getAssetDescriptor(assetPayload, currentAsset) || assets[0] || null;
  _currentAssetDescriptor = selected;
  if (_chartBooted) syncChartPricePrecision(null, selected ? selected.symbol : currentAsset, selected);
  const grouped = assets.reduce((acc, item) => { acc[item.category] = (acc[item.category] || 0) + 1; return acc; }, {});
  summaryEl.innerHTML = [
    {label:'Active Assets', value:String(assets.length), color:'var(--cy)'},
    {label:'Selected Asset', value:selected ? selected.symbol : '—', color:'var(--tx)'},
    {label:'Primary Feed', value:selected ? selected.primary_provider : '—', color:'var(--gr)'},
    {label:'Quote Mode', value:selected ? formatQuoteMode(selected.quote_mode) : '—', color:'var(--bl)'}
  ].map(item => '<div class="dp-stat"><div class="dp-val" style="color:'+item.color+'">'+item.value+'</div><div class="dp-lbl">'+item.label+'</div></div>').join('');
  detailEl.textContent = selected ? selected.symbol + ' (' + selected.category + ') uses ' + selected.primary_provider + ' as the primary chart feed.' + (selected.secondary_provider ? ' ' + selected.secondary_provider + ' is kept as fallback.' : ' No secondary fallback is configured for this asset.') : 'Asset universe unavailable right now.';
  tagsEl.innerHTML = Object.entries(grouped).map(([category, count]) => '<div class="dp-tag">'+category+' '+count+'</div>').join('');
  const risk = eventPayload.risk_outlook || {};
  const events = eventPayload.events || [];
  eventEl.innerHTML = '<div class="event-risk-card"><div class="event-risk-title">Event Risk Outlook</div><div class="event-risk-text" style="color:'+((risk.reduce_trading ? 'var(--rd)' : (events.length ? 'var(--am)' : 'var(--gr')))+'">'+(risk.summary || (risk.reduce_trading ? 'Reduce risk around the next high-impact macro windows.' : events.length ? events.length+' scheduled events on deck.' : 'No elevated event-risk flags right now.'))+'</div></div>' + (events.length ? events.slice(0,4).map(event => '<div class="event-item"><div class="event-name">'+(event.title || event.event || 'No scheduled events')+'</div><div class="event-meta">'+(event.time || event.date || '')+(event.impact ? ' · ' + event.impact : '')+'</div></div>').join('') : '<div class="event-item"><div class="event-name">No scheduled events</div><div class="event-meta">Quiet macro slate</div></div>');
  status.textContent = assets.length ? assets.length + ' assets' : 'Unavailable';
  status.style.background = assets.length ? 'rgba(0,208,132,.12)' : 'rgba(255,69,96,.12)';
  status.style.color = assets.length ? 'var(--gr)' : 'var(--rd)';
  const readiness = signal ? entryReadiness(signal) : {state:'neutral',label:'Context only',detail:'signal pending'};
  const confirm = signal ? confirmationState(signal) : {state:'neutral',label:'Unknown',detail:'confirmation pending'};
  const entryCount = signal ? signalNum(signal, 'entry_confirmation_count', 0) : 0;
  const entryRequired = signal ? signalNum(signal, 'entry_confirmation_bars_required', 0) : 0;
  const policyBits = signal ? regimeBits(signal) : [];
  const reviewNotes = signal ? reviewNotesLine(signal) : '';
  signalSummaryEl.innerHTML = [
    {label:'Entry status', value:readiness.label, color:readiness.state === 'ready' ? 'var(--gr)' : readiness.state === 'waiting' ? 'var(--am)' : readiness.state === 'blocked' ? 'var(--rd)' : 'var(--tx2)'},
    {label:'Confirmation', value:confirm.label, color:confirm.state === 'ready' ? 'var(--gr)' : confirm.state === 'waiting' ? 'var(--am)' : 'var(--tx2)'},
    {label:'Entry confirmations', value:entryRequired ? `${entryCount}/${entryRequired}` : String(entryCount || '—'), color:confirm.state === 'waiting' ? 'var(--am)' : 'var(--gr)'},
    {label:'Retest / Pullback', value:(boolValue(meta.breakout_retest_ready, signal?.breakout_retest_ready) ? 'R' : '—') + ' / ' + (boolValue(meta.first_pullback_ready, signal?.first_pullback_ready) ? 'P' : '—'), color:'var(--bl)'},
    {label:'Sweep / Failed Opp', value:(boolValue(meta.liquidity_sweep_reclaim, signal?.liquidity_sweep_reclaim) ? 'Sweep' : '—') + ' / ' + (boolValue(meta.failed_opposite_move_confirmed, signal?.failed_opposite_move_confirmed) ? 'Confirmed' : '—'), color:(boolValue(meta.liquidity_sweep_reclaim, signal?.liquidity_sweep_reclaim) || boolValue(meta.failed_opposite_move_confirmed, signal?.failed_opposite_move_confirmed)) ? 'var(--gr)' : 'var(--tx2)'},
    {label:'Pattern family', value:presentLabel(meta.pattern_family || signal?.pattern_family, '—'), color:'var(--pu)'},
    {label:'Elite rank', value:presentLabel(meta.elite_pattern_rank || signal?.elite_pattern_rank, '—'), color:'var(--cy)'},
    {label:'Policy', value:policyBits[0] || '—', color:policyBits.length ? 'var(--rd)' : 'var(--tx2)'},
    {label:'Impulse age', value:numValue(meta.impulse_age_bars, signal?.impulse_age_bars) == null ? '—' : String(Math.round(numValue(meta.impulse_age_bars, signal?.impulse_age_bars))), color:'var(--am)'},
    {label:'Extension / Target', value:(numValue(meta.extension_score, signal?.extension_score) == null ? '—' : Number(numValue(meta.extension_score, signal?.extension_score)).toFixed(2)) + ' / ' + (numValue(meta.target_efficiency_score, signal?.target_efficiency_score) == null ? '—' : Number(numValue(meta.target_efficiency_score, signal?.target_efficiency_score) * 100).toFixed(0)+'%'), color:'var(--cy)'}
  ].map(item => '<div class="dp-stat"><div class="dp-val" style="color:'+item.color+'">'+item.value+'</div><div class="dp-lbl">'+item.label+'</div></div>').join('');
  signalDetailEl.textContent = signal ? 'Market context is separate from entry-qualified status. ' + (selected ? selected.symbol : currentAsset) + ' is currently ' + readiness.label.toLowerCase() + ' with confirmation ' + confirm.label.toLowerCase() + (entryRequired ? ` (${entryCount}/${entryRequired} bars)` : '') + '.' : 'Signal context unavailable.';
  signalTagsEl.innerHTML = [
    statusChip(readiness.state, readiness.label),
    statusChip(confirm.state === 'neutral' ? 'neutral' : confirm.state, 'Confirm ' + confirm.label),
    entryRequired ? `<div class="dp-tag">confirm ${entryCount}/${entryRequired}</div>` : (entryCount ? `<div class="dp-tag">confirm ${entryCount}</div>` : ''),
    boolValue(meta.breakout_retest_ready, signal?.breakout_retest_ready) ? '<div class="dp-tag">retest ready</div>' : '',
    boolValue(meta.first_pullback_ready, signal?.first_pullback_ready) ? '<div class="dp-tag">pullback ready</div>' : '',
    boolValue(meta.liquidity_sweep_reclaim, signal?.liquidity_sweep_reclaim) ? '<div class="dp-tag">sweep reclaim</div>' : '',
    boolValue(meta.failed_opposite_move_confirmed, signal?.failed_opposite_move_confirmed) ? '<div class="dp-tag">failed opposite confirmed</div>' : '',
    policyBits.length ? '<div class="dp-tag">'+policyBits[0]+'</div>' : '',
    exactKillReason(signal) ? '<div class="dp-tag">kill: '+exactKillReason(signal)+'</div>' : '',
    reviewNotes ? '<div class="dp-tag">notes: '+reviewNotes.slice(0,40)+'</div>' : ''
  ].filter(Boolean).join('');
  signalNotesEl.innerHTML = '<div class="event-risk-card"><div class="event-risk-title">Operator lens</div><div class="event-risk-text">' + (signal ? 'Context can be constructive while execution is still blocked. Current state: ' + readiness.detail + '. ' + (policyBits.length ? policyBits.join(' · ') + '. ' : '') + (reviewNotes ? 'Review notes: ' + reviewNotes + '. ' : '') + (exactKillReason(signal) ? 'Kill reason: ' + exactKillReason(signal) + '.' : '') : 'No tracked setup is attached to the selected asset yet.') + '</div></div><div class="event-item"><div class="event-name">Pattern</div><div class="event-meta">'+presentLabel(meta.pattern_family || signal?.pattern_family, 'N/A')+' · rank '+presentLabel(meta.elite_pattern_rank || signal?.elite_pattern_rank, '—')+'</div></div><div class="event-item"><div class="event-name">Liquidity read</div><div class="event-meta">'+(boolValue(meta.liquidity_sweep_reclaim, signal?.liquidity_sweep_reclaim) ? 'Sweep/reclaim detected' : 'No sweep reclaim published')+' · '+(boolValue(meta.failed_opposite_move_confirmed, signal?.failed_opposite_move_confirmed) ? 'failed opposite confirmed' : 'failed opposite not confirmed')+'</div></div><div class="event-item"><div class="event-name">Kill reason</div><div class="event-meta">'+(exactKillReason(signal) || 'None published')+'</div></div><div class="event-item"><div class="event-name">Review notes</div><div class="event-meta">'+(reviewNotes || 'None published')+'</div></div>';
  signalStatusEl.textContent = signal ? readiness.label : 'Idle';
  signalStatusEl.style.background = readiness.state === 'ready' ? 'rgba(0,208,132,.12)' : readiness.state === 'waiting' ? 'rgba(255,165,0,.15)' : readiness.state === 'blocked' ? 'rgba(255,69,96,.12)' : 'rgba(122,136,153,.15)';
  signalStatusEl.style.color = readiness.state === 'ready' ? 'var(--gr)' : readiness.state === 'waiting' ? 'var(--am)' : readiness.state === 'blocked' ? 'var(--rd)' : 'var(--tx2)';
  renderChartIntegrity();
  renderMarketStatePanels(runtimeStatus, signal);
}
async function loadCandles(){
  if(_chartMode !== 'live' || candlesLoading) return;
  candlesLoading = true;
  const st = document.getElementById('chartStatus');
  st.textContent = 'Loading…'; st.style.background = 'rgba(41,121,255,.15)'; st.style.color = 'var(--bl)';
  const d = await fetchWithTimeout('/api/chart/candles?asset=' + encodeURIComponent(currentAsset) + '&interval=' + currentTf, 25000);
  candlesLoading = false;
  if(d && d.candles && d.candles.length){
    _lastLiveReloadAt = Date.now();
    _chartCandles = d.candles.slice();
    _chartTfUsed = d.interval_used || currentTf;
    _chartLiveOverlayAllowed = d.live_overlay_allowed !== false;
    _chartLiveSource = String(d.live_price_source || '');
    _chartHistorySource = String(d.data_source || '');
    _chartHistorySourceClass = String(d.data_source_class || '');
    _chartProviderFamily = String(d.provider_family || '');
    _chartProviderWarning = String(d.provider_warning_message || '');
    _historyCursor = _chartCandles.length ? Number(_chartCandles[0].time) : null;
    _historyHasMore = false;
    updateHistoryButtons();
    clearLivePriceLine();
    syncChartPricePrecision(_chartCandles, currentAsset, _currentAssetDescriptor);
    cSeries.setData(_chartCandles);
    vSeries.setData(_chartCandles.map(volumeBarFor));
    lwChart.timeScale().fitContent();
    st.textContent = d.candles.length + ' bars · ' + _chartTfUsed + (_chartHistorySource ? ' · ' + _chartHistorySource : '');
    st.style.background = 'rgba(0,208,132,.12)'; st.style.color = 'var(--gr)';
  }else{
    _chartCandles = [];
    st.textContent = String(d?.message || ('No ' + currentTf + ' data')).slice(0,72);
    st.style.background = 'rgba(255,69,96,.12)'; st.style.color = 'var(--rd)';
  }
  renderChartIntegrity();
}
async function loadHistoryCandles(opts = {}){
  if(_chartMode !== 'history' || candlesLoading) return;
  candlesLoading = true;
  updateHistoryButtons();
  const st = document.getElementById('chartStatus');
  st.textContent = opts.append ? 'Loading older history…' : 'Loading history…';
  st.style.background = 'rgba(41,121,255,.15)'; st.style.color = 'var(--bl)';
  clearLivePriceLine();
  const bars = Number(document.getElementById('historyBars')?.value || 500);
  const resolvedEnd = opts.endTime || historyEndTimeFromInput();
  const params = new URLSearchParams({asset:currentAsset,interval:currentTf,bars:String(bars)});
  if(resolvedEnd) params.set('end_time', resolvedEnd);
  const d = await fetchWithTimeout('/api/chart/history?' + params.toString(), 40000);
  candlesLoading = false;
  if(d && d.candles && d.candles.length){
    const incoming = d.candles.slice();
    _chartCandles = opts.append ? mergeCandles(_chartCandles, incoming) : incoming;
    _chartTfUsed = d.interval_used || currentTf;
    _chartLiveOverlayAllowed = false;
    _chartLiveSource = '';
    _chartHistorySource = String(d.data_source || '');
    _chartHistorySourceClass = String(d.data_source_class || '');
    _chartProviderFamily = String(d.provider_family || '');
    _chartProviderWarning = String(d.provider_warning_message || '');
    _historyCursor = d.oldest_time || (_chartCandles.length ? Number(_chartCandles[0].time) : null);
    _historyHasMore = !!d.has_more;
    updateHistoryButtons();
    syncChartPricePrecision(_chartCandles, currentAsset, _currentAssetDescriptor);
    cSeries.setData(_chartCandles);
    vSeries.setData(_chartCandles.map(volumeBarFor));
    lwChart.timeScale().fitContent();
    st.textContent = _chartCandles.length + ' bars · ' + _chartTfUsed + (_chartHistorySource ? ' · ' + _chartHistorySource : '') + (_historyHasMore ? ' · older available' : ' · start reached');
    st.style.background = 'rgba(0,208,132,.12)'; st.style.color = 'var(--gr)';
  }else{
    _historyHasMore = false;
    updateHistoryButtons();
    st.textContent = String(d?.message || ('No ' + currentTf + ' history')).slice(0,72);
    st.style.background = 'rgba(255,69,96,.12)'; st.style.color = 'var(--rd)';
  }
  renderChartIntegrity();
}
async function startStream(){
  if(!_chartBooted || _chartMode !== 'live') return;
  if(sseSource) sseSource.close();
  const livePriceEl = document.getElementById('livePrice');
  if(livePriceEl) livePriceEl.textContent = '—';
  let token = '';
  try{
    if(typeof window.dashboardGetApiToken === 'function'){
      token = await window.dashboardGetApiToken(false) || '';
    }
  }catch(err){ token = ''; }
  const params = new URLSearchParams({asset: currentAsset});
  if(token) params.set('token', token);
  sseSource = new EventSource('/api/chart/stream?' + params.toString());
  sseSource.onmessage = e => {
    try{
      const d = JSON.parse(e.data);
      if(d.type === 'tick'){
        const price = Number(d.price);
        const tickTs = Number(d.ts || 0);
        _chartLiveSource = String(d.source || _chartLiveSource || '');
        if(livePriceEl) livePriceEl.textContent = price ? formatChartPrice(price) : '—';
        if(price){
          if(_chartLiveOverlayAllowed) applyLiveTickToChart(price, tickTs);
          else setLivePriceLine(price);
        }
      }
    }catch(err){}
  };
  sseSource.onerror = () => {
    try{ sseSource.close(); }catch(err){}
    sseSource = null;
    if(_chartMode === 'live' && !document.hidden){
      setTimeout(() => { startStream(); }, 3000);
    }
  };
}
async function loadHeatmap(manual=false){
  const grid = document.getElementById('heatmapGrid');
  const st = document.getElementById('hmStatus');
  if(manual){ grid.innerHTML = '<div class="hm-cell skeleton sk-cell"></div>'.repeat(6); st.textContent = 'Loading…'; st.style.background = 'rgba(41,121,255,.15)'; st.style.color = 'var(--bl)'; }
  const d = await fetchWithTimeout('/api/market/heatmap', 40000);
  if(!d || !d.items || !d.items.length){
    _heatmapItems = [];
    grid.innerHTML = '<div class="empty" style="grid-column:span 5">Price data loading — check back in 30s</div>';
    st.textContent = 'Unavailable'; st.style.background = 'rgba(255,69,96,.12)'; st.style.color = 'var(--rd)';
    return;
  }
  _heatmapItems = Array.isArray(d.items) ? d.items.slice() : [];
  const numericMoves = d.items.map(i => i && i.change_pct !== null && i.change_pct !== undefined ? Number(i.change_pct) : NaN).filter(v => Number.isFinite(v));
  const max = Math.max.apply(null, numericMoves.map(v => Math.abs(v)).concat([0.1]));
  grid.innerHTML = d.items.map(i => {
    const hasPct = i && i.change_pct !== null && i.change_pct !== undefined && Number.isFinite(Number(i.change_pct));
    const pct = hasPct ? Number(i.change_pct) : null;
    const inten = hasPct ? Math.min(1, Math.abs(pct) / max) : 0;
    const bg = !hasPct ? 'rgba(122,136,153,.12)' : pct >= 0 ? 'rgba(0,208,132,' + (0.08 + inten * 0.5) + ')' : 'rgba(255,69,96,' + (0.08 + inten * 0.5) + ')';
    const tc = !hasPct ? 'var(--tx2)' : pct >= 0 ? 'var(--gr)' : 'var(--rd)';
    return '<div class="hm-cell" style="background:'+bg+'" onclick="changeToAsset(\''+i.asset+'\')"><div class="hm-asset" style="color:var(--tx)">'+i.asset.replace('-USD','').replace('/USD','').replace('=F','').replace('^','')+'</div><div class="hm-pct" style="color:'+tc+'">'+(hasPct ? (pct >= 0 ? '+' : '') + pct.toFixed(2)+'%' : 'LIVE')+'</div><div class="hm-price">'+formatAssetPrice(i.price, i.asset, {})+'</div></div>';
  }).join('');
  st.textContent = d.items.length + ' assets';
  st.style.background = 'rgba(0,208,132,.12)'; st.style.color = 'var(--gr)';
}
let corrAttempts = 0;
async function loadCorrelation(manual=false){
  const wrap = document.getElementById('corrWrap');
  const st = document.getElementById('corrStatus');
  if(manual) corrAttempts = 0;
  st.textContent = 'Loading…'; st.style.background = 'rgba(41,121,255,.15)'; st.style.color = 'var(--bl)';
  const d = await fetchWithTimeout('/api/correlation-matrix', 60000);
  corrAttempts++;
  if(!d || !d.success || !Array.isArray(d.labels) || !Array.isArray(d.matrix) || !d.labels.length || !d.matrix.length){
    st.textContent = corrAttempts < 3 ? 'Loading '+corrAttempts+'/3…' : 'Unavailable';
    st.style.background = corrAttempts < 3 ? 'rgba(255,165,0,.15)' : 'rgba(255,69,96,.12)';
    st.style.color = corrAttempts < 3 ? 'var(--am)' : 'var(--rd)';
    wrap.innerHTML = '<div class="empty">'+(corrAttempts < 3 ? 'Fetching price history… retry '+corrAttempts+'/3' : 'Unavailable — needs 30d price history')+'</div>';
    if(corrAttempts < 3) setTimeout(() => loadCorrelation(), 15000);
    return;
  }
  const cols = '48px ' + d.labels.map(() => '1fr').join(' ');
  let html = '<div class="corr-grid" style="grid-template-columns:'+cols+'"><div></div>';
  d.labels.forEach(l => html += '<div class="corr-cell" style="font-size:9px;color:var(--tx3);height:20px;background:none">'+l.replace('-USD','').replace('/USD','').replace('=F','').replace('^','')+'</div>');
  d.matrix.forEach((row, i) => {
    html += '<div class="corr-label">'+d.labels[i].replace('-USD','').replace('/USD','').replace('=F','').replace('^','')+'</div>';
    row.forEach((val, j) => {
      const valid = val !== null && val !== undefined && val !== '' && Number.isFinite(Number(val));
      const v = valid ? Number(val) : 0;
      const abs = Math.abs(v);
      const bg = !valid ? 'rgba(122,136,153,.12)' : i === j ? 'rgba(41,121,255,.2)' : v > 0 ? 'rgba(0,208,132,'+(abs * 0.8)+')' : 'rgba(255,69,96,'+(abs * 0.8)+')';
      const tc = !valid ? 'var(--tx3)' : i === j ? 'var(--bl)' : abs > 0.5 ? (v > 0 ? 'var(--gr)' : 'var(--rd)') : 'var(--tx2)';
      html += '<div class="corr-cell" style="background:'+bg+';color:'+tc+'">'+(valid ? v.toFixed(2) : '—')+'</div>';
    });
  });
  html += '</div>';
  wrap.innerHTML = html;
  st.textContent = d.labels.length + ' assets';
  st.style.background = 'rgba(0,208,132,.12)'; st.style.color = 'var(--gr)';
}
(function initResize(){
  const handle = document.getElementById('resizeHandle');
  const panel = document.getElementById('chartPanel');
  let startY, startH;
  handle.addEventListener('mousedown', e => {
    startY = e.clientY; startH = panel.offsetHeight; document.body.style.cursor = 'ns-resize'; document.body.style.userSelect = 'none';
    function onMove(ev){ const newH = Math.max(200, Math.min(800, startH + (ev.clientY - startY))); panel.style.height = newH + 'px'; if(lwChart){ const c = document.getElementById('lwChart'); lwChart.resize(c.clientWidth, c.clientHeight); } }
    function onUp(){ document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp); document.body.style.cursor = ''; document.body.style.userSelect = ''; }
    document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp);
  });
})();
function changeToAsset(a){ currentAsset = a; syncChartPricePrecision(null, currentAsset, null); const sel = document.getElementById('chartAsset'); for(let i=0;i<sel.options.length;i++){ if(sel.options[i].value === a){ sel.selectedIndex = i; break; } } loadDataPlane(); if(_chartBooted){ if(_chartMode === 'history') loadHistoryCandles({append:false}); else { loadCandles(); startStream(); } } }
function changeChartAsset(){ currentAsset = document.getElementById('chartAsset').value; syncChartPricePrecision(null, currentAsset, null); loadDataPlane(); if(_chartBooted){ if(_chartMode === 'history') loadHistoryCandles({append:false}); else { loadCandles(); startStream(); } } }
function setTf(tf, el){ currentTf = tf; document.querySelectorAll('.chart-controls .tb-btn').forEach(b => b.classList.remove('on')); el.classList.add('on'); if(_chartMode === 'history') loadHistoryCandles({append:false}); else loadCandles(); }
function setChartMode(mode){ _chartMode = mode === 'history' ? 'history' : 'live'; updateChartModeUi(); if(sseSource){ sseSource.close(); sseSource = null; } if(!_chartBooted) return; if(_chartMode === 'history') loadHistoryCandles({append:false}); else { loadCandles(); startStream(); } }
function onHistoryBarsChanged(){ if(_chartMode === 'history') loadHistoryCandles({append:false}); }
function jumpHistory(){ if(_chartMode !== 'history'){ setChartMode('history'); return; } loadHistoryCandles({append:false, endTime:historyEndTimeFromInput()}); }
function loadOlderHistory(){ if(_chartMode !== 'history' || !_historyHasMore || !_historyCursor) return; const beforeIso = new Date((Number(_historyCursor) - 1) * 1000).toISOString(); loadHistoryCandles({append:true, endTime:beforeIso}); }
function resetHistory(){ const input = document.getElementById('historyEndDate'); if(input) input.value = ''; if(_chartMode === 'history') loadHistoryCandles({append:false}); }
function bootChartSection(){
  if(_chartBooted) return;
  if(typeof LightweightCharts === 'undefined'){
    _chartRetryCount += 1;
    const status = document.getElementById('chartStatus');
    if(status){ status.textContent = _chartRetryCount > 20 ? 'Chart library unavailable' : 'Chart library loading…'; status.style.background = _chartRetryCount > 20 ? 'rgba(255,69,96,.12)' : 'rgba(255,165,0,.15)'; status.style.color = _chartRetryCount > 20 ? 'var(--rd)' : 'var(--am)'; }
    if(_chartRetryCount <= 20) setTimeout(bootChartSection, 500);
    return;
  }
  initChart();
  _chartBooted = true;
  updateChartModeUi();
  if(_chartMode === 'history') loadHistoryCandles({append:false}); else { loadCandles(); startStream(); }
}
function _boot(){
  loadAssets().then(() => { bootChartSection(); }).catch(() => {
    const sel = document.getElementById('chartAsset');
    if(sel && !sel.options.length) sel.innerHTML = '<option value="">Asset list unavailable</option>';
  });
  setTimeout(() => loadDataPlane(), 150);
  setTimeout(() => loadHeatmap(), 300);
  setTimeout(() => loadCorrelation(), 1000);
}
_boot();
setInterval(() => { if(!_chartBooted){ bootChartSection(); return; } if(_chartMode === 'live'){ const sec = intervalSeconds(currentTf) || 60; const minReloadMs = Math.max(sec * 1000, 15000); if(!_lastLiveReloadAt || (Date.now() - _lastLiveReloadAt) >= minReloadMs) loadCandles(); } }, 15000);
setInterval(() => { if(!document.hidden) loadDataPlane(); }, 10000);
setInterval(() => { loadHeatmap(); }, 60000);
document.addEventListener('visibilitychange', () => { if(!document.hidden){ _marketIntelligenceCache = null; loadDataPlane(); } });

