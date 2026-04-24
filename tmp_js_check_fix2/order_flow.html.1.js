
// Clock
setInterval(() => {
  document.getElementById('clock').textContent = new Date().toLocaleTimeString();
}, 1000);

let _orderFlowCache = null;
let _orderFlowPending = null;
async function fetchOrderFlowOverview(){
  const now = Date.now();
  if(_orderFlowCache && now - _orderFlowCache.fetched < 10000) return _orderFlowCache.data;
  if(_orderFlowPending) return _orderFlowPending;
  _orderFlowPending = (async function(){
    try{
      const d = await window.dashboardFetchJson('/api/page-overview?page=order_flow', {timeoutMs: 12000, init:{cache:'no-store'}});
      if(d.success){
        _orderFlowCache = {fetched: Date.now(), data: d};
        return d;
      }
      return {success:false};
    } finally {
      _orderFlowPending = null;
    }
  })();
  return _orderFlowPending;
}

// Engine status
async function checkStatus() {
  try {
    const page = await fetchOrderFlowOverview();
    const d = page.status || {};
    const live = page.command_center?.live_summary || {};
    const ok = d.engine_running;
    const providerRouting = d.provider_routing || {};
    document.getElementById('engineDot').className = 'pulse' + (ok ? '' : ' off');
    document.getElementById('engineStatus').textContent = ok ? 'Live' : 'Offline';
    document.getElementById('providerStatus').textContent = providerRouting.summary_label || 'Unavailable';
    const liveBalance = Number(live.balance ?? d.balance);
    const livePnl = Number(live.balance_delta ?? live.daily_pnl ?? 0);
    const topBalance = document.getElementById('topBalance');
    topBalance.textContent = Number.isFinite(liveBalance) ? '$' + liveBalance.toLocaleString('en', {minimumFractionDigits:2, maximumFractionDigits:2}) : '—';
    topBalance.style.color = livePnl < 0 ? 'var(--rd)' : 'var(--gr)';
  } catch(e) {}
}

// Imbalance
async function loadImbalance() {
  try {
    const page = await fetchOrderFlowOverview();
    const d = page.imbalance || {};
    if (!d.success) return;
    const imb = d.imbalances || {};
    const assets = Object.keys(imb);
    if (!assets.length) { document.getElementById('imbList').innerHTML = '<div class="empty">No imbalance data</div>'; return; }

    let buys = 0, sells = 0, bc = 0;
    assets.forEach(a => { const v = imb[a]; if (v > 0) { buys += v; bc++; } else if (v < 0) sells += Math.abs(v); });
    document.getElementById('mBuyPressure').textContent  = bc ? (buys/bc).toFixed(3) : '—';
    document.getElementById('mSellPressure').textContent = (assets.length - bc) ? (sells/(assets.length-bc)).toFixed(3) : '—';

    document.getElementById('imbTimestamp').textContent = new Date(d.timestamp).toLocaleTimeString();
    const html = assets.map(asset => {
      const score = imb[asset] || 0;
      const pct   = Math.abs(score) * 100;
      const isBuy = score >= 0;
      const col   = isBuy ? 'var(--gr)' : 'var(--rd)';
      return `<div class="imb-row">
        <div class="imb-asset">${asset.replace('USDT','')}</div>
        <div class="imb-bar-wrap">
          <div class="imb-bar ${isBuy ? 'buy' : 'sell'}" style="width:${Math.min(pct,100)}%"></div>
        </div>
        <div class="imb-score" style="color:${col}">${score >= 0 ? '+' : ''}${score.toFixed(3)}</div>
        <div class="imb-bias" style="color:${col}">${isBuy ? 'BUY' : 'SELL'}</div>
      </div>`;
    }).join('');
    document.getElementById('imbList').innerHTML = html;
  } catch(e) { document.getElementById('imbList').innerHTML = '<div class="empty">Unavailable</div>'; }
}

// Walls
async function loadWalls() {
  try {
    const page = await fetchOrderFlowOverview();
    const d = page.walls || {};
    const walls = d.walls || [];
    document.getElementById('mWallCount').textContent = walls.length;
    document.getElementById('wallBadge').textContent  = walls.length + ' detected';
    if (!walls.length) { document.getElementById('wallList').innerHTML = '<div class="empty">No active walls</div>'; return; }
    const strClass = s => s === 'EXTREME' ? 'str-extreme' : s === 'STRONG' ? 'str-strong' : 'str-moderate';
    const html = walls.slice(0,15).map(w => `
      <div class="wall-item">
        <div class="wall-side ${w.side === 'BID' ? 'wall-bid' : 'wall-ask'}">${w.side}</div>
        <div class="wall-info">
          <div class="wall-asset">${(w.asset||'').replace('USDT','')}</div>
          <div class="wall-price">${Number(w.price||0).toFixed(5)}</div>
        </div>
        <div class="wall-strength ${strClass(w.strength)}">${w.strength||'—'}</div>
        <div class="wall-size">${Number(w.size_ratio||0).toFixed(1)}×</div>
      </div>`).join('');
    document.getElementById('wallList').innerHTML = html;
  } catch(e) {}
}

// Stop Hunts
async function loadHunts() {
  try {
    const page = await fetchOrderFlowOverview();
    const d = page.hunts || {};
    const hunts = d.hunts || [];
    document.getElementById('mHuntCount').textContent = hunts.length;
    document.getElementById('huntBadge').textContent  = hunts.length + ' recent';
    if (!hunts.length) { document.getElementById('huntList').innerHTML = '<div class="empty">No sweep events detected</div>'; return; }
    const html = hunts.slice(0,12).map(h => {
      const sig = h.implication || 'BUY';
      return `<div class="hunt-item">
        <div class="hunt-icon">⚡</div>
        <div class="hunt-info">
          <div class="hunt-asset">${(h.asset||'').replace('USDT','')}</div>
          <div class="hunt-meta">Wall @ ${Number(h.wall_price||0).toFixed(4)} · Wick ${Number(h.wick_pct||0).toFixed(3)}% · ${h.wall_side||''}</div>
        </div>
        <div class="hunt-signal ${sig === 'BUY' ? 'sig-buy' : 'sig-sell'}">${sig}</div>
        <div class="hunt-conf">${Math.round((h.confidence||0)*100)}%</div>
      </div>`;
    }).join('');
    document.getElementById('huntList').innerHTML = html;
  } catch(e) {}
}

function formatFlowAsset(asset){
  return String(asset || '').replace('USDT', '').replace('-USD', '');
}

function depthPriceDigits(price){
  const value = Number(price || 0);
  if(!Number.isFinite(value) || value <= 0) return 4;
  if(value >= 1000) return 1;
  if(value >= 100) return 2;
  if(value >= 10) return 3;
  return 5;
}
function fmtDepthPrice(value){
  const price = Number(value);
  if(!Number.isFinite(price) || price <= 0) return '—';
  return price.toFixed(depthPriceDigits(price));
}
function fmtDepthLevel(level){
  if(!Array.isArray(level) || level.length < 2) return '—';
  const px = fmtDepthPrice(level[0]);
  const size = Number(level[1]);
  return Number.isFinite(size) ? `${px} · ${size.toFixed(size >= 100 ? 0 : 2)}` : px;
}
function fmtDepthAge(seconds){
  const value = Number(seconds);
  if(!Number.isFinite(value) || value < 0) return '—';
  if(value < 1) return `${Math.max(1, Math.round(value * 1000))}ms`;
  return `${value < 10 ? value.toFixed(1) : value.toFixed(0)}s`;
}
function depthPressureLabel(value){
  const token = String(value || '').trim().toLowerCase();
  if(!token) return 'Balanced';
  return token.replace(/_/g,' ');
}
async function loadDepthTape() {
  try {
    const page = await fetchOrderFlowOverview();
    const d = page.depth || {};
    const badge = document.getElementById('depthTapeBadge');
    const list = document.getElementById('depthTapeList');
    if(!d.success){
      badge.textContent = 'Unavailable';
      list.innerHTML = '<div class="empty">Dukascopy depth feed unavailable</div>';
      return;
    }
    const rows = Array.isArray(d.rows) ? d.rows : [];
    if(!rows.length){
      badge.textContent = d.running ? 'Waiting…' : (d.enabled ? 'Starting…' : 'Disabled');
      const reason = !d.enabled
        ? 'Dukascopy live depth is disabled in the environment.'
        : d.running
          ? 'The Dukascopy bridge is running, but no fresh depth snapshots have arrived yet.'
          : 'The Dukascopy bridge is not running yet. Start the full bot after Java and Maven are available.';
      list.innerHTML = `<div class="empty">${reason}</div>`;
      return;
    }
    badge.textContent = `${rows.length} live`;
    list.innerHTML = '<div class="depth-tape-grid">' + rows.map((row) => {
      const price = fmtDepthPrice(row.price);
      const topBid = fmtDepthLevel((row.orderbook_top_bids || [])[0]);
      const topAsk = fmtDepthLevel((row.orderbook_top_asks || [])[0]);
      const imbalance = Number(row.book_imbalance || 0);
      const imbalanceText = `${imbalance >= 0 ? '+' : ''}${imbalance.toFixed(3)}`;
      const spread = Number(row.spread_bps || 0);
      const spreadText = Number.isFinite(spread) && spread > 0 ? `${spread.toFixed(2)} bps` : '—';
      const ageText = fmtDepthAge(row.age_seconds);
      const providerLine = `${String(row.environment || 'demo').toUpperCase()} · ${row.dukascopy_symbol || row.asset || '—'}`;
      return `<div class="depth-tape-card">
        <div class="depth-tape-head">
          <div>
            <div class="depth-tape-asset">${row.asset || '—'}</div>
            <div class="depth-tape-kicker">${providerLine}</div>
          </div>
          <div class="depth-tape-age">${ageText}</div>
        </div>
        <div class="depth-tape-price" style="color:${imbalance >= 0 ? 'var(--gr)' : 'var(--rd)'}">${price}</div>
        <div class="depth-tape-meta">Spread ${spreadText} · ${row.depth_levels || 0} levels · Pressure ${depthPressureLabel(row.pressure_direction)}</div>
        <div class="depth-tape-book">
          <div class="depth-tape-side buy">
            <div class="depth-tape-side-label">Top Bid</div>
            <div class="depth-tape-level buy">${topBid}</div>
          </div>
          <div class="depth-tape-side sell">
            <div class="depth-tape-side-label">Top Ask</div>
            <div class="depth-tape-level sell">${topAsk}</div>
          </div>
        </div>
        <div class="depth-tape-foot">
          <span>Bid vol ${Number(row.bid_vol || 0).toFixed(1)}</span>
          <span>Ask vol ${Number(row.ask_vol || 0).toFixed(1)}</span>
          <span>Book ${imbalanceText}</span>
        </div>
      </div>`;
    }).join('') + '</div>';
  } catch(e) {
    document.getElementById('depthTapeBadge').textContent = 'Error';
    document.getElementById('depthTapeList').innerHTML = '<div class="empty">Live depth feed unavailable</div>';
  }
}

function wallWeight(wall){
  const strength = String(wall?.strength || '').toUpperCase();
  const sizeRatio = Number(wall?.size_ratio || 0);
  const strengthBase = strength === 'EXTREME' ? 1 : strength === 'STRONG' ? 0.7 : 0.4;
  return strengthBase * Math.min(2, Math.max(0.5, sizeRatio / 2 || 0.5));
}
function signalMeta(signal){return signal && typeof signal.metadata === 'object' && signal.metadata ? signal.metadata : {};}
function signalValue(signal,key){const meta=signalMeta(signal);return meta[key] !== undefined ? meta[key] : signal ? signal[key] : undefined;}
function signalNum(signal,key,fallback=0){const num=Number(signalValue(signal,key));return Number.isFinite(num)?num:fallback;}
function signalBool(signal,key){return signalValue(signal,key) === true;}
function signalText(signal,key){const value=signalValue(signal,key);return value == null ? '' : String(value).trim();}
function signalList(signal,key){const value=signalValue(signal,key);if(Array.isArray(value))return value.map(v=>String(v||'').trim()).filter(Boolean);if(typeof value === 'string'){const raw=value.trim();return raw?[raw]:[];}return [];}
function normalizeFlowAsset(asset){
  return String(asset || '')
    .toUpperCase()
    .replace('-USD','')
    .replace('/USD','')
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
  return String(signalValue(signal, 'exact_kill_reason') || signalValue(signal, 'execution_kill_reason') || signalValue(signal, 'blocked_reason') || signalValue(signal, 'kill_reason') || signalValue(signal, 'killed_by') || signalValue(signal, 'reason') || signalList(signal, 'execution_hard_blocks')[0] || signalList(signal, 'late_entry_risk_reasons')[0] || signalList(signal, 'rejected_reasons')[0] || '').trim();
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
function buildFlowState(imbalance, walls, hunts){
  const state = {};
  function ensure(asset){
    const key = normalizeFlowAsset(asset);
    if(!key) return null;
    if(!state[key]){
      state[key] = {asset:asset, key:key, imbalance:0, wallDelta:0, huntDelta:0, bidWalls:0, askWalls:0, buyHunts:0, sellHunts:0, score:0};
    }
    return state[key];
  }
  Object.entries((imbalance && imbalance.imbalances) || {}).forEach(([asset, score]) => {
    const row = ensure(asset);
    if(row) row.imbalance = Number(score || 0);
  });
  (walls || []).forEach((wall) => {
    const row = ensure(wall.asset);
    if(!row) return;
    const weight = wallWeight(wall);
    if(String(wall.side || '').toUpperCase() === 'BID'){
      row.bidWalls += 1;
      row.wallDelta += weight;
    }else{
      row.askWalls += 1;
      row.wallDelta -= weight;
    }
  });
  (hunts || []).forEach((hunt) => {
    const row = ensure(hunt.asset);
    if(!row) return;
    const weight = Math.max(0.2, Number(hunt.confidence || 0) || 0.2);
    if(String(hunt.implication || '').toUpperCase() === 'SELL'){
      row.sellHunts += 1;
      row.huntDelta -= weight;
    }else{
      row.buyHunts += 1;
      row.huntDelta += weight;
    }
  });
  return Object.values(state).map((row) => {
    row.score = row.imbalance + row.wallDelta * 0.2 + row.huntDelta * 0.25;
    return row;
  }).sort((a,b) => Math.abs(b.score) - Math.abs(a.score));
}
function buildSignalIndex(signals){
  const index = new Map();
  (Array.isArray(signals) ? signals : []).forEach((signal) => {
    const key = normalizeFlowAsset(signal.asset || signal.symbol);
    if(key && !index.has(key)) index.set(key, signal);
  });
  return index;
}
function signalFlowSupport(signal){
  return {
    retest: signalBool(signal, 'breakout_retest_ready'),
    pullback: signalBool(signal, 'first_pullback_ready'),
    sweep: signalBool(signal, 'liquidity_sweep_reclaim'),
    failedOpposite: signalBool(signal, 'failed_opposite_move_confirmed'),
    confirmation: confirmationState(signal),
    readiness: entryReadiness(signal),
    policy: regimeBits(signal),
    notes: reviewNotesLine(signal),
  };
}

async function loadFlowBridge() {
  try {
    const page = await fetchOrderFlowOverview();
    const status = page.status || {};
    const routing = status.provider_routing || {};
    const diagnostics = status.signal_diagnostics || {};
    const imbalance = page.imbalance || {};
    const walls = page.walls?.walls || [];
    const hunts = page.hunts?.hunts || [];
    const commandCenter = page.command_center || {};
    const liveSignals = Array.isArray(commandCenter.latest_signals) ? commandCenter.latest_signals : [];
    const signalIndex = buildSignalIndex(liveSignals);
    const flowRanked = buildFlowState(imbalance, walls, hunts);
    const flowMap = new Map(flowRanked.map(row => [normalizeFlowAsset(row.asset), row]));
    const bullishCount = Number(imbalance.bullish_count || 0);
    const bearishCount = Number(imbalance.bearish_count || 0);
    const fragileCount = Number(diagnostics.broker_fragile_count || 0);
    const supportiveCount = Number(diagnostics.broker_supportive_count || 0);
    const trueDepthCount = Number(diagnostics.true_depth_count || 0);
    const syntheticDepthCount = Number(diagnostics.synthetic_depth_count || 0);
    const conflictCount = Number(diagnostics.cross_conflict_count || 0);
    const blockCount = Number(diagnostics.recent_pattern_block_count || 0);
    const supportStats = liveSignals.reduce((acc, signal) => {
      const flow = flowMap.get(normalizeFlowAsset(signal.asset || signal.symbol));
      if(!flow) return acc;
      const support = signalFlowSupport(signal);
      const flowAligned = flow.score >= 0;
      if(support.retest) acc.retest += 1;
      if(support.pullback) acc.pullback += 1;
      if(support.sweep) acc.sweep += 1;
      if(support.failedOpposite) acc.failedOpposite += 1;
      if(support.confirmation.state === 'ready') acc.confirmationReady += 1;
      if(flowAligned) acc.flowAligned += 1;
      if(flowAligned && support.readiness.state === 'ready') acc.flowAlignedReady += 1;
      if(flowAligned && support.readiness.state === 'blocked') acc.flowAlignedBlocked += 1;
      if(signalNum(signal, 'extension_score', 0) >= 0.80) acc.overextended += 1;
      if((signalNum(signal, 'candle_quality_score', 0) > 0 && signalNum(signal, 'candle_quality_score', 0) < 0.55) || (signalNum(signal, 'session_quality_score', 0) > 0 && signalNum(signal, 'session_quality_score', 0) < 0.55) || (signalNum(signal, 'target_efficiency_score', 0) > 0 && signalNum(signal, 'target_efficiency_score', 0) < 0.50)) acc.qualityBlocked += 1;
      if(/regime|policy/.test(String(signalValue(signal, 'regime_policy_summary') || signalValue(signal, 'regime_policy') || signalValue(signal, 'regime_label') || '').toLowerCase()) || /regime|policy/.test(exactKillReason(signal).toLowerCase())) acc.policyBlocked += 1;
      return acc;
    }, {retest:0,pullback:0,sweep:0,failedOpposite:0,confirmationReady:0,flowAligned:0,flowAlignedReady:0,flowAlignedBlocked:0,overextended:0,qualityBlocked:0,policyBlocked:0});
    const bridgeState = fragileCount || conflictCount || blockCount ? 'Guarded' : diagnostics.count || liveSignals.length ? 'Aligned' : 'Idle';
    document.getElementById('flowBridgeBadge').textContent = bridgeState;
    const list = document.getElementById('flowBridgeList');
    if(!diagnostics.count && !walls.length && !hunts.length && !Object.keys(imbalance.imbalances || {}).length && !liveSignals.length){
      list.innerHTML = '<div class="empty">Flow bridge is waiting for live order-flow and active signal diagnostics.</div>';
      return;
    }
    list.innerHTML = '<div class="flow-bridge-grid">' + [
      {
        title: 'Provider spine',
        value: routing.summary_label || 'Unavailable',
        meta: routing.fallback_label ? `Fallback ${routing.fallback_label}` : 'No fallback label published',
        color: 'var(--cy)',
        sub: 'How live broker routing is feeding the flow stack',
      },
      {
        title: 'Signal diagnostics',
        value: diagnostics.summary_label || (liveSignals.length ? 'Command center live' : 'No active diagnostics'),
        meta: `${supportiveCount} supportive · ${fragileCount} fragile · ${liveSignals.length} live signals`,
        color: fragileCount ? 'var(--am)' : supportiveCount ? 'var(--gr)' : 'var(--tx2)',
        sub: 'Whether current flow aligns with live broker-quality checks and command-center signals',
      },
      {
        title: 'Flow balance',
        value: `${bullishCount} / ${bearishCount}`,
        meta: `${walls.length} walls · ${hunts.length} hunts`,
        color: bullishCount > bearishCount ? 'var(--gr)' : bearishCount > bullishCount ? 'var(--rd)' : 'var(--cy)',
        sub: 'Bullish vs bearish pressure in the active flow picture',
      },
      {
        title: 'Entry support',
        value: `${supportStats.retest}/${supportStats.pullback}`,
        meta: `${supportStats.sweep} sweep reclaim · ${supportStats.failedOpposite} failed-opposite reclaim`,
        color: supportStats.retest || supportStats.pullback || supportStats.sweep ? 'var(--gr)' : 'var(--tx2)',
        sub: 'How often flow is supporting retest and pullback style entries',
      },
      {
        title: 'Gate overlap',
        value: `${supportStats.flowAlignedReady}/${supportStats.flowAlignedBlocked}`,
        meta: `${supportStats.flowAligned} flow-aligned setups · ${supportStats.confirmationReady} confirmation-ready`,
        color: supportStats.flowAlignedBlocked ? 'var(--rd)' : supportStats.flowAlignedReady ? 'var(--gr)' : 'var(--tx2)',
        sub: 'Flow-aligned ideas that still hit non-flow execution gates',
      },
      {
        title: 'Depth & spillover',
        value: `${trueDepthCount}T / ${syntheticDepthCount}S`,
        meta: `${conflictCount} cross conflicts · ${blockCount} pattern blocks`,
        color: trueDepthCount ? 'var(--gr)' : syntheticDepthCount ? 'var(--am)' : 'var(--tx2)',
        sub: 'Depth mode and whether cross-market context is blocking entries',
      },
    ].map(item => `
      <div class="flow-bridge-card">
        <div class="flow-bridge-kicker">${item.title}</div>
        <div class="flow-bridge-value ${String(item.value).length > 16 ? 'compact' : ''}" style="color:${item.color}">${item.value}</div>
        <div class="flow-bridge-note">${item.meta}</div>
        <div class="flow-bridge-sub">${item.sub}</div>
      </div>
    `).join('') + '</div>';

    const wallBidLead = walls.filter((wall) => String(wall.side || '').toUpperCase() === 'BID').length;
    const wallAskLead = walls.length - wallBidLead;
    const huntBuyLead = hunts.filter((hunt) => String(hunt.implication || '').toUpperCase() === 'BUY').length;
    const huntSellLead = hunts.length - huntBuyLead;
    const frictionState = fragileCount || conflictCount || blockCount ? 'Guarded' : hunts.length || walls.length || liveSignals.length ? 'Live' : 'Idle';
    document.getElementById('frictionBridgeBadge').textContent = frictionState;
    document.getElementById('executionFrictionPanel').innerHTML = '<div class="flow-bridge-grid">' + [
      {
        title: 'Execution posture',
        value: frictionState,
        meta: fragileCount ? `${fragileCount} fragile broker contexts still drag the tape` : `${supportStats.flowAlignedBlocked} flow-aligned setups still hit execution gates`,
        color: fragileCount ? 'var(--am)' : diagnostics.count ? 'var(--gr)' : 'var(--tx2)',
        sub: 'Desk view of whether flow can be trusted for execution right now',
      },
      {
        title: 'Wall asymmetry',
        value: `${wallBidLead}/${wallAskLead}`,
        meta: walls.length ? `${wallBidLead} bid-led vs ${wallAskLead} ask-led liquidity walls` : 'No wall concentration yet',
        color: wallBidLead > wallAskLead ? 'var(--gr)' : wallAskLead > wallBidLead ? 'var(--rd)' : 'var(--cy)',
        sub: 'Where passive liquidity is leaning across the active book',
      },
      {
        title: 'Hunt pressure',
        value: `${huntBuyLead}/${huntSellLead}`,
        meta: hunts.length ? `${hunts.length} stop-hunt events in the current window` : 'No hunts detected in the active lookback',
        color: huntBuyLead > huntSellLead ? 'var(--gr)' : huntSellLead > huntBuyLead ? 'var(--rd)' : 'var(--tx2)',
        sub: 'Which side has been getting cleaned out most recently',
      },
      {
        title: 'Cross drag',
        value: `${conflictCount}/${blockCount}`,
        meta: conflictCount || blockCount ? `${supportStats.policyBlocked} policy · ${supportStats.qualityBlocked} quality blockers in the live signal set` : 'Cross-market checks are not the main drag right now',
        color: conflictCount || blockCount ? 'var(--rd)' : 'var(--gr)',
        sub: 'Review stack friction beyond the raw order-book read',
      },
    ].map(item => `
      <div class="flow-bridge-card">
        <div class="flow-bridge-kicker">${item.title}</div>
        <div class="flow-bridge-value ${String(item.value).length > 16 ? 'compact' : ''}" style="color:${item.color}">${item.value}</div>
        <div class="flow-bridge-note">${item.meta}</div>
        <div class="flow-bridge-sub">${item.sub}</div>
      </div>
    `).join('') + '</div>';

    const strongWalls = walls.filter((wall) => ['STRONG', 'EXTREME'].includes(String(wall.strength || '').toUpperCase())).length;
    const wallQualityPct = walls.length ? ((strongWalls / walls.length) * 100) : 0;
    const flowState = {};
    function ensure(asset){
      if(!asset) return null;
      if(!flowState[asset]){
        flowState[asset] = {asset, imbalance:0, wallDelta:0, huntDelta:0, bidWalls:0, askWalls:0, buyHunts:0, sellHunts:0};
      }
      return flowState[asset];
    }
    Object.entries(imbalance.imbalances || {}).forEach(([asset, score]) => {
      const row = ensure(asset);
      if(row) row.imbalance = Number(score || 0);
    });
    walls.forEach((wall) => {
      const row = ensure(wall.asset);
      if(!row) return;
      const weight = wallWeight(wall);
      if(String(wall.side || '').toUpperCase() === 'BID'){
        row.bidWalls += 1;
        row.wallDelta += weight;
      }else{
        row.askWalls += 1;
        row.wallDelta -= weight;
      }
    });
    hunts.forEach((hunt) => {
      const row = ensure(hunt.asset);
      if(!row) return;
      const weight = Math.max(0.2, Number(hunt.confidence || 0) || 0.2);
      if(String(hunt.implication || '').toUpperCase() === 'SELL'){
        row.sellHunts += 1;
        row.huntDelta -= weight;
      }else{
        row.buyHunts += 1;
        row.huntDelta += weight;
      }
    });
    const depthRanked = Object.values(flowState).map((row) => {
      row.score = row.imbalance + row.wallDelta * 0.2 + row.huntDelta * 0.25;
      return row;
    }).sort((a,b) => Math.abs(b.score) - Math.abs(a.score));
    const leadDepth = depthRanked[0] || null;
    const leadSweep = hunts.slice().sort((a,b) => Number(b.confidence || 0) - Number(a.confidence || 0))[0] || null;
    const depthState = trueDepthCount ? 'True-led' : syntheticDepthCount ? 'Synthetic-led' : walls.length || hunts.length ? 'Proxy' : 'Idle';
    document.getElementById('depthQualityBadge').textContent = depthState;
    document.getElementById('depthQualityPanel').innerHTML = '<div class="flow-bridge-grid">' + [
      {
        title: 'Runtime depth',
        value: `${trueDepthCount}T / ${syntheticDepthCount}S`,
        meta: diagnostics.count ? diagnostics.summary_label || 'Live diagnostics active' : 'No active signal diagnostics yet',
        color: trueDepthCount ? 'var(--gr)' : syntheticDepthCount ? 'var(--am)' : 'var(--tx2)',
        sub: 'Whether the desk is trading on true depth or synthetic proxies',
      },
      {
        title: 'Wall quality',
        value: walls.length ? `${wallQualityPct.toFixed(0)}%` : '—',
        meta: walls.length ? `${strongWalls}/${walls.length} walls are strong or extreme` : 'No live wall set yet',
        color: wallQualityPct >= 60 ? 'var(--gr)' : wallQualityPct >= 35 ? 'var(--am)' : 'var(--tx2)',
        sub: 'How much of the visible book has real size behind it',
      },
      {
        title: 'Sweep leader',
        value: leadSweep ? formatFlowAsset(leadSweep.asset) : '—',
        meta: leadSweep ? `${String(leadSweep.implication || '').toUpperCase()} ${(Number(leadSweep.confidence || 0) * 100).toFixed(0)}% · ${String(leadSweep.wall_side || '').toUpperCase()}` : 'No dominant sweep in the active window',
        color: leadSweep ? (String(leadSweep.implication || '').toUpperCase() === 'SELL' ? 'var(--rd)' : 'var(--gr)') : 'var(--tx2)',
        sub: 'The cleanest sweep / stop-hunt event in the recent tape',
      },
      {
        title: 'Best book',
        value: leadDepth ? formatFlowAsset(leadDepth.asset) : '—',
        meta: leadDepth ? `Score ${(leadDepth.score >= 0 ? '+' : '') + leadDepth.score.toFixed(3)} · ${leadDepth.bidWalls} bid / ${leadDepth.askWalls} ask walls` : 'No blended depth leader yet',
        color: leadDepth ? (leadDepth.score >= 0 ? 'var(--gr)' : 'var(--rd)') : 'var(--tx2)',
        sub: 'Composite depth leader from imbalance, walls, and sweep pressure',
      },
    ].map(item => `
      <div class="flow-bridge-card">
        <div class="flow-bridge-kicker">${item.title}</div>
        <div class="flow-bridge-value ${String(item.value).length > 16 ? 'compact' : ''}" style="color:${item.color}">${item.value}</div>
        <div class="flow-bridge-note">${item.meta}</div>
        <div class="flow-bridge-sub">${item.sub}</div>
      </div>
    `).join('') + '</div>';

    const absorptionRows = depthRanked.slice(0,4).map((row) => {
      const rejection = row.bidWalls && row.sellHunts ? 'Bid absorption failed' : row.askWalls && row.buyHunts ? 'Ask rejection failed' : 'Mixed response';
      return {
        title: formatFlowAsset(row.asset),
        value: `${row.bidWalls}/${row.askWalls}`,
        meta: `${row.buyHunts} buy hunts · ${row.sellHunts} sell hunts · ${rejection}`,
        color: row.score >= 0 ? 'var(--gr)' : 'var(--rd)',
      };
    });
    document.getElementById('absorptionBadge').textContent = absorptionRows.length ? 'Live' : 'Idle';
    document.getElementById('absorptionPanel').innerHTML = absorptionRows.length
      ? '<div class="flow-bridge-grid">' + absorptionRows.map(item => `
          <div class="flow-bridge-card">
            <div class="flow-bridge-kicker">${item.title}</div>
            <div class="flow-bridge-value" style="color:${item.color}">${item.value}</div>
            <div class="flow-bridge-note">${item.meta}</div>
            <div class="flow-bridge-sub">Bid/ask wall absorption versus recent sweep rejection.</div>
          </div>
        `).join('') + '</div>'
      : '<div class="empty">No absorption / rejection pattern is clear right now</div>';

    const acceleration = [
      {
        title: 'Wall build',
        value: `${walls.length}`,
        meta: strongWalls ? `${strongWalls} strong walls in the active tape` : 'No strong wall build yet',
        color: strongWalls >= 3 ? 'var(--gr)' : walls.length ? 'var(--am)' : 'var(--tx2)',
      },
      {
        title: 'Sweep velocity',
        value: `${hunts.length}`,
        meta: hunts.length ? `${huntBuyLead} buy-led · ${huntSellLead} sell-led sweeps` : 'No active sweep acceleration',
        color: hunts.length >= 4 ? 'var(--rd)' : hunts.length ? 'var(--am)' : 'var(--tx2)',
      },
      {
        title: 'Imbalance tilt',
        value: `${bullishCount}/${bearishCount}`,
        meta: bullishCount > bearishCount ? 'Buy-side pressure is accelerating' : bearishCount > bullishCount ? 'Sell-side pressure is accelerating' : 'Pressure is balanced',
        color: bullishCount > bearishCount ? 'var(--gr)' : bearishCount > bullishCount ? 'var(--rd)' : 'var(--cy)',
      },
      {
        title: 'Lead tape',
        value: leadDepth ? formatFlowAsset(leadDepth.asset) : '—',
        meta: leadDepth ? `Score ${(leadDepth.score >= 0 ? '+' : '') + leadDepth.score.toFixed(3)} from combined flow inputs` : 'No accelerating tape leader',
        color: leadDepth ? (leadDepth.score >= 0 ? 'var(--gr)' : 'var(--rd)') : 'var(--tx2)',
      },
      {
        title: 'Entry gates',
        value: `${supportStats.flowAlignedReady}/${supportStats.flowAlignedBlocked}`,
        meta: `${supportStats.retest} retest · ${supportStats.pullback} pullback · ${supportStats.overextended} overextended`,
        color: supportStats.flowAlignedBlocked ? 'var(--am)' : supportStats.flowAlignedReady ? 'var(--gr)' : 'var(--tx2)',
      },
    ];
    document.getElementById('accelerationBadge').textContent = walls.length || hunts.length || bullishCount || bearishCount ? 'Tracking' : 'Idle';
    document.getElementById('accelerationPanel').innerHTML = '<div class="flow-bridge-grid">' + acceleration.map(item => `
      <div class="flow-bridge-card">
        <div class="flow-bridge-kicker">${item.title}</div>
        <div class="flow-bridge-value" style="color:${item.color}">${item.value}</div>
        <div class="flow-bridge-note">${item.meta}</div>
        <div class="flow-bridge-sub">Whether liquidity and sweep pressure are accelerating or fading.</div>
      </div>
    `).join('') + '</div>';
  } catch(e) {
    document.getElementById('absorptionBadge').textContent = 'Unavailable';
    document.getElementById('accelerationBadge').textContent = 'Unavailable';
    document.getElementById('flowBridgeList').innerHTML = '<div class="empty">Flow bridge unavailable</div>';
    document.getElementById('executionFrictionPanel').innerHTML = '<div class="empty">Execution friction unavailable</div>';
    document.getElementById('depthQualityPanel').innerHTML = '<div class="empty">Depth quality unavailable</div>';
    document.getElementById('absorptionPanel').innerHTML = '<div class="empty">Absorption / rejection unavailable</div>';
    document.getElementById('accelerationPanel').innerHTML = '<div class="empty">Flow acceleration unavailable</div>';
  }
}

async function loadWatchlist() {
  try {
    const page = await fetchOrderFlowOverview();
    const commandCenter = page.command_center || {};
    const liveSignals = Array.isArray(commandCenter.latest_signals) ? commandCenter.latest_signals : [];
    const imbalances = page.imbalance?.imbalances || {};
    const walls = page.walls?.walls || [];
    const hunts = page.hunts?.hunts || [];
    const signalIndex = buildSignalIndex(liveSignals);
    const state = {};

    function ensure(asset){
      if(!asset) return null;
      if(!state[asset]){
        state[asset] = {
          asset,
          imbalance: 0,
          bidWalls: 0,
          askWalls: 0,
          wallDelta: 0,
          buyHunts: 0,
          sellHunts: 0,
          huntDelta: 0,
        };
      }
      return state[asset];
    }

    Object.entries(imbalances).forEach(([asset, score]) => {
      const row = ensure(asset);
      if(row) row.imbalance = Number(score || 0);
    });

    walls.forEach((wall) => {
      const row = ensure(wall.asset);
      if(!row) return;
      const weight = wallWeight(wall);
      if(String(wall.side || '').toUpperCase() === 'BID'){
        row.bidWalls += 1;
        row.wallDelta += weight;
      } else {
        row.askWalls += 1;
        row.wallDelta -= weight;
      }
    });

    hunts.forEach((hunt) => {
      const row = ensure(hunt.asset);
      if(!row) return;
      const weight = Math.max(0.2, Number(hunt.confidence || 0) || 0.2);
      if(String(hunt.implication || '').toUpperCase() === 'SELL'){
        row.sellHunts += 1;
        row.huntDelta -= weight;
      } else {
        row.buyHunts += 1;
        row.huntDelta += weight;
      }
    });

    const ranked = Object.values(state).map((row) => {
      const score = row.imbalance + row.wallDelta * 0.2 + row.huntDelta * 0.25;
      let bias = 'Neutral';
      let biasClass = 'neutral';
      if(score >= 0.15){ bias = 'Bullish'; biasClass = 'buy'; }
      else if(score <= -0.15){ bias = 'Bearish'; biasClass = 'sell'; }
      row.score = score;
      row.bias = bias;
      row.biasClass = biasClass;
      return row;
    }).sort((a, b) => Math.abs(b.score) - Math.abs(a.score));

    document.getElementById('watchBadge').textContent = ranked.length ? ranked.length + ' ranked' : 'No signals';

    if(!ranked.length){
      document.getElementById('watchList').innerHTML = '<div class="empty">No flow signals available</div>';
      return;
    }

    document.getElementById('watchList').innerHTML = ranked.slice(0, 8).map((row) => {
      const color = row.score >= 0 ? 'var(--gr)' : 'var(--rd)';
      const signal = signalIndex.get(normalizeFlowAsset(row.asset));
      const support = signal ? signalFlowSupport(signal) : null;
      const tags = [
        `<span class="watch-tag ${row.biasClass}">${row.bias}</span>`,
        `<span class="watch-tag">Imbalance ${(row.imbalance >= 0 ? '+' : '') + row.imbalance.toFixed(3)}</span>`,
      ];
      if(row.bidWalls || row.askWalls){
        tags.push(`<span class="watch-tag">${row.bidWalls} bid / ${row.askWalls} ask walls</span>`);
      }
      if(row.buyHunts || row.sellHunts){
        tags.push(`<span class="watch-tag">${row.buyHunts} buy / ${row.sellHunts} sell hunts</span>`);
      }
      if(support){
        if(support.retest) tags.push(`<span class="watch-tag">Retest</span>`);
        if(support.pullback) tags.push(`<span class="watch-tag">Pullback</span>`);
        if(support.sweep) tags.push(`<span class="watch-tag">Sweep reclaim</span>`);
        if(support.failedOpposite) tags.push(`<span class="watch-tag">Failed opposite</span>`);
        tags.push(`<span class="watch-tag ${support.readiness.state === 'ready' ? 'buy' : support.readiness.state === 'blocked' ? 'sell' : 'neutral'}">${support.readiness.label}</span>`);
      }
      return `<div class="watch-item">
        <div class="watch-head">
          <div class="watch-asset">${formatFlowAsset(row.asset)}</div>
          <div class="watch-score" style="color:${color}">${row.score >= 0 ? '+' : ''}${row.score.toFixed(3)}</div>
        </div>
        <div class="watch-meta">${signal ? `${support.confirmation.label} · ${support.policy[0] || 'No policy note'} · ${reviewNotesLine(signal) || 'No review notes'}` : 'Composite flow blends live imbalance, liquidity walls, and recent hunt signals.'}</div>
        <div class="watch-tags">${tags.join('')}</div>
      </div>`;
    }).join('');
  } catch(e) {
    document.getElementById('watchList').innerHTML = '<div class="empty">Flow watchlist unavailable</div>';
  }
}

async function refresh() {
  await Promise.allSettled([
    checkStatus(),
    loadImbalance(),
    loadWalls(),
    loadHunts(),
    loadDepthTape(),
    loadWatchlist(),
    loadFlowBridge(),
  ]);
}

refresh();
setInterval(function(){
  if(document.hidden) return;
  _orderFlowCache = null;
  refresh();
}, 5000);
document.addEventListener('visibilitychange', function(){
  if(!document.hidden){
    _orderFlowCache = null;
    refresh();
  }
});

