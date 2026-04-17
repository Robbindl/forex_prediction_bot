
let startTime=Date.now();
let cpuHist=[],ramHist=[],cpuSpark=null,ramSpark=null;

function initSparks(){
  const opts=(label,color)=>({
    type:'line',data:{labels:Array(20).fill(''),datasets:[{data:Array(20).fill(0),borderColor:color,borderWidth:1.5,fill:true,backgroundColor:color.replace('rgb','rgba').replace(')',',0.1)'),tension:.3,pointRadius:0}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},animation:false,
      scales:{x:{display:false},y:{min:0,max:100,display:false}}}
  });
  cpuSpark=new Chart(document.getElementById('cpuSpark'),opts('CPU','rgb(41,121,255)'));
  ramSpark=new Chart(document.getElementById('ramSpark'),opts('RAM','rgb(179,136,255)'));
}

function updateSpark(inst,hist,val){
  hist.push(val);if(hist.length>20)hist.shift();
  inst.data.datasets[0].data=[...hist];inst.update('none');
}

function barColor(pct){return pct>85?'var(--rd)':pct>70?'var(--am)':'var(--gr)'}
function signalMeta(signal){return signal && typeof signal.metadata === 'object' && signal.metadata ? signal.metadata : {};}
function signalValue(signal,key){const meta=signalMeta(signal);return meta[key] !== undefined ? meta[key] : signal ? signal[key] : undefined;}
function signalNum(signal,key,fallback=0){const num=Number(signalValue(signal,key));return Number.isFinite(num)?num:fallback;}
function signalBool(signal,key){return signalValue(signal,key) === true;}
function signalText(signal,key){const value=signalValue(signal,key);return value == null ? '' : String(value).trim();}
function signalList(signal,key){const value=signalValue(signal,key);if(Array.isArray(value))return value.map(v=>String(v||'').trim()).filter(Boolean);if(typeof value === 'string'){const raw=value.trim();return raw?[raw]:[];}return [];}
function exactSignalReason(signal){return String(signalText(signal, 'exact_kill_reason') || signalText(signal, 'execution_kill_reason') || signalText(signal, 'blocked_reason') || signalText(signal, 'kill_reason') || signalText(signal, 'killed_by') || signalList(signal, 'execution_hard_blocks')[0] || signalList(signal, 'late_entry_risk_reasons')[0] || signalList(signal, 'rejected_reasons')[0] || '').trim();}
function overextendedSignal(signal){return signalNum(signal, 'extension_score', 0) >= 0.80 || /overextend|extension|late setup|too far/i.test(exactSignalReason(signal).toLowerCase());}
function weakCandleSignal(signal){const candle=signalNum(signal, 'candle_quality_score', 0);return (candle > 0 && candle < 0.55) || /candle/.test(exactSignalReason(signal).toLowerCase());}
function weakSessionSignal(signal){const session=signalNum(signal, 'session_quality_score', 0);return (session > 0 && session < 0.55) || /session/.test(exactSignalReason(signal).toLowerCase());}
function weakTargetSignal(signal){const target=signalNum(signal, 'target_efficiency_score', 0);return (target > 0 && target < 0.50) || /target/.test(exactSignalReason(signal).toLowerCase());}
function signalEntryState(signal){
  const kill = exactSignalReason(signal);
  const ready = signalBool(signal, 'entry_confirmation_ready');
  const count = signalNum(signal, 'entry_confirmation_count', 0);
  const required = signalNum(signal, 'entry_confirmation_bars_required', 0);
  const retest = signalBool(signal, 'breakout_retest_ready');
  const pullback = signalBool(signal, 'first_pullback_ready');
  if(kill) return 'blocked';
  if(ready && (retest || pullback || !required)) return 'ready';
  if((retest || pullback) || (required && count < required)) return 'waiting';
  return 'neutral';
}
function deriveDecisionGateCounts(signals){
  const rows = Array.isArray(signals) ? signals : [];
  let confirmationWait = 0, retestMissing = 0, pullbackMissing = 0, extensionVeto = 0, weakCandle = 0, weakSession = 0, targetVeto = 0, oldImpulse = 0, regimeVeto = 0, clusterVeto = 0, blockedCount = 0;
  rows.forEach(function(item){
    const kill = exactSignalReason(item).toLowerCase();
    const count = signalNum(item, 'entry_confirmation_count', 0);
    const required = signalNum(item, 'entry_confirmation_bars_required', 0);
    const ready = signalBool(item, 'entry_confirmation_ready');
    const retest = signalBool(item, 'breakout_retest_ready');
    const pullback = signalBool(item, 'first_pullback_ready');
    const impulse = signalNum(item, 'impulse_age_bars', 0);
    const cluster = signalNum(item, 'cluster_penalty', 0);
    const policy = (signalText(item, 'regime_policy_summary') || signalText(item, 'regime_policy') || signalText(item, 'regime_label')).toLowerCase();
    if(!ready && required > 0 && count < required) confirmationWait += 1;
    if(!retest) retestMissing += 1;
    if(!pullback) pullbackMissing += 1;
    if(overextendedSignal(item)) extensionVeto += 1;
    if(weakCandleSignal(item)) weakCandle += 1;
    if(weakSessionSignal(item)) weakSession += 1;
    if(weakTargetSignal(item)) targetVeto += 1;
    if(impulse >= 6 || /impulse|old/.test(kill)) oldImpulse += 1;
    if(/regime|policy/.test(kill) || /regime|policy/.test(policy)) regimeVeto += 1;
    if(cluster >= 0.7 || /cluster/.test(kill)) clusterVeto += 1;
    if(kill || overextendedSignal(item) || cluster >= 0.7 || weakCandleSignal(item) || weakSessionSignal(item) || weakTargetSignal(item) || impulse >= 6) blockedCount += 1;
  });
  return {
    confirmation_wait_count: confirmationWait,
    retest_missing_count: retestMissing,
    pullback_missing_count: pullbackMissing,
    extension_veto_count: extensionVeto,
    weak_candle_veto_count: weakCandle,
    weak_session_veto_count: weakSession,
    target_efficiency_veto_count: targetVeto,
    old_impulse_veto_count: oldImpulse,
    regime_veto_count: regimeVeto,
    cluster_veto_count: clusterVeto,
    blocked_candidate_count: blockedCount,
  };
}
function deriveDecisionFunnel(signals, tradeLifecycle, openPositions){
  const rows = Array.isArray(signals) ? signals : [];
  const seeded = Number(tradeLifecycle?.seeded || rows.length || 0);
  const structure = Number(tradeLifecycle?.structure_valid || rows.filter(function(item){
    return signalBool(item, 'breakout_retest_ready') || signalBool(item, 'first_pullback_ready') || signalBool(item, 'entry_confirmation_ready');
  }).length || 0);
  const confirm = Number(tradeLifecycle?.confirmation_ready || rows.filter(function(item){
    return signalBool(item, 'entry_confirmation_ready');
  }).length || 0);
  const approved = Number(tradeLifecycle?.approved || rows.filter(function(item){
    return signalEntryState(item) === 'ready';
  }).length || 0);
  const executed = Number(tradeLifecycle?.executed || tradeLifecycle?.opened || openPositions || 0);
  const closed = Number(tradeLifecycle?.closed || 0);
  return {seeded, structure, confirm, approved, executed, closed};
}

let _systemMonitorCache = null;
let _systemMonitorPending = null;
async function fetchSystemMonitorOverview(){
  const now = Date.now();
  if(_systemMonitorCache && now - _systemMonitorCache.fetched < 10000){
    return _systemMonitorCache.data;
  }
  if(_systemMonitorPending) return _systemMonitorPending;
  _systemMonitorPending = (async function(){
    try{
      const d = await window.dashboardFetchJson('/api/page-overview?page=system_monitor', {timeoutMs: 12000});
      if(d.success){
        _systemMonitorCache = {fetched: Date.now(), data: d};
        return d;
      }
      return {success:false};
    } finally {
      _systemMonitorPending = null;
    }
  })();
  return _systemMonitorPending;
}

// Uptime counter
setInterval(()=>{
  const s=Math.floor((Date.now()-startTime)/1000);
  const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sec=s%60;
  document.getElementById('uptime').textContent=`${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`;
},1000);

async function load(){
  try{
    const overview = await fetchSystemMonitorOverview();
    const d = overview.health || {};
    const snapshot = overview.snapshot || {};
    const commandCenter = overview.command_center || {};
    const liveSignals = Array.isArray(commandCenter.latest_signals) ? commandCenter.latest_signals : [];
    const tradeLifecycle = commandCenter.trade_lifecycle || {};
    const gateCounts = deriveDecisionGateCounts(liveSignals);
    const funnel = deriveDecisionFunnel(liveSignals, tradeLifecycle, Number(d.open_positions || 0));

    document.getElementById('updateTime').textContent='Updated: '+new Date(new Date(d.timestamp||Date.now()).getTime()+3*3600000).toISOString().substring(11,19)+' EAT';
    document.getElementById('sysDot').style.background=d.processes?.['TradingCore']?'var(--gr)':'var(--rd)';

    // CPU
    const cpu=d.cpu_pct||0;
    document.getElementById('cpuPct').textContent=cpu.toFixed(1)+'%';
    document.getElementById('cpuPct').style.color=barColor(cpu);
    document.getElementById('cpuBar').style.cssText=`width:${cpu}%;background:${barColor(cpu)}`;
    updateSpark(cpuSpark,cpuHist,cpu);

    // RAM
    const ram=d.ram_pct||0;
    document.getElementById('ramPct').textContent=ram.toFixed(1)+'%';
    document.getElementById('ramPct').style.color=barColor(ram);
    document.getElementById('ramBar').style.cssText=`width:${ram}%;background:${barColor(ram)}`;
    document.getElementById('ramSub').textContent='Process: '+(d.process_mem_mb||0)+' MB';
    updateSpark(ramSpark,ramHist,ram);

    // Disk
    const disk=d.disk_pct||0;
    document.getElementById('diskPct').textContent=disk.toFixed(1)+'%';
    document.getElementById('diskBar').style.cssText=`width:${disk}%;background:${barColor(disk)}`;
    document.getElementById('openPos').textContent=d.open_positions||0;
    document.getElementById('cooldowns').textContent=d.active_cooldowns||0;

    // Trading stats
    document.getElementById('sBalance').textContent='$'+(d.balance||0).toFixed(2);
    document.getElementById('sStrategy').textContent=d.strategy_mode||'—';
    document.getElementById('sPositions').textContent=d.open_positions||0;
    document.getElementById('sCooldowns').textContent=d.active_cooldowns||0;

    // Issues
    const issues=d.issues||[];
    const iEl=document.getElementById('issuePanel');
    if(!issues.length){iEl.innerHTML='<div class="no-issues">All systems operational</div>';}
    else{iEl.innerHTML=issues.map(i=>`<div class="issue-row">${i}</div>`).join('');}

    // Process list
    const procs=d.processes||{};
    const pEl=document.getElementById('procList');
    pEl.innerHTML=Object.entries(procs).map(([name,ok])=>`
      <div class="proc-row">
        <div class="proc-dot ${ok?'proc-dot-on':'proc-dot-off'}"></div>
        <div class="proc-name">${name}</div>
        <div class="proc-status ${ok?'proc-on':'proc-off'}">${ok?'Running':'Stopped'}</div>
      </div>`).join('');

    // Runtime services
    const services=d.phase_health||{};
    const serviceNames={
      phase1_data_feeds:   'Market Data Feeds',
      phase2_whale_intel:  'Whale Intelligence',
      phase3_order_flow:   'Order Flow',
      phase4_narrative_ai: 'Narrative AI',
      phase7_intel_alerts: 'Intelligence Alerts',
    };
    const serviceEl=document.getElementById('serviceList');
    if(serviceEl){
      serviceEl.innerHTML=Object.entries(serviceNames).map(([key,name])=>{
        const ok=services[key];
        return `<div class="proc-row">
          <div class="proc-dot ${ok?'proc-dot-on':'proc-dot-off'}"></div>
          <div class="proc-name">${name}</div>
          <div class="proc-status ${ok?'proc-on':'proc-off'}">${ok?'Active':'Inactive'}</div>
        </div>`;
      }).join('');
    }

    const runtimePanel = document.getElementById('runtimePanel');
    const runtimeBadge = document.getElementById('runtimeBadge');
    const runtimeMode = d.strategy_mode || 'playbook_only';
    if(runtimeBadge) runtimeBadge.textContent = runtimeMode;
    if(runtimePanel){
      runtimePanel.innerHTML = `
        <div class="train-grid">
          <div class="train-card">
            <div class="train-top">
              <div class="train-name">Runtime</div>
              <div class="train-state ${issues.length ? 'am' : 'gn'}">${issues.length ? 'guarded' : 'ready'}</div>
            </div>
            <div class="train-meta">Mode: ${runtimeMode}</div>
            <div class="train-meta">Open positions: ${d.open_positions || 0}</div>
            <div class="train-meta">Cooldowns: ${d.active_cooldowns || 0}</div>
          </div>
          <div class="train-card">
            <div class="train-top">
              <div class="train-name">Decision flow</div>
              <div class="train-state ${funnel.approved || funnel.executed ? 'gn' : 'am'}">seed ${funnel.seeded} → exec ${funnel.executed}</div>
            </div>
            <div class="train-meta">Structure ${funnel.structure} · confirmation ${funnel.confirm} · approved ${funnel.approved}</div>
            <div class="train-meta">Closed ${funnel.closed} · waits ${gateCounts.confirmation_wait_count}</div>
            <div class="train-issue">The runtime now exposes the same seed-to-exec funnel the bot uses for entry-quality gating.</div>
          </div>
        </div>`;
    }

    const freshness = snapshot.source_health || {};
    const freshnessNames = {
      technicals: 'Technicals',
      order_book: 'Order Book',
      liquidations: 'Liquidations',
      funding_rate: 'Funding & OI',
    };
    const freshnessKeys = Object.keys(freshnessNames);
    const seenFreshness = freshnessKeys.filter(k => freshness[k] && freshness[k].status !== 'never_seen');
    const freshNow = seenFreshness.filter(k => freshness[k] && freshness[k].fresh);
    const integrityBadge = document.getElementById('integrityBadge');
    const integrityState = issues.length || Number(d.recent_error_count || 0) || (seenFreshness.length && freshNow.length !== seenFreshness.length)
      ? 'Guarded'
      : d.processes?.['TradingCore']
        ? 'Stable'
        : 'Warming';
    integrityBadge.textContent = integrityState;
    integrityBadge.style.color = integrityState === 'Stable' ? 'var(--gr)' : integrityState === 'Guarded' ? 'var(--am)' : 'var(--bl)';
    document.getElementById('integrityPanel').innerHTML = `
      <div class="train-grid">
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Process truth</div>
            <div class="train-state ${d.processes?.['TradingCore'] ? 'gn' : 'rd'}">${d.processes?.['TradingCore'] ? 'online' : 'offline'}</div>
          </div>
          <div class="train-meta">${Object.values(procs).filter(Boolean).length}/${Object.keys(procs).length} tracked processes running</div>
          <div class="train-issue">${issues.length ? `${issues.length} active issue flags in the monitor` : 'No monitor issue flags right now'}</div>
        </div>
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Feed freshness</div>
            <div class="train-state ${freshNow.length === seenFreshness.length && seenFreshness.length ? 'gn' : seenFreshness.length ? 'am' : 'bl'}">${seenFreshness.length ? `${freshNow.length}/${seenFreshness.length}` : 'waiting'}</div>
          </div>
          <div class="train-meta">${seenFreshness.length ? `${freshNow.length} of ${seenFreshness.length} tracked feeds are fresh` : 'No live freshness samples yet'}</div>
          <div class="train-issue">${Number(d.stale_source_count || 0)} stale sources · ${Number(d.never_seen_source_count || 0)} never seen</div>
        </div>
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Execution friction</div>
            <div class="train-state ${Number(d.recent_error_count || 0) ? 'am' : 'gn'}">${Number(d.recent_error_count || 0) ? 'watch' : 'clean'}</div>
          </div>
          <div class="train-meta">Recent errors ${Number(d.recent_error_count || 0)} · cooldowns ${Number(d.active_cooldowns || 0)}</div>
          <div class="train-issue">${Number(d.open_positions || 0)} open positions under the current runtime</div>
        </div>
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Operator trust</div>
            <div class="train-state ${integrityState === 'Stable' ? 'gn' : integrityState === 'Guarded' ? 'am' : 'bl'}">${integrityState.toLowerCase()}</div>
          </div>
          <div class="train-meta">Mode ${runtimeMode} · balance ${(d.balance || 0).toFixed(2)}</div>
          <div class="train-issue">${integrityState === 'Stable' ? 'All major runtime surfaces agree with the current monitor view.' : 'Use the freshness, latency, and gate panels before trusting the desk blindly.'}</div>
        </div>
      </div>`;

    const metrics = overview.metrics || {};
    const errors = overview.errors || {};
    const staleCount = Number(d.stale_source_count || 0);
    const recentErrors = Number(d.recent_error_count || 0);
    const decisionLatency = Number(metrics.decision?.p95_ms || metrics.decision?.avg_ms || 0);
    const telegramLatency = Number(metrics.telegram_send?.p95_ms || metrics.telegram_send?.avg_ms || 0);
    const frictionState = recentErrors || staleCount || cpu >= 85 || ram >= 85 ? 'Guarded' : decisionLatency >= 800 || telegramLatency >= 1000 ? 'Watch' : 'Clean';
    document.getElementById('execFrictionBadge').textContent = frictionState;
    document.getElementById('execFrictionPanel').innerHTML = `
      <div class="train-grid">
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Latency drag</div>
            <div class="train-state ${decisionLatency >= 800 ? 'am' : 'gn'}">${decisionLatency ? decisionLatency.toFixed(0) + 'ms' : '—'}</div>
          </div>
          <div class="train-meta">Decision P95${telegramLatency ? ` · Telegram ${telegramLatency.toFixed(0)}ms` : ''}</div>
          <div class="train-issue">${decisionLatency >= 800 ? 'Decision latency is elevated enough to slow entries.' : 'Latency is not the main desk drag right now.'}</div>
        </div>
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Gate pressure</div>
            <div class="train-state ${gateCounts.confirmation_wait_count || gateCounts.extension_veto_count || gateCounts.regime_veto_count || gateCounts.cluster_veto_count ? 'am' : 'gn'}">${gateCounts.confirmation_wait_count + gateCounts.extension_veto_count + gateCounts.weak_candle_veto_count + gateCounts.weak_session_veto_count + gateCounts.target_efficiency_veto_count + gateCounts.regime_veto_count + gateCounts.cluster_veto_count}</div>
          </div>
          <div class="train-meta">Conf ${gateCounts.confirmation_wait_count} · Retest ${gateCounts.retest_missing_count} · Pullback ${gateCounts.pullback_missing_count}</div>
          <div class="train-issue">Ext ${gateCounts.extension_veto_count} · Quality ${gateCounts.weak_candle_veto_count}/${gateCounts.weak_session_veto_count}/${gateCounts.target_efficiency_veto_count} · Policy ${gateCounts.regime_veto_count}/${gateCounts.cluster_veto_count}</div>
        </div>
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Feed friction</div>
            <div class="train-state ${staleCount ? 'am' : 'gn'}">${staleCount}</div>
          </div>
          <div class="train-meta">${Number(d.never_seen_source_count || 0)} never-seen sources · ${Object.keys(snapshot.source_health || {}).length} tracked feeds</div>
          <div class="train-issue">${staleCount ? 'Stale feeds are directly increasing execution uncertainty.' : 'No tracked stale-feed drag right now.'}</div>
        </div>
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Resource drag</div>
            <div class="train-state ${cpu >= 85 || ram >= 85 ? 'rd' : cpu >= 70 || ram >= 70 ? 'am' : 'gn'}">${Math.max(cpu, ram).toFixed(0)}%</div>
          </div>
          <div class="train-meta">CPU ${cpu.toFixed(1)}% · RAM ${ram.toFixed(1)}%</div>
          <div class="train-issue">${cpu >= 85 || ram >= 85 ? 'Machine pressure can slow chart, signal, and alert surfaces together.' : 'Host resources are not the leading source of friction.'}</div>
        </div>
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Error churn</div>
            <div class="train-state ${recentErrors ? 'am' : 'gn'}">${recentErrors}</div>
          </div>
          <div class="train-meta">${errors.rate_per_min || 0}/min recent error rate</div>
          <div class="train-issue">${recentErrors ? 'Recent runtime errors are still adding operator noise.' : 'Error tracker is currently quiet.'}</div>
        </div>
      </div>`;

    const funnelBadge = document.getElementById('funnelBadge');
    if(funnelBadge) funnelBadge.textContent = `${funnel.seeded} → ${funnel.executed}`;
    const funnelPanel = document.getElementById('funnelPanel');
    if(funnelPanel){
      funnelPanel.innerHTML = `
        <div class="train-grid">
          <div class="train-card">
            <div class="train-top"><div class="train-name">Seeded</div><div class="train-state cy">${funnel.seeded}</div></div>
            <div class="train-meta">Raw signals that reached the entry stack</div>
            <div class="train-issue">Seeded ideas include both live signals and near-miss candidates.</div>
          </div>
          <div class="train-card">
            <div class="train-top"><div class="train-name">Structure valid</div><div class="train-state ${funnel.structure ? 'gn' : 'mu'}">${funnel.structure}</div></div>
            <div class="train-meta">Retest / pullback structure still intact</div>
            <div class="train-issue">This is the pre-confirmation path the bot is watching.</div>
          </div>
          <div class="train-card">
            <div class="train-top"><div class="train-name">Confirmation ready</div><div class="train-state ${funnel.confirm ? 'am' : 'mu'}">${funnel.confirm}</div></div>
            <div class="train-meta">Bars or reclaim checks satisfied</div>
            <div class="train-issue">Signals here are past the main waiting gate.</div>
          </div>
          <div class="train-card">
            <div class="train-top"><div class="train-name">Approved</div><div class="train-state ${funnel.approved ? 'gn' : 'mu'}">${funnel.approved}</div></div>
            <div class="train-meta">Reviewed and cleared by the policy stack</div>
            <div class="train-issue">Approved setups are past quality, regime, and cluster gates.</div>
          </div>
          <div class="train-card">
            <div class="train-top"><div class="train-name">Executed</div><div class="train-state ${funnel.executed ? 'gn' : 'mu'}">${funnel.executed}</div></div>
            <div class="train-meta">Live positions entered from the funnel</div>
            <div class="train-issue">${funnel.closed} closed in the tracked window</div>
          </div>
        </div>`;
    }

    // Data freshness
    const freshEl = document.getElementById('freshnessPanel');
    const staleMapEl = document.getElementById('staleMapPanel');
    const seenCount = seenFreshness.length;
    const freshCount = freshNow.length;
    const staleKeys = freshnessKeys.filter(k => freshness[k] && freshness[k].status !== 'never_seen' && !freshness[k].fresh);
    document.getElementById('freshBadge').textContent = seenCount ? `${freshCount}/${freshnessKeys.length} fresh` : 'warming';
    if(!seenCount){
      freshEl.innerHTML = '<div class="empty">Waiting for live telemetry from the data stack</div>';
    }else{
      freshEl.innerHTML = `<div class="fresh-grid">${freshnessKeys.map((key)=>{
        const item = freshness[key] || {};
        const ok = !!item.fresh;
        const status = item.status === 'never_seen' ? 'waiting' : ok ? 'fresh' : 'stale';
        const age = item.age_secs == null ? 'No samples yet' : `${item.age_secs.toFixed ? item.age_secs.toFixed(1) : item.age_secs}s old`;
        const threshold = item.threshold == null ? '—' : `Threshold ${item.threshold}s`;
        return `<div class="fresh-card">
          <div class="fresh-top">
            <div class="proc-dot ${ok?'proc-dot-on':'proc-dot-off'}"></div>
            <div class="fresh-name">${freshnessNames[key]}</div>
            <div class="fresh-state ${ok?'proc-on':'proc-off'}">${status}</div>
          </div>
          <div class="fresh-age">${age}</div>
          <div class="fresh-threshold">${threshold}</div>
        </div>`;
      }).join('')}</div>`;
    }
    document.getElementById('staleMapBadge').textContent = staleKeys.length ? `${staleKeys.length} stale` : seenCount ? 'Clear' : 'Waiting';
    if(!seenCount){
      staleMapEl.innerHTML = '<div class="empty">The stale-feed map will appear after the monitor sees live source telemetry.</div>';
    }else if(!staleKeys.length){
      staleMapEl.innerHTML = '<div class="empty">No tracked feeds are stale right now.</div>';
    }else{
      staleMapEl.innerHTML = `<div class="fresh-grid">${staleKeys.map((key) => {
        const item = freshness[key] || {};
        const age = item.age_secs == null ? 'No samples yet' : `${item.age_secs.toFixed ? item.age_secs.toFixed(1) : item.age_secs}s old`;
        const threshold = item.threshold == null ? 'Threshold —' : `Threshold ${item.threshold}s`;
        return `<div class="fresh-card">
          <div class="fresh-top">
            <div class="proc-dot proc-dot-off"></div>
            <div class="fresh-name">${freshnessNames[key] || key}</div>
            <div class="fresh-state proc-off">stale</div>
          </div>
          <div class="fresh-age">${age}</div>
          <div class="fresh-threshold">${threshold}</div>
        </div>`;
      }).join('')}</div>`;
    }

    // Connections
    const connEl=document.getElementById('connPanel');
    const igBroker=d.ig_broker||{};
    const feedConnections=d.feed_connections||{};
    const derivFeed=feedConnections.deriv||{};
    const binanceFeed=feedConnections.binance||{};
    const igFeed=feedConnections.ig||{};
    const providerRouting=((overview.command_center||{}).provider_routing)||{};
    const universeCount=Number(providerRouting.asset_count||0);
    const connItems=[
      {name:'Trading Engine',ok:procs['TradingCore']},
      {name:'Web Dashboard',ok:procs['Web dashboard']},
      {name:'Asset Universe',ok:universeCount>0, meta:universeCount?`${universeCount} tracked assets across routed feeds`:''},
      {name:'Deriv Stream',ok:!!derivFeed.connected, meta:derivFeed.symbol_count?`${derivFeed.symbol_count} symbols`:derivFeed.assets||''},
      {name:'Binance Fallback',ok:!!binanceFeed.connected, meta:binanceFeed.symbol_count?`${binanceFeed.symbol_count} symbols`:binanceFeed.assets||''},
      {name:'Exchange Feeds',ok:services['phase1_data_feeds']},
      {name:'IG Routed Data',ok:!!igFeed.connected, meta:igFeed.symbol_count?`${igFeed.symbol_count} assets`:igFeed.assets||''},
      {name:'PostgreSQL',ok:procs['PostgreSQL']},
      {name:'Redis',ok:procs['Redis']},
      {name:'Command Bot',ok:procs['Telegram']},
      {name:'Outcome Tracker',ok:procs['PredTracker']},
      {name:'Intel Alerts',ok:services['phase7_intel_alerts']},
    ];
    connEl.innerHTML='<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:12px 16px">'+
      connItems.map(c=>`<div style="display:flex;align-items:center;gap:8px;padding:8px 10px;background:var(--s2);border-radius:7px">
        <div style="width:8px;height:8px;border-radius:50%;background:${c.ok?'var(--gr)':'var(--rd)'}"></div>
        <div style="display:flex;flex-direction:column;gap:2px">
          <span style="font-size:12px">${c.name}</span>
          ${c.meta ? `<span style="font-size:10px;color:var(--tx3)">${c.meta}</span>` : ''}
        </div>
        <span style="margin-left:auto;font-size:11px;color:${c.ok?'var(--gr)':'var(--rd)'};font-weight:600">${c.ok?'OK':'Down'}</span>
      </div>`).join('')+'</div>';

    // IG broker panel
    const igPanel=document.getElementById('igPanel');
    const igBadge=document.getElementById('igBadge');
    if(!igBroker.enabled){
      igBadge.textContent='disabled';
      igPanel.innerHTML='<div class="empty">IG routing is disabled</div>';
    }else if(!igBroker.authenticated){
      igBadge.textContent='offline';
      igBadge.style.color='var(--rd)';
      igPanel.innerHTML=`<div class="empty">IG auth/data unavailable: ${igBroker.error_message||igBroker.error_code||'unknown error'}</div>`;
    }else{
      igBadge.textContent=(igBroker.environment||'').toUpperCase()||'ready';
      igBadge.style.color='var(--gr)';
      const watchlists=(igBroker.watchlists||[]).map(w=>w.name).filter(Boolean);
      const activities=(igBroker.recent_activities||[]);
      igPanel.innerHTML=`
        <div class="stat-mini-grid">
          <div class="sm"><div class="sm-val bl">${(igBroker.environment||'').toUpperCase()||'—'}</div><div class="sm-lbl">Environment</div></div>
          <div class="sm"><div class="sm-val">${igBroker.account_id||'—'}</div><div class="sm-lbl">Account</div></div>
          <div class="sm"><div class="sm-val gn">${igBroker.balance==null?'—':'$'+Number(igBroker.balance).toFixed(2)}</div><div class="sm-lbl">IG Balance</div></div>
          <div class="sm"><div class="sm-val cy">${igBroker.available==null?'—':'$'+Number(igBroker.available).toFixed(2)}</div><div class="sm-lbl">Available</div></div>
        </div>
        <div style="padding:0 16px 12px;font-size:12px;color:var(--tx2);line-height:1.6">
          Routed assets: ${(igBroker.routed_assets||[]).join(', ') || '—'}<br>
          Watchlists: ${watchlists.length ? watchlists.join(', ') : 'none'}<br>
          Recent account activity: ${activities.length||0}
        </div>`;
    }

  }catch(e){console.error(e);}
}

// Latency metrics
async function loadLatency(){
  try{
    const overview = await fetchSystemMonitorOverview();
    const d = overview || {};
    if(!d.success)return;
    const m=d.metrics||{};
    const order=['decision','prediction','backtest','sentiment_fetch','telegram_send'];
    const labels={'decision':'Decision Engine','prediction':'Playbook Scan','backtest':'Replay Audit',
                  'sentiment_fetch':'Sentiment','telegram_send':'Telegram'};
    const items=order.filter(k=>m[k]&&m[k].count>0);
    if(!items.length){document.getElementById('latencyPanel').innerHTML='<div class="empty">No data yet — signals needed</div>';return;}
    const maxVal=Math.max(...items.map(k=>m[k].p95_ms||0),1);
    const barColor=ms=>ms<500?'var(--gr)':ms<2000?'var(--am)':'var(--rd)';
    document.getElementById('latBadge').textContent=items.length+' tracked';
    document.getElementById('latencyPanel').innerHTML=items.map(k=>{
      const s=m[k];const pct=Math.min(100,(s.p95_ms/maxVal)*100);
      return`<div class="lat-row">
        <div class="lat-name">${labels[k]||k}</div>
        <div class="lat-bar-wrap"><div class="lat-bar" style="width:${pct}%;background:${barColor(s.p95_ms)}"></div></div>
        <div class="lat-val" style="color:${barColor(s.p95_ms)}">${s.avg_ms.toFixed(0)}ms avg</div>
        <div style="font-size:10px;color:var(--tx3);width:60px;text-align:right">P95:${s.p95_ms.toFixed(0)}ms</div>
      </div>`;
    }).join('');
  }catch(e){}
}

// Error tracker
async function loadErrors(){
  try{
    const overview = await fetchSystemMonitorOverview();
    const d = overview || {};
    if(!d.success)return;
    const errs=(d.errors || {});
    const rate=errs.rate_per_min||0;
    const last10=errs.last_10||[];
    document.getElementById('errBadge').textContent=rate+'/min';
    document.getElementById('errBadge').style.color=rate>=5?'var(--rd)':rate>=1?'var(--am)':'var(--gr)';
    if(!last10.length){document.getElementById('errorPanel').innerHTML='<div class="empty" style="color:var(--gr)">✅ No recent errors</div>';return;}
    const timeAgo=ts=>{const m=Math.floor((Date.now()/1000-ts)/60);return m<1?'now':m+'m ago';};
    document.getElementById('errorPanel').innerHTML=last10.slice().reverse().map(e=>`
      <div class="err-row">
        <div class="err-module">${e.module||'unknown'}  <span style="color:var(--tx3);font-weight:400">${timeAgo(e.ts)}</span></div>
        <div class="err-msg">${(e.message||'').slice(0,120)}</div>
      </div>`).join('');
  }catch(e){}
}

// Decision quality
async function loadSignalPerf(){
  try{
    const overview = await fetchSystemMonitorOverview();
    const d = overview || {};
    if(!d.success)return;
    const sigs=d.snapshot || {};
    const commandCenter = d.command_center || {};
    const liveSignals = Array.isArray(commandCenter.latest_signals) ? commandCenter.latest_signals : [];
    const tradeLifecycle = commandCenter.trade_lifecycle || {};
    const gateCounts = deriveDecisionGateCounts(liveSignals);
    const funnel = deriveDecisionFunnel(liveSignals, tradeLifecycle, Number(d.health?.open_positions || 0));
    const kills=sigs.signal_kills||{};
    const total=sigs.total_signals||0;
    const wr=((sigs.win_rate||0)*100).toFixed(1);
    document.getElementById('sigBadge').textContent=total+' tracked';

    const killEntries=Object.entries(kills).sort((a,b)=>b[1]-a[1]);
    const killHtml=killEntries.length?killEntries.map(([name,count])=>`
      <div class="kill-card">
        <div class="kill-val">${count}</div>
        <div class="kill-lbl">${name.replaceAll('_',' ')}</div>
      </div>`).join('')
      :'<div style="color:var(--tx3);font-size:12px">No rejects recorded yet</div>';

    document.getElementById('signalPerfPanel').innerHTML=`
      <div class="train-grid">
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Signal yield</div>
            <div class="train-state ${Number(wr) >= 55 ? 'gn' : Number(wr) >= 45 ? 'am' : 'rd'}">${wr}%</div>
          </div>
          <div class="train-meta">${total} tracked signals</div>
          <div class="train-issue">Live win rate from the current signal snapshot.</div>
        </div>
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Decision funnel</div>
            <div class="train-state ${funnel.executed ? 'gn' : 'am'}">${funnel.seeded} → ${funnel.executed}</div>
          </div>
          <div class="train-meta">Seed ${funnel.seeded} · Structure ${funnel.structure} · Confirm ${funnel.confirm}</div>
          <div class="train-issue">Approved ${funnel.approved} · Closed ${funnel.closed}</div>
        </div>
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Gate profile</div>
            <div class="train-state ${gateCounts.confirmation_wait_count || gateCounts.extension_veto_count || gateCounts.regime_veto_count || gateCounts.cluster_veto_count ? 'am' : 'gn'}">${gateCounts.confirmation_wait_count + gateCounts.extension_veto_count + gateCounts.weak_candle_veto_count + gateCounts.weak_session_veto_count + gateCounts.target_efficiency_veto_count + gateCounts.regime_veto_count + gateCounts.cluster_veto_count}</div>
          </div>
          <div class="train-meta">Conf ${gateCounts.confirmation_wait_count} · Retest ${gateCounts.retest_missing_count} · Pullback ${gateCounts.pullback_missing_count}</div>
          <div class="train-issue">Ext ${gateCounts.extension_veto_count} · Candle ${gateCounts.weak_candle_veto_count} · Session ${gateCounts.weak_session_veto_count} · Target ${gateCounts.target_efficiency_veto_count}</div>
        </div>
        <div class="train-card">
          <div class="train-top">
            <div class="train-name">Policy blocks</div>
            <div class="train-state ${gateCounts.regime_veto_count || gateCounts.cluster_veto_count ? 'rd' : 'gn'}">${gateCounts.regime_veto_count + gateCounts.cluster_veto_count}</div>
          </div>
          <div class="train-meta">Old impulse ${gateCounts.old_impulse_veto_count} · blocked ${gateCounts.blocked_candidate_count}</div>
          <div class="train-issue">${killEntries.length ? 'Reject breakdown is still surfacing the dominant exact blockers.' : 'No reject breakdown has been recorded yet.'}</div>
        </div>
      </div>
      <div style="margin-top:14px">
        <div class="kill-grid">${killHtml}</div>
      </div>`;
  }catch(e){}
}

function loadAll(){load();loadLatency();loadErrors();loadSignalPerf();}
initSparks();loadAll();setInterval(() => { if(!document.hidden) loadAll(); },15000);
document.addEventListener('visibilitychange', () => { if(!document.hidden){ _systemMonitorCache = null; loadAll(); }});

