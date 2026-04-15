
setInterval(()=>{document.getElementById('clock').textContent=new Date(Date.now()+3*3600000).toUTCString().substring(17,25)+' EAT';},1000);

const DASHBOARD_TIME_ZONE='Africa/Nairobi';

function fmtNum(n,dec=2){return n==null?'—':(n>=0?'+':'')+Number(n).toFixed(dec)}
function fmtMoney(n){return n==null?'—':'$'+Number(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}
function colorClass(n){return n>=0?'gn':'rd'}
function humanToken(v){return String(v||'').replace(/_/g,' ')}
function provenanceLine(item){
  const bits=[];
  const hist=item.history_source?`Hist ${humanToken(item.history_source)}`:'';
  const live=(item.live_source||item.runtime_primary_provider)?`Live ${humanToken(item.live_source||item.runtime_primary_provider)}`:'';
  const quote=item.quote_mode?humanToken(item.quote_mode):'';
  const histClass=item.history_source_class?humanToken(item.history_source_class):'';
  if(hist)bits.push(hist);
  if(live)bits.push(live);
  if(quote)bits.push(quote);
  if(histClass)bits.push(histClass);
  return bits.join(' · ');
}
function parseDashboardTime(raw){
  if(!raw)return null;
  const text=String(raw).trim();
  if(!text)return null;
  const normalized=(text.endsWith('Z')||/[+-]\d{2}:\d{2}$/.test(text)||/[+-]\d{4}$/.test(text))?text:text+'Z';
  const dt=new Date(normalized);
  return Number.isNaN(dt.getTime())?null:dt;
}
function fmtDashboardTime(raw){
  const dt=parseDashboardTime(raw);
  if(!dt)return'—';
  return new Intl.DateTimeFormat('en-GB',{timeZone:DASHBOARD_TIME_ZONE,day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',hour12:false}).format(dt);
}
function depthLabel(item){
  if(item.depth_mode)return humanToken(item.depth_mode)
  if(item.depth_available)return 'true depth';
  if(item.synthetic_depth_available)return 'synthetic depth';
  return 'top of book';
}
function recentPatternLine(item){
  const notes=Array.isArray(item.recent_pattern_notes)?item.recent_pattern_notes:[];
  if(item.recent_pattern_block_new_entries)return 'Pattern blocked';
  if(notes.length)return humanToken(notes[0]);
  return '';
}
function titleToken(value,fallback='—'){
  const raw=String(value||'').trim();
  if(!raw)return fallback;
  return raw.replace(/_/g,' ').replace(/\b\w/g,m=>m.toUpperCase());
}
function metricNumber(value, digits=0, fallback='—'){
  const num=Number(value);
  return Number.isFinite(num)?num.toFixed(digits):fallback;
}
function itemMeta(item){
  return item && typeof item.metadata === 'object' && item.metadata ? item.metadata : {};
}
function itemValue(item,key){
  const meta=itemMeta(item);
  return meta[key] !== undefined ? meta[key] : item ? item[key] : undefined;
}
function itemText(item,key){
  const value=itemValue(item,key);
  return value == null ? '' : String(value).trim();
}
function itemNum(item,key,fallback=0){
  const num=Number(itemValue(item,key));
  return Number.isFinite(num)?num:fallback;
}
function itemBool(item,key){
  const value=itemValue(item,key);
  return value === true ? true : value === false ? false : null;
}
function itemList(item,key){
  const value=itemValue(item,key);
  if(Array.isArray(value))return value.map(v=>String(v||'').trim()).filter(Boolean);
  if(typeof value === 'string'){const raw=value.trim();return raw?[raw]:[];}
  return [];
}
function qualityScoreText(value, digits=0, fallback='—'){
  const num=Number(value);
  if(!Number.isFinite(num))return fallback;
  return (num <= 1.25 ? num*100 : num).toFixed(digits)+'%';
}
function extensionScoreText(value, digits=2, fallback='—'){
  const num=Number(value);
  return Number.isFinite(num)?num.toFixed(digits):fallback;
}
function eliteRankText(value){
  const num=Number(value);
  if(!Number.isFinite(num) || num <= 0)return '—';
  return num <= 1.25 ? (num*100).toFixed(0)+'%' : num.toFixed(0);
}
function hasEntryStateContext(item){
  return Boolean(
    itemText(item,'exact_kill_reason')
    || itemText(item,'execution_kill_reason')
    || itemText(item,'blocked_reason')
    || itemNum(item,'entry_confirmation_count',0) > 0
    || itemNum(item,'entry_confirmation_bars_required',0) > 0
    || itemNum(item,'extension_score',0) > 0
    || itemNum(item,'candle_quality_score',0) > 0
    || itemNum(item,'session_quality_score',0) > 0
    || itemNum(item,'target_efficiency_score',0) > 0
    || itemNum(item,'cluster_penalty',0) > 0
    || itemNum(item,'impulse_age_bars',0) > 0
    || itemBool(item,'breakout_retest_ready') === true
    || itemBool(item,'first_pullback_ready') === true
    || itemBool(item,'failed_opposite_move_confirmed') === true
    || itemBool(item,'entry_confirmation_ready') === true
  );
}
function exactKillReason(item){
  const hardBlocks=itemList(item,'execution_hard_blocks');
  const lateReasons=itemList(item,'late_entry_risk_reasons');
  const rejected=itemList(item,'rejected_reasons');
  return titleToken(
    itemText(item,'exact_kill_reason')
    || itemText(item,'execution_kill_reason')
    || itemText(item,'blocked_reason')
    || itemText(item,'kill_reason')
    || itemText(item,'killed_by')
    || itemText(item,'reason')
    || (hardBlocks[0] || '')
    || (lateReasons[0] || '')
    || (rejected[0] || '')
    || String(item?.top_blocker || item?.lead_blocker || ''),
    'No kill reason'
  );
}
function knownKillReason(item){
  const reason=exactKillReason(item);
  return reason && reason !== 'No kill reason' ? reason : '';
}
function reviewNotesLine(item){
  const notes=[...itemList(item,'market_review_notes'),...itemList(item,'execution_review_notes')];
  return notes.length?notes.join(' · '):'';
}
function stageClass(stage){
  const key=String(stage||'').toLowerCase();
  if(key==='open')return 'tape-open';
  if(key==='partial')return 'tape-partial';
  return 'tape-closed';
}
function eliteEntryState(item){
  const confReady=itemBool(item,'entry_confirmation_ready')===true;
  const retestReady=itemBool(item,'breakout_retest_ready')===true;
  const pullbackReady=itemBool(item,'first_pullback_ready')===true;
  const failedOpposite=itemBool(item,'failed_opposite_move_confirmed')===true;
  const count=itemNum(item,'entry_confirmation_count',0);
  const required=itemNum(item,'entry_confirmation_bars_required',0);
  const extension=itemNum(item,'extension_score',0);
  const candle=itemNum(item,'candle_quality_score',0);
  const session=itemNum(item,'session_quality_score',0);
  const target=itemNum(item,'target_efficiency_score',0);
  const cluster=itemNum(item,'cluster_penalty',0);
  const kill=exactKillReason(item);
  const killKnown=kill && kill !== 'No kill reason';
  const structureReady=retestReady || pullbackReady || failedOpposite;
  if(confReady && (structureReady || required <= 0) && extension < 0.80 && candle >= 0.55 && session >= 0.55 && target >= 0.50 && cluster < 0.45 && !killKnown){
    return {label:'Ready', className:'gn', pill:'ready'};
  }
  if(killKnown || extension >= 0.80 || cluster >= 0.70){
    return {label:'Blocked', className:'rd', pill:'blocked'};
  }
  if(!hasEntryStateContext(item)){
    return {label:'Waiting', className:'am', pill:'waiting'};
  }
  if(required > 0 && count < required){
    return {label:'Waiting', className:'am', pill:'waiting'};
  }
  return {label:'Waiting', className:'am', pill:'waiting'};
}
function confirmationLine(item){
  const ready=itemBool(item,'entry_confirmation_ready')===true;
  const count=itemNum(item,'entry_confirmation_count',0);
  const required=itemNum(item,'entry_confirmation_bars_required',0);
  if(ready)return `Confirmation ready${required?` · ${count}/${required} bars`:''}`;
  if(required)return `Waiting confirmation · ${count}/${required} bars`;
  return 'Confirmation pending';
}
function readinessBits(item){
  const bits=[];
  const retest=itemBool(item,'breakout_retest_ready');
  const pullback=itemBool(item,'first_pullback_ready');
  const failedOpposite=itemBool(item,'failed_opposite_move_confirmed');
  bits.push(retest===true?'Retest ready':retest===false?'Retest missing':'Retest —');
  bits.push(pullback===true?'Pullback ready':pullback===false?'Pullback missing':'Pullback —');
  bits.push(confirmationLine(item));
  if(failedOpposite!=null)bits.push(failedOpposite?'Opposite move failed':'Opposite move not failed');
  return bits;
}
function qualityBits(item){
  const bits=[];
  const pattern=itemText(item,'pattern_family');
  const rank=itemValue(item,'elite_pattern_rank');
  const impulse=itemValue(item,'impulse_age_bars');
  const extension=itemValue(item,'extension_score');
  const candle=itemValue(item,'candle_quality_score');
  const session=itemValue(item,'session_quality_score');
  const target=itemValue(item,'target_efficiency_score');
  const cluster=itemValue(item,'cluster_penalty');
  if(pattern)bits.push(`Pattern ${titleToken(pattern)}`);
  if(rank!=null && rank !== '' && eliteRankText(rank) !== '—')bits.push(`Elite ${eliteRankText(rank)}`);
  if(Number(impulse||0) > 0)bits.push(`Impulse ${metricNumber(impulse,0)} bars`);
  if(Number(extension||0) > 0)bits.push(`Ext ${extensionScoreText(extension,2)}`);
  if(Number(candle||0) > 0)bits.push(`Candle ${qualityScoreText(candle,0)}`);
  if(Number(session||0) > 0)bits.push(`Session ${qualityScoreText(session,0)}`);
  if(Number(target||0) > 0)bits.push(`Target ${qualityScoreText(target,0)}`);
  if(Number(cluster||0) > 0)bits.push(`Cluster ${metricNumber(cluster,2)}`);
  return bits;
}
function confirmationProgress(item){
  const count=itemNum(item,'entry_confirmation_count',0);
  const required=itemNum(item,'entry_confirmation_bars_required',0);
  if(required>0)return Math.max(0,Math.min(100,(count/required)*100));
  return itemBool(item,'entry_confirmation_ready')===true?100:0;
}
function regimeBits(item){
  return [itemText(item,'regime_policy_summary'),itemText(item,'regime_policy'),itemText(item,'regime_label')].filter(Boolean).map(v=>titleToken(v));
}
function buildEliteReason(item){
  const state=eliteEntryState(item);
  if(!hasEntryStateContext(item) && item.session_open){
    return 'Monitoring session for a qualified structure';
  }
  if(state.label==='Ready'){
    const path=itemBool(item,'breakout_retest_ready')===true?'Retest':itemBool(item,'first_pullback_ready')===true?'Pullback':'Confirmed';
    return `${path} lane armed · ${confirmationLine(item)}`;
  }
  if(state.label==='Blocked'){
    return knownKillReason(item) || buildQualityPressure(item) || 'Entry review found a hard block';
  }
  return confirmationLine(item);
}
function buildQualityPressure(item){
  const extension=itemNum(item,'extension_score',0);
  const candle=itemNum(item,'candle_quality_score',0);
  const session=itemNum(item,'session_quality_score',0);
  const target=itemNum(item,'target_efficiency_score',0);
  const cluster=itemNum(item,'cluster_penalty',0);
  const issues=[];
  if(extension>=0.80)issues.push('overextended');
  if(candle>0 && candle<0.55)issues.push('weak candle');
  if(session>0 && session<0.55)issues.push('weak session');
  if(target>0 && target<0.50)issues.push('poor target');
  if(cluster>=0.70)issues.push('cluster heavy');
  return issues.join(' · ');
}
function eliteChip(label, cls){return `<span class="elite-chip ${cls||''}">${label}</span>`}
function eliteChipRow(item){
  const chips=[];
  const retest=itemBool(item,'breakout_retest_ready');
  const pullback=itemBool(item,'first_pullback_ready');
  const failedOpposite=itemBool(item,'failed_opposite_move_confirmed');
  const pattern=itemText(item,'pattern_family');
  const rank=itemValue(item,'elite_pattern_rank');
  const cluster=itemNum(item,'cluster_penalty',0);
  chips.push(eliteChip(retest===true?'Retest ready':retest===false?'Retest missing':'Retest —',retest===true?'ok':retest===false?'warn':''));
  chips.push(eliteChip(pullback===true?'Pullback ready':pullback===false?'Pullback missing':'Pullback —',pullback===true?'ok':pullback===false?'warn':''));
  chips.push(eliteChip(confirmationLine(item),itemBool(item,'entry_confirmation_ready')===true?'ok':'warn'));
  if(failedOpposite!=null)chips.push(eliteChip(failedOpposite?'Opposite move failed':'Opp move live',failedOpposite?'ok':'warn'));
  if(pattern)chips.push(eliteChip(`Pattern ${titleToken(pattern)}`,'info'));
  if(rank!=null && rank !== '' && eliteRankText(rank) !== '—')chips.push(eliteChip(`Elite ${eliteRankText(rank)}`,'info'));
  if(itemValue(item,'cluster_penalty')!=null)chips.push(eliteChip(`Cluster ${metricNumber(cluster,2)}`,cluster>=0.70?'bad':cluster>=0.45?'warn':''));
  return `<div class="elite-chip-row">${chips.join('')}</div>`;
}
function statusPill(item,labelOverride){
  const state=eliteEntryState(item);
  return `<span class="status-pill ${state.pill}">${labelOverride||state.label}</span>`;
}
function sessionRadarState(item){
  if(!item || !item.session_open){
    return {label:'Blocked', className:'am', pill:'blocked'};
  }
  if(!hasEntryStateContext(item)){
    return {label:'Open', className:'gn', pill:'ready'};
  }
  const state=eliteEntryState(item);
  if(state.label !== 'Waiting')return state;
  const required=itemNum(item,'entry_confirmation_bars_required',0);
  const count=itemNum(item,'entry_confirmation_count',0);
  if(required > 0 && count < required){
    return {label:'Confirm', className:'am', pill:'waiting'};
  }
  if(itemBool(item,'breakout_retest_ready') === false){
    return {label:'Retest', className:'am', pill:'waiting'};
  }
  if(itemBool(item,'first_pullback_ready') === false){
    return {label:'Pullback', className:'am', pill:'waiting'};
  }
  return {label:'Setup', className:'am', pill:'waiting'};
}
function renderCommandBoardCards(targetId, items, emptyText){
  const el=document.getElementById(targetId);
  if(!el)return;
  if(!items||!items.length){
    el.innerHTML=`<div class="empty" style="grid-column:1/-1">${emptyText}</div>`;
    return;
  }
  el.innerHTML=items.map(item=>`<div class="board-card"><div class="board-title">${item.title||'Desk read'}</div><div class="board-value ${item.valueClass||'mu'}">${item.value??'—'}</div><div class="board-note">${item.note||''}</div></div>`).join('');
}
function renderDeskRows(targetId, items, emptyText){
  const el=document.getElementById(targetId);
  if(!el)return;
  if(!items||!items.length){
    el.innerHTML=`<div class="empty">${emptyText}</div>`;
    return;
  }
  el.innerHTML=`<div class="desk-list">${items.map(item=>`<div class="desk-row"><div class="desk-main"><div class="desk-head">${item.head||'—'}</div><div class="desk-meta">${item.meta||''}</div></div><div class="desk-side ${item.sideClass||''}">${item.side||''}</div></div>`).join('')}</div>`;
}
function renderTradeTape(events){
  const panel=document.getElementById('tradeTapePanel');
  const badge=document.getElementById('tradeTapeBadge');
  if(!panel||!badge)return;
  badge.textContent=events&&events.length?`${events.length} recent`:'Quiet';
  if(!events||!events.length){
    panel.innerHTML='<div class="empty">No recent trade lifecycle events</div>';
    return;
  }
  panel.innerHTML=`<div class="desk-list">${events.map(item=>{
    const eventLine=item.note||titleToken(item.close_reason||item.stage,'No event note');
    const detailLine=[item.event_time?fmtDashboardTime(item.event_time):'', item.continuation_summary||''].filter(Boolean).join(' · ');
    return `<div class="desk-row"><span class="tape-stage ${stageClass(item.stage)}">${titleToken(item.stage,'event')}</span><div class="desk-main"><div class="desk-head">${item.asset||'—'} ${item.direction||''}</div><div class="desk-meta">${eventLine}${detailLine?` · ${detailLine}`:''}</div></div><div class="desk-side ${Number(item.pnl||0)>=0?'gn':'rd'}">${item.pnl==null?'—':fmtMoney(item.pnl)}</div></div>`;
  }).join('')}</div>`;
}
function setActionStatus(message,type='info'){
  const el=document.getElementById('positionActionStatus');
  if(!el)return;
  if(!message){el.className='action-status';el.textContent='';return;}
  el.className='action-status show '+type;el.textContent=message;
}
function renderTopOpportunities(items){
  const panel=document.getElementById('topOpsPanel');
  if(!panel)return;
  if(!items||!items.length){panel.innerHTML='';panel.style.display='none';return;}
  panel.style.display='grid';
  panel.innerHTML=items.slice(0,5).map((item,idx)=>{
    const rank=Number(item.opportunity_rank||idx+1);
    const opp=Number(item.opportunity_score||0);
    const conf=Number(item.confidence||0);
    const mem=Number(item.memory_score||0);
    const execQ=Number(item.execution_quality_score||0);
    const brokerQ=Number(item.broker_quality_score||0);
    const microQ=Number(item.microstructure_score||0);
    const dir=(item.direction||'').toUpperCase();
    const dirCol=dir==='BUY'?'var(--gr)':'var(--rd)';
    const source=item.source==='position'?'Open position':'Live signal';
    const brokerState=humanToken(item.broker_agreement_state||item.broker_context||'');
    const crossPeer=item.cross_asset_primary_peer||'';
    const crossRelation=humanToken(item.cross_asset_primary_relation||'');
    const crossAlign=Number(item.cross_asset_alignment||0);
    const crossLine=crossPeer?`Cross ${crossAlign>=0?'support':'conflict'} via ${crossPeer}${crossRelation?` · ${crossRelation}`:''}`:'';
    const patternLine=recentPatternLine(item);
    const regimeLine=regimeBits(item).join(' · ');
    const killReason=String(item.exact_kill_reason || item.execution_kill_reason || item.kill_reason || item.killed_by || item.reason || '').trim();
    const state=eliteEntryState(item);
    const pressure=buildQualityPressure(item);
    return `<div class="topop-card"><div class="topop-head"><div><div class="topop-rank">Rank ${rank}</div><div class="topop-asset">${item.asset||'?'}</div></div><div class="topop-score">${opp.toFixed(3)}</div></div><div style="display:flex;align-items:center;justify-content:space-between;gap:8px"><div style="font-size:11px;color:${dirCol};font-weight:700">${dir||'—'} · ${(conf*100).toFixed(0)}%</div>${statusPill(item,state.label)}</div><div class="topop-meta">Memory ${mem?mem.toFixed(0):'—'} · Exec ${execQ?execQ.toFixed(0):'—'} · Broker ${brokerQ.toFixed(2)}</div><div class="topop-meta">Micro ${microQ.toFixed(2)} · Depth ${depthLabel(item)}${brokerState?` · ${brokerState}`:''}</div><div class="topop-meta">${buildEliteReason(item)}</div><div class="topop-meta">${qualityBits(item).slice(0,4).join(' · ') || 'Elite metrics pending'}</div>${regimeLine?`<div class="topop-meta">${regimeLine}</div>`:''}${pressure?`<div class="topop-meta">${pressure}</div>`:''}${crossLine?`<div class="topop-meta">${crossLine}</div>`:''}${patternLine?`<div class="topop-meta">${patternLine}</div>`:''}${reviewNotesLine(item)?`<div class="topop-meta">${reviewNotesLine(item)}</div>`:''}${killReason?`<div class="topop-meta">${exactKillReason(item)}</div>`:''}<div class="topop-meta">${source}${item.timeframe?` · ${item.timeframe}`:''}</div></div>`;
  }).join('');
}

let _commandCenterCache = null;
let _commandCenterPending = null;
let _commandCenterStreamAbort = null;
let _commandCenterStreamReconnect = null;
let _commandCenterStreamActive = false;
let _commandCenterStreamDisabled = false;
let _commandCenterLastClosedTradeCount = null;

function setCommandCenterUnavailable(message){
  const msg = message || 'Command center data unavailable';
  document.getElementById('signalsPanel').innerHTML = `<div class="empty">${msg}</div>`;
  document.getElementById('topOpsPanel').style.display = 'grid';
  document.getElementById('topOpsPanel').innerHTML = `<div class="empty" style="grid-column:1/-1;padding:14px 0">${msg}</div>`;
  document.getElementById('whalePanel').innerHTML = `<div class="empty">${msg}</div>`;
  document.getElementById('focusBadge').textContent = 'Unavailable';
  document.getElementById('focusPanel').innerHTML = `<div class="empty" style="grid-column:1/-1">${msg}</div>`;
  document.getElementById('eliteSummaryBadge').textContent = 'Unavailable';
  document.getElementById('eliteSummaryPanel').innerHTML = `<div class="empty" style="grid-column:1/-1">${msg}</div>`;
  document.getElementById('sentLabel').textContent = 'Unavailable';
  document.getElementById('pulseChips').innerHTML = '<span class="mini-chip bad">API timeout</span>';
  document.getElementById('whyNotBadge').textContent = 'Unavailable';
  document.getElementById('whyNotPanel').innerHTML = `<div class="empty" style="grid-column:1/-1">${msg}</div>`;
  document.getElementById('sessionRadarBadge').textContent = 'Unavailable';
  document.getElementById('sessionRadarPanel').innerHTML = `<div class="empty">${msg}</div>`;
  document.getElementById('watchlistBadge').textContent = 'Unavailable';
  document.getElementById('watchlistPanel').innerHTML = `<div class="empty">${msg}</div>`;
  document.getElementById('nearMissBadge').textContent = 'Unavailable';
  document.getElementById('nearMissPanel').innerHTML = `<div class="empty">${msg}</div>`;
  document.getElementById('lifecycleBadge').textContent = 'Unavailable';
  document.getElementById('lifecyclePanel').innerHTML = `<div class="empty" style="grid-column:1/-1">${msg}</div>`;
  document.getElementById('tradeTapeBadge').textContent = 'Unavailable';
  document.getElementById('tradeTapePanel').innerHTML = `<div class="empty">${msg}</div>`;
  const pnlStatsEl = document.getElementById('pnlStats');
  if(pnlStatsEl) pnlStatsEl.textContent = 'P&L history unavailable';
  if(_pnlChart){_pnlChart.destroy();_pnlChart = null;}
}
function renderWhalePanel(d){
  document.getElementById('whaleCount').textContent=d.alert_count_24h||0;
  const wEl=document.getElementById('whalePanel');
  const alerts=d.recent||[];
  if(!alerts.length){wEl.innerHTML='<div class="empty">No whale alerts</div>';return;}
  wEl.innerHTML=alerts.slice(0,4).map(a=>{
    const killReason=knownKillReason(a);
    return `<div class="whale-item"><div class="whale-icon">🐋</div><div class="whale-info"><div class="whale-title">${a.symbol||a.asset||'?'}</div><div class="whale-meta">${a.source||''} · ${confirmationLine(a)}</div><div class="whale-meta">${qualityBits(a).slice(0,4).join(' · ') || 'Execution quality pending'}</div>${regimeBits(a).length?`<div class="whale-meta">${regimeBits(a).join(' · ')}</div>`:''}${killReason?`<div class="note-callout bad">${killReason}</div>`:''}${reviewNotesLine(a)?`<div class="note-callout warn">${reviewNotesLine(a)}</div>`:''}</div><div class="whale-amount">$${((a.value_usd||0)/1e6).toFixed(1)}M</div></div>`;
  }).join('');
}
async function fetchCommandCenterOverview(){
  const now = Date.now();
  if(_commandCenterCache && now - _commandCenterCache.fetched < 1000)return _commandCenterCache.data;
  if(_commandCenterPending)return _commandCenterPending;
  _commandCenterPending = (async () => {
    try{
      const page = await fetchProtectedJson(`/api/command-center?live=1&_=${Date.now()}`, {timeoutMs: 15000});
      if(page && page.success){_commandCenterCache = {fetched: Date.now(), data: page};return page;}
      return {success:false};
    } finally {_commandCenterPending = null;}
  })();
  return _commandCenterPending;
}
function stopCommandCenterStream(){
  if(_commandCenterStreamReconnect){clearTimeout(_commandCenterStreamReconnect);_commandCenterStreamReconnect = null;}
  if(_commandCenterStreamAbort){_commandCenterStreamAbort.abort();_commandCenterStreamAbort = null;}
  _commandCenterStreamActive = false;
}
function scheduleCommandCenterStreamReconnect(delayMs = 3000){
  if(document.hidden || _commandCenterStreamDisabled)return;
  if(_commandCenterStreamReconnect)clearTimeout(_commandCenterStreamReconnect);
  _commandCenterStreamReconnect = setTimeout(() => {_commandCenterStreamReconnect = null;startCommandCenterStream();}, delayMs);
}
async function startCommandCenterStream(){
  if(_commandCenterStreamDisabled || _commandCenterStreamActive || _commandCenterStreamAbort)return;
  if(typeof window.fetch !== 'function'){_commandCenterStreamDisabled = true;return;}
  const controller = new AbortController();
  _commandCenterStreamAbort = controller;
  try{
    const response = await window.fetch(`/api/command-center/stream?_=${Date.now()}`, {method: 'GET',cache: 'no-store',headers: {'Accept': 'application/x-ndjson'},signal: controller.signal});
    if(!response.ok)throw new Error(`HTTP ${response.status}`);
    if(!response.body || typeof response.body.getReader !== 'function'){_commandCenterStreamDisabled = true;_commandCenterStreamActive = false;return;}
    _commandCenterStreamActive = true;
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while(true){
      const {value, done} = await reader.read();
      if(done)break;
      buffer += decoder.decode(value, {stream:true});
      let newlineIndex = buffer.indexOf('\n');
      while(newlineIndex >= 0){
        const rawLine = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);
        if(rawLine){
          try{
            const msg = JSON.parse(rawLine);
            if(msg && msg.type === 'refresh')loadMain();
          }catch(err){console.warn('[CC stream] parse error', err);}
        }
        newlineIndex = buffer.indexOf('\n');
      }
    }
  }catch(err){
    if(err && err.name === 'AbortError')return;
    console.warn('[CC stream]', err);
  }finally{
    if(_commandCenterStreamAbort === controller)_commandCenterStreamAbort = null;
    _commandCenterStreamActive = false;
    if(!document.hidden && !_commandCenterStreamDisabled)scheduleCommandCenterStreamReconnect();
  }
}
function collectElitePool(d){
  return [].concat(d.latest_signals||[], d.top_opportunities||[], d.near_misses||[], (d.watchlist_ladder?.hot)||[], (d.watchlist_ladder?.almost_ready)||[], d.positions||[]);
}
function renderEliteSummary(d){
  const pool=collectElitePool(d);
  const ready=pool.filter(item=>eliteEntryState(item).label==='Ready');
  const waiting=pool.filter(item=>eliteEntryState(item).label==='Waiting');
  const blocked=pool.filter(item=>eliteEntryState(item).label==='Blocked');
  const confPending=pool.filter(item=>itemBool(item,'entry_confirmation_ready')!==true && itemNum(item,'entry_confirmation_bars_required',0)>0);
  const retestReady=pool.filter(item=>itemBool(item,'breakout_retest_ready')===true);
  const pullbackReady=pool.filter(item=>itemBool(item,'first_pullback_ready')===true);
  const overextended=pool.filter(item=>itemNum(item,'extension_score',0)>=0.80);
  const lowQuality=pool.filter(item=>{
    const candle=itemNum(item,'candle_quality_score',0);
    const session=itemNum(item,'session_quality_score',0);
    const target=itemNum(item,'target_efficiency_score',0);
    return (candle>0 && candle<0.55) || (session>0 && session<0.55) || (target>0 && target<0.50);
  });
  const clusterHeavy=pool.filter(item=>itemNum(item,'cluster_penalty',0)>=0.70);
  const regimeAware=pool.filter(item=>regimeBits(item).length);
  const regimeBlocked=pool.filter(item=>{
    const reason=exactKillReason(item).toLowerCase();
    return /regime|policy/.test(reason) || /regime|policy/.test(String(itemText(item,'regime_policy_summary') || itemText(item,'regime_policy') || itemText(item,'regime_label') || '').toLowerCase());
  });
  document.getElementById('eliteSummaryBadge').textContent=`${ready.length} ready · ${blocked.length} blocked`;
  renderCommandBoardCards('eliteSummaryPanel',[
    {title:'Entry state',value:`${ready.length}/${waiting.length}/${blocked.length}`,valueClass:blocked.length?'am':ready.length?'gn':'mu',note:'Ready / Waiting / Blocked across live queue, near misses, watchlist, and book.'},
    {title:'Confirmation queue',value:confPending.length?`${confPending.length} waiting`:'Clear',valueClass:confPending.length?'am':'gn',note:confPending.length?'Signals are staged but still need confirmation bars or reclaim proof.':'No visible confirmation backlog right now.'},
    {title:'Path mix',value:`R ${retestReady.length} · P ${pullbackReady.length}`,valueClass:(retestReady.length||pullbackReady.length)?'cy':'mu',note:'Breakout retest and first-pullback readiness currently visible across the desk.'},
    {title:'Extension pressure',value:overextended.length?`${overextended.length} hot`:'Contained',valueClass:overextended.length?'rd':'gn',note:overextended.length?'Overextension is actively vetoing or degrading entries.':'Extension pressure is contained.'},
    {title:'Quality drag',value:lowQuality.length?`${lowQuality.length} weak`:'Stable',valueClass:lowQuality.length?'am':'gn',note:'Weak candle, session, or target efficiency is degrading entry quality.'},
    {title:'Cluster drag',value:clusterHeavy.length?`${clusterHeavy.length} blocked`:'Light',valueClass:clusterHeavy.length?'rd':'gn',note:'Cluster penalty is suppressing overlapping ideas when too many similar setups stack up.'},
    {title:'Regime policy',value:regimeBlocked.length?`${regimeBlocked.length} blocked`:regimeAware.length?`${regimeAware.length} tagged`:'Visible',valueClass:regimeBlocked.length?'rd':regimeAware.length?'cy':'mu',note:regimeAware.length?'Regime policy summaries now travel with the live decision stack.':'No regime policy note is visible in the current pool.'},
  ],'Elite entry summary unavailable');
}
async function loadMain(){
  let d;
  try{
    const page = await fetchCommandCenterOverview();
    if(page.success){d = page || {};} else {setCommandCenterUnavailable('Command center timed out');return;}
    if(!d||!d.success){setCommandCenterUnavailable('Command center data unavailable');return;}
  }catch(e){console.error('[CC] fetch:',e);return;}

  const providerRouting=d.provider_routing||{};
  const signalQuality=d.signal_quality||{};
  const signalDiagnostics=d.signal_diagnostics||{};

  try{
    const live=d.live_summary||{};
    const closedTradeCount=Number(live.closed_trades??d.total_trades??0);
    if(_commandCenterLastClosedTradeCount===null){_commandCenterLastClosedTradeCount=closedTradeCount;}
    else if(closedTradeCount!==_commandCenterLastClosedTradeCount){_commandCenterLastClosedTradeCount=closedTradeCount;if(!document.hidden)loadHistory();}
    const realizedBalance=Number(d.balance||0);
    const liveBalance=Number(live.balance??d.balance??0);
    const balanceDelta=Number(live.balance_delta??(liveBalance-Number(live.initial_balance??d.initial_balance??realizedBalance)));
    const balanceEl=document.getElementById('mBal');
    balanceEl.textContent=fmtMoney(liveBalance);
    balanceEl.className='mc-value '+(balanceDelta>=0?'gn':'rd');
    document.getElementById('topBalance').textContent=fmtMoney(liveBalance);
    document.getElementById('topBalance').style.color=balanceDelta>=0?'var(--gr)':'var(--rd)';
    document.getElementById('mBalSub').textContent=`Realized ${fmtMoney(realizedBalance)} · Open ${fmtMoney(live.open_pnl||0)}`;
    const dp=Number(live.daily_pnl??d.daily_pnl??0);
    const dpEl=document.getElementById('mDPnl');
    dpEl.textContent=fmtNum(dp);dpEl.className='mc-value '+(dp>=0?'gn':'rd');
    document.getElementById('mDTrades').textContent=`Realized ${fmtMoney(d.daily_pnl||0)} · Floating ${fmtMoney(live.open_pnl||0)}`;
    const liveWr=Number(live.win_rate??d.win_rate??0);
    const wrEl=document.getElementById('mWr');
    wrEl.textContent=liveWr.toFixed(1)+'%';
    wrEl.className='mc-value '+(liveWr>=60?'gn':liveWr<=40?'rd':'am');
    document.getElementById('mWrSub').textContent=`${Number(live.closed_trades??d.total_trades??0)} closed · ${Number(live.open_positions??0)} open`;
    const posLen=(d.positions||[]).length;
    document.getElementById('mPos').textContent=posLen;
    document.getElementById('mPosSub').textContent=`BUY ${Number(live.buy_count||0)} · SELL ${Number(live.sell_count||0)}`;
    document.getElementById('posCount').textContent=posLen;
    const running=d.engine_running;
    const ready=!!d.engine_ready;
    document.getElementById('engineDot').className='pulse'+(running?'':' off');
    document.getElementById('engineStatus').textContent=running?'Engine running':'Engine stopped';
    document.getElementById('readyDot').className='pulse'+(ready?'':' off');
    document.getElementById('readyStatus').textContent=ready?'Decision stack ready':'Decision stack warming';
    const providerLabel=providerRouting.summary_label||'Unavailable';
    document.getElementById('providerStatus').textContent=providerLabel;
    document.getElementById('engineBadge').textContent=running?'Live':'Offline';
    document.getElementById('engineBadge').className='tb-badge '+(running?'badge-live':'badge-off');
    const topOpportunities = d.top_opportunities || [];
    const lead = topOpportunities[0] || null;
    const positions = d.positions || [];
    const categoryCounts = positions.reduce((acc, item) => {const key = String(item.category || 'unknown');acc[key] = (acc[key] || 0) + 1;return acc;}, {});
    const leadCategory = Object.entries(categoryCounts).sort((a,b) => b[1] - a[1])[0];
    const supportiveCount = Number(signalDiagnostics.broker_supportive_count || 0);
    const fragileCount = Number(signalDiagnostics.broker_fragile_count || 0);
    const trueDepthCount = Number(signalDiagnostics.true_depth_count || 0);
    const syntheticDepthCount = Number(signalDiagnostics.synthetic_depth_count || 0);
    const focusState = lead ? 'Armed' : posLen ? 'Managing' : 'Quiet';
    document.getElementById('focusBadge').textContent = focusState;
    document.getElementById('focusPanel').innerHTML = [
      {value: lead ? `${lead.asset || '—'} ${String(lead.direction || lead.signal || '').toUpperCase()}` : 'No queue',valueClass: lead ? (String(lead.direction || lead.signal || '').toUpperCase() === 'SELL' ? 'rd' : 'gn') : 'mu',label: 'Next Candidate',sub: lead ? `Opp ${Number(lead.opportunity_score || 0).toFixed(3)} · ${buildEliteReason(lead)}` : 'No setup is currently strong enough to lead the desk'},
      {value: providerRouting.summary_label || 'Unavailable',valueClass: providerRouting.summary_label ? 'cy' : 'mu',label: 'Feed Spine',sub: providerRouting.fallback_label ? `Fallback ${providerRouting.fallback_label}` : 'No published fallback split'},
      {value: posLen ? `${posLen} open` : 'Flat',valueClass: posLen ? 'pu' : 'mu',label: 'Book State',sub: live.book_bias ? `${live.book_bias} · ${live.open_state||'Flat'}` : (leadCategory ? `${leadCategory[0]} leads with ${leadCategory[1]} positions` : 'No open exposure in the book')},
      {value: `${supportiveCount}/${fragileCount}`,valueClass: fragileCount ? 'am' : supportiveCount ? 'gn' : 'mu',label: 'Broker Pressure',sub: signalDiagnostics.count ? `${trueDepthCount} true depth · ${syntheticDepthCount} synthetic depth` : 'No live pressure diagnostics yet'},
    ].map(item => `<div class="pulse-card"><div class="pulse-val ${item.valueClass}">${item.value}</div><div class="pulse-lbl">${item.label}</div><div class="pulse-sub">${item.sub}</div></div>`).join('');
    renderEliteSummary(d);
  }catch(e){console.error('[CC] metrics:',e);}

  try{
    const sigs=(d.latest_signals||[]).filter(s=>(s.signal||s.direction||'HOLD')!=='HOLD');
    renderTopOpportunities(d.top_opportunities||[]);
    document.getElementById('sigCount').textContent=sigs.length;
    const sEl=document.getElementById('signalsPanel');
    if(!sigs.length){sEl.innerHTML='<div class="empty">No active signals</div>';}
    else{
      sEl.innerHTML=sigs.slice(0,6).map(s=>{
        const dir=(s.signal||s.direction||'HOLD').toUpperCase();
        const c=Number(s.confidence||0);
        const cp=(c*100).toFixed(0);
        const barCol=c>=0.75?'var(--gr)':c>=0.62?'var(--am)':'var(--rd)';
        const mem=Number(s.memory_score||0);
        const execQ=Number(s.execution_quality_score||0);
        const opp=Number(s.opportunity_score||0);
        const brokerQ=Number(s.broker_quality_score||0);
        const microQ=Number(s.microstructure_score||0);
        const entryState=eliteEntryState(s);
        const detail=`Mem ${mem?mem.toFixed(0):'—'} · Exec ${execQ?execQ.toFixed(0):'—'} · Opp ${opp?opp.toFixed(3):'—'}`;
        const brokerState=humanToken(s.broker_agreement_state||s.broker_context||'');
        const provenance=provenanceLine(s);
        const crossPeer=s.cross_asset_primary_peer||'';
        const crossRelation=humanToken(s.cross_asset_primary_relation||'');
        const crossAlign=Number(s.cross_asset_alignment||0);
        const crossLine=crossPeer?`${crossAlign>=0?'Cross support':'Cross conflict'} via ${crossPeer}${crossRelation?` · ${crossRelation}`:''}`:'';
        const patternLine=recentPatternLine(s);
        const pressure=buildQualityPressure(s);
        return`<div class="sig-row"><span class="sig-dir ${dir==='BUY'?'sig-buy':'sig-sell'}">${dir}</span><div style="flex:1"><div class="sig-asset">${s.asset||'?'} ${statusPill(s,entryState.label)}</div><div class="sig-meta">${s.category||''} · ${detail}</div>${eliteChipRow(s)}${provenance?`<div class="sig-meta">${provenance}</div>`:''}<div class="sig-meta">Broker ${brokerQ.toFixed(2)}${brokerState?` · ${brokerState}`:''} · Micro ${microQ.toFixed(2)} · ${depthLabel(s)}</div>${crossLine?`<div class="sig-meta">${crossLine}</div>`:''}<div class="sig-meta">${readinessBits(s).join(' · ')}</div><div class="sig-meta">${qualityBits(s).join(' · ') || 'Elite entry metrics pending'}</div>${pressure?`<div class="note-callout warn">${pressure}</div>`:''}${(s.exact_kill_reason||s.execution_kill_reason)?`<div class="note-callout bad">${exactKillReason(s)}</div>`:''}${patternLine?`<div class="sig-meta">${patternLine}</div>`:''}${reviewNotesLine(s)?`<div class="note-callout ${entryState.label==='Ready'?'ok':entryState.label==='Blocked'?'bad':'warn'}">${reviewNotesLine(s)}</div>`:''}</div><div style="text-align:right"><div class="sig-conf ${entryState.className}" style="color:${barCol}">${cp}%</div><div class="conf-bar-mini"><div class="cbm-fill" style="width:${cp}%;background:${barCol}"></div></div><div class="sig-meta" style="margin-top:6px">${confirmationProgress(s).toFixed(0)}% conf</div></div></div>`;
      }).join('');
    }
  }catch(e){console.error('[CC] signals:',e);}

  try{
    const why=d.why_not_traded||{};
    const leadBlocker=titleToken(why.lead_blocker,'No blocker');
    const policyBlockers=(why.top_blockers||[]).filter(item=>/regime|policy|cluster/i.test(String(item.label||''))).reduce((sum,item)=>sum+Number(item.count||0),0);
    document.getElementById('whyNotBadge').textContent=why.lead_count?`${leadBlocker} · ${why.lead_count}`:'Clear';
    renderCommandBoardCards('whyNotPanel',[
      {title:'Lead blocker',value:leadBlocker,valueClass:why.lead_count?'am':'gn',note:why.lead_count?`${why.lead_count} recent rejects were dominated by this blocker.`:'No repeated blocker is dominating the journal right now.'},
      {title:'Confirmation drag',value:String((why.confirmation_pending_count||why.blocked_by_confirmation_count||0)),valueClass:Number(why.confirmation_pending_count||why.blocked_by_confirmation_count||0)?'am':'gn',note:'Candidates staged by structure but still waiting on confirmation bars or reclaim proof.'},
      {title:'Extension / cluster',value:`${Number(why.overextended_count||0)} / ${Number(why.cluster_blocked_count||0)}`,valueClass:(Number(why.overextended_count||0)||Number(why.cluster_blocked_count||0))?'rd':'gn',note:'Overextended setups and cluster-suppressed ideas removed before execution.'},
      {title:'Policy lock',value:String(policyBlockers||0),valueClass:policyBlockers?'rd':'gn',note:policyBlockers?'Regime and cluster policy are visible in the recent reject mix.':'No regime-policy lock is dominating recent rejects.'},
      ...((why.top_assets||[]).slice(0,3).map(item=>({title:item.asset||'—',value:exactKillReason(item),valueClass:'cy',note:[`${item.count||0} rejects`, confirmationLine(item), qualityBits(item).slice(0,3).join(' · '), regimeBits(item).join(' · '), reviewNotesLine(item)].filter(Boolean).join(' · ')}))),
    ].slice(0,6),'No recurring blockers in the recent journal');

    const radar=d.session_radar||{};
    document.getElementById('sessionRadarBadge').textContent=radar.open_count!=null?`${radar.open_count} open / ${radar.blocked_count||0} blocked`:'Unavailable';
    renderDeskRows('sessionRadarPanel',(radar.rows||[]).slice(0,8).map(item=>{
      const allowedSessions=((item.allowed_sessions||[]).map(v=>titleToken(v))).join(', ') || 'n/a';
      const sessionContext=item.session_open
        ? [buildEliteReason(item), regimeBits(item).join(' · '), reviewNotesLine(item)].filter(Boolean).join(' · ')
        : `Allowed ${allowedSessions}`;
      const meta=item.session_open
        ? `${titleToken(item.category)} open now · ${item.preferred_interval||'—'} · ${item.primary_provider||'—'}${sessionContext?` · ${sessionContext}`:''}`
        : `${sessionContext} · ${item.primary_provider||'—'} · Session gated`;
      return {head:`${item.asset} · ${titleToken(item.current_session,'Off')}`,meta,side:item.session_open?'OPEN':'BLOCKED',sideClass:item.session_open?'gn':'am'};
    }),'Session radar unavailable');

    const ladder=d.watchlist_ladder||{};
    const sections=[
      {label:'Hot',items:ladder.hot||[],cls:'gn',text:item=>`${item.asset||'—'} ${item.direction||''}`,note:item=>[`Opp ${(Number(item.opportunity_score||0)).toFixed(3)} · Conf ${(Number(item.confidence||0)*100).toFixed(0)}%`, buildEliteReason(item), qualityBits(item).slice(0,3).join(' · '), regimeBits(item).join(' · '), reviewNotesLine(item)].filter(Boolean).join(' · ')},
      {label:'Almost Ready',items:ladder.almost_ready||[],cls:'am',text:item=>`${item.asset||'—'} ${item.direction||''}`,note:item=>[`${titleToken(item.reason,'Near miss')} · setup ${(Number(item.setup_quality||0)).toFixed(3)}`, buildEliteReason(item), itemBool(item,'breakout_retest_ready')===false?'Retest missing':'', itemBool(item,'first_pullback_ready')===false?'Pullback missing':'', qualityBits(item).slice(0,3).join(' · '), regimeBits(item).join(' · '), reviewNotesLine(item)].filter(Boolean).join(' · ')},
      {label:'Blocked',items:ladder.blocked||[],cls:'rd',text:item=>`${item.asset||'—'} · ${titleToken(item.current_session,'Off')}`,note:item=>[knownKillReason(item) || (!item.session_open ? 'Session gated' : 'Held out of the active queue'), !item.session_open?`Allowed ${((item.allowed_sessions||[]).map(v=>titleToken(v))).join(', ') || 'n/a'}`:'', qualityBits(item).slice(0,3).join(' · '), regimeBits(item).join(' · '), reviewNotesLine(item)].filter(Boolean).join(' · ')},
      {label:'Inactive',items:ladder.inactive||[],cls:'mu',text:item=>`${item.asset||'—'} · ${titleToken(item.category,'Unknown')}`,note:item=>{const elite=eliteRankText(itemValue(item,'elite_pattern_rank'));return [`${item.preferred_interval||'—'} playbook interval`, itemText(item,'pattern_family')?`Pattern ${titleToken(itemText(item,'pattern_family'))}`:'', elite!=='—'?`Elite ${elite}`:''].filter(Boolean).join(' · ');}},
    ];
    document.getElementById('watchlistBadge').textContent=sections.reduce((sum,section)=>sum+(section.items?.length||0),0)+' staged';
    document.getElementById('watchlistPanel').innerHTML='<div class="board-grid">'+sections.map(section=>`<div class="board-card"><div class="board-title">${section.label}</div>${section.items && section.items.length ? section.items.slice(0,3).map(item=>`<div class="desk-meta" style="margin-top:6px"><span class="${section.cls}" style="font-weight:700">${section.text(item)}</span><br>${section.note(item)}</div>`).join('') : '<div class="board-note">No assets in this ladder state.</div>'}</div>`).join('')+'</div>';

    const nearMisses=d.near_misses||[];
    document.getElementById('nearMissBadge').textContent=nearMisses.length?`${nearMisses.length} tracked`:'Clear';
    renderDeskRows('nearMissPanel',nearMisses.slice(0,6).map(item=>({head:`${item.asset||'—'} ${String(item.direction||'').toUpperCase() || ''}`.trim(),meta:[`${knownKillReason(item) || buildEliteReason(item)} · Opp ${(Number(item.opportunity_score||0)).toFixed(3)} · Setup ${(Number(item.setup_quality||0)).toFixed(3)}`, readinessBits(item).join(' · '), qualityBits(item).slice(0,4).join(' · '), regimeBits(item).join(' · '), reviewNotesLine(item)].filter(Boolean).join(' · '),side:item.session_label?titleToken(item.session_label):eliteEntryState(item).label.toUpperCase(),sideClass:eliteEntryState(item).className})),'No near-miss setups right now');

    const lifecycle=d.trade_lifecycle||{};
    const lifecyclePool=collectElitePool(d);
    const funnelSeeded=Number(lifecycle.seeded||0);
    const funnelStructure=Number(lifecycle.structure_valid||lifecyclePool.filter(item=>itemBool(item,'breakout_retest_ready')===true || itemBool(item,'first_pullback_ready')===true || itemBool(item,'entry_confirmation_ready')===true).length||0);
    const funnelConfirm=Number(lifecycle.confirmation_ready||lifecyclePool.filter(item=>itemBool(item,'entry_confirmation_ready')===true).length||0);
    const funnelApproved=Number(lifecycle.approved||0);
    const funnelExecuted=Number(lifecycle.executed||lifecycle.opened||Number((d.positions||[]).length||0));
    document.getElementById('lifecycleBadge').textContent=`${funnelSeeded} seed → ${funnelExecuted} exec`;
    renderCommandBoardCards('lifecyclePanel',[
      {title:'Seeded',value:String(funnelSeeded),valueClass:'cy',note:'Raw journal signals that reached the seed stage.'},
      {title:'Structure Valid',value:String(funnelStructure),valueClass:funnelStructure?'bl':'mu',note:'Retest or pullback structure still intact before the confirmation gate.'},
      {title:'Confirmation Ready',value:String(funnelConfirm),valueClass:funnelConfirm?'am':'mu',note:'Signals that satisfied elite confirmation bars / reclaim checks.'},
      {title:'Approved',value:String(funnelApproved),valueClass:'gn',note:'Signals that survived the review stack.'},
      {title:'Executed',value:String(funnelExecuted),valueClass:funnelExecuted?'pu':'mu',note:'Signals that made it from review into a live position.'},
      {title:'Closed',value:String(lifecycle.closed||0),valueClass:(lifecycle.closed||0)?'rd':'mu',note:'Fully closed parent trades in the recent window.'},
    ],'No lifecycle data');

    renderTradeTape(d.trade_tape||[]);
    renderPnlChart(d.pnl_curve||[], d.pnl_curve_stats||{});
    renderWhalePanel(d);
  }catch(e){console.error('[CC] desk panels:',e);}

  try{
    const ss=Number(d.sentiment_score||0);
    const ssLabel=ss>0.2?'Bullish':ss<-0.2?'Bearish':'Neutral';
    const ssColor=ss>0.2?'var(--gr)':ss<-0.2?'var(--rd)':'var(--am)';
    document.getElementById('sentScore').textContent=(ss>=0?'+':'')+ss.toFixed(3);
    document.getElementById('sentScore').style.color=ssColor;
    document.getElementById('sentLabel').textContent=ssLabel;
    document.getElementById('sentBar').style.width=((ss+1)/2*100)+'%';
    document.getElementById('sentBar').style.background=ssColor;
    const pool=collectElitePool(d);
    const readyCount=pool.filter(item=>eliteEntryState(item).label==='Ready').length;
    const waitingCount=pool.filter(item=>eliteEntryState(item).label==='Waiting').length;
    const blockedCount=pool.filter(item=>eliteEntryState(item).label==='Blocked').length;
    const chips = [
      {label:`Whales ${d.alert_count_24h||0}`, cls:(d.alert_count_24h||0)?'warn':'ok'},
      {label:d.engine_ready?'Ready':'Warming', cls:d.engine_ready?'ok':'warn'},
      {label:`Elite ${readyCount}/${waitingCount}/${blockedCount}`, cls:blockedCount?'warn':readyCount?'ok':'info'},
      {label:`Feeds ${providerRouting.summary_label||'—'}`, cls:(providerRouting.primary_counts||{}).IG?'ok':''},
      {label:`Memory ${Number(signalQuality.avg_memory_score||0).toFixed(0)}`, cls:Number(signalQuality.avg_memory_score||0)>=60?'ok':'warn'},
      {label:`Exec ${Number(signalQuality.avg_execution_quality||0).toFixed(0)}`, cls:Number(signalQuality.avg_execution_quality||0)>=60?'ok':'warn'},
      {label:`Opp ${Number(signalQuality.avg_opportunity_score||0).toFixed(3)}`, cls:Number(signalQuality.avg_opportunity_score||0)>=0.7?'ok':''},
      {label:`Diag ${signalDiagnostics.summary_label||'—'}`, cls:Number(signalDiagnostics.broker_fragile_count||0)?'warn':'ok'},
      {label:signalQuality.top_signal_asset?`Top ${signalQuality.top_signal_asset}`:`Positions ${d.open_positions||0}`, cls:(d.open_positions||0)?'ok':''},
    ];
    document.getElementById('pulseChips').innerHTML = chips.map(c=>`<span class="mini-chip ${c.cls||''}">${c.label}</span>`).join('');
  }catch(e){console.error('[CC] sentiment:',e);}

  try{
    const pos=d.positions||[];
    document.getElementById('posCount').textContent=pos.length||0;
    _allPositions = pos;
    const pf=document.getElementById('posFilter');
    if(pf && pf.value && pf.value !== 'all')filterPositions();
    else renderPositions(pos);
  }catch(e){console.error('[CC] positions:',e);}
}

async function loadWhales(){
  try{
    const page = await fetchCommandCenterOverview();
    if(!page.success){document.getElementById('whalePanel').innerHTML='<div class="empty">Whale data unavailable</div>';return;}
    const d = page || {};
    if(!d||!d.success){document.getElementById('whalePanel').innerHTML='<div class="empty">Whale data unavailable</div>';return;}
    renderWhalePanel(d);
  }catch(e){}
}
function apiHeaders(){return {'Content-Type': 'application/json'};}
async function fetchProtectedJson(url, options = {}){
  return window.dashboardFetchJson(url, {timeoutMs: options.timeoutMs || 12000,init: {method: options.method || 'GET',headers: Object.assign({}, apiHeaders(), options.headers || {}),body: options.body,cache: options.cache || 'no-store'}});
}
async function bootCommandCenter(){
  try{if(window.dashboardAuthReady)await window.dashboardAuthReady;}catch(_){}
  await Promise.allSettled([loadMain(),loadHistory()]);
  startCommandCenterStream();
}
bootCommandCenter();
setInterval(() => { if(!document.hidden && !_commandCenterStreamActive){ loadMain(); } }, 5000);
setInterval(loadHistory,60000);
document.addEventListener('visibilitychange', () => {if(!document.hidden){loadMain();if(!_commandCenterStreamActive && !_commandCenterStreamDisabled)startCommandCenterStream();}});
window.addEventListener('pagehide', stopCommandCenterStream);
window.addEventListener('beforeunload', stopCommandCenterStream);

async function loadHistory(){
  try{
    const filter=document.getElementById('historyFilter')?.value||'all';
    const d=await fetchProtectedJson(`/api/trade-history?limit=50&no_cache=1&_=${Date.now()}`, {timeoutMs: 12000});
    if(!d.success){document.getElementById('historyBody').innerHTML='<tr><td colspan="11" class="empty">Trade history unavailable</td></tr>';return;}
    let trades=d.trades||[];
    trades=trades.filter(t=>!t.is_partial_close);
    if(filter==='forex')trades=trades.filter(t=>t.category==='forex');
    else if(filter==='crypto')trades=trades.filter(t=>t.category==='crypto');
    else if(filter==='commodities')trades=trades.filter(t=>t.category==='commodities');
    else if(filter==='indices')trades=trades.filter(t=>t.category==='indices');
    else if(filter==='won')trades=trades.filter(t=>(t.pnl||0)>0);
    else if(filter==='lost')trades=trades.filter(t=>(t.pnl||0)<0);
    document.getElementById('historyCount').textContent=trades.length;
    const tbody=document.getElementById('historyBody');
    if(!trades.length){tbody.innerHTML='<tr><td colspan="11" class="empty">No closed trades</td></tr>';return;}
    const reasonLabel=r=>{if(!r)return'—';if(r.includes('Partial TP') && r.includes('->'))return r;if(r.includes('Take Profit'))return'Take Profit';if(r.includes('Stop Loss'))return r.includes('offline') ? 'Stop Loss (Offline)' : 'Stop Loss';if(r.includes('Trail'))return'Trailing Exit';if(r.includes('Manual'))return'Manual Close';if(r.includes('Break'))return'Break-Even Exit';return r;};
    const sm=p=>p>0&&p<10;
    tbody.innerHTML=trades.map(t=>{
      const pnl=Number(t.pnl||0);
      const pnlCol=pnl>=0?'var(--gr)':'var(--rd)';
      const dir=((t.direction||t.signal||'BUY')+'').toUpperCase();
      const lot=Number(t.lot_size||0);
      const ep=Number(t.entry_price||0);
      const xp=Number(t.exit_price||0);
      const execQ=Number(t.execution_quality_score||0);
      const rrRealized=Number(t.rr_realized||0);
      const provenance=provenanceLine(t);
      const fmt=n=>n>0?(sm(n)?n.toFixed(5):n.toFixed(2)):'—';
      const fmtLot=n=>{if(!(n>0)) return '—';if(n>=1) return n.toFixed(2);return n.toFixed(4).replace(/0+$/,'').replace(/\.$/,'');};
      const dur=t.duration_str||'—';
      const catEmoji={forex:'💱',crypto:'₿',commodities:'🥇',indices:'📈'}[t.category]||'📊';
      const execTags=[];
      if(execQ>0) execTags.push(`Exec ${execQ.toFixed(0)}`);
      if(rrRealized!==0) execTags.push(`R ${rrRealized>0?'+':''}${rrRealized.toFixed(2)}`);
      if(t.premature_stop) execTags.push('Premature stop');
      if(t.target_miss) execTags.push('Target missed');
      if(t.late_entry) execTags.push('Late entry');
      const eliteBits=qualityBits(t).slice(0,4);
      const exitLabel=reasonLabel(String(t.display_exit_reason||t.exit_reason||''));
      const killReason=knownKillReason(t);
      const readinessMeta=`<div style="font-size:10px;color:var(--tx3);margin-top:2px">${readinessBits(t).slice(0,3).join(' · ')}</div>`;
      return`<tr style="opacity:0.9"><td><strong>${t.asset||'?'}</strong><span style="font-size:10px;color:var(--tx3);margin-left:4px">${catEmoji}</span></td><td style="color:${dir==='BUY'?'var(--gr)':'var(--rd)'};font-weight:700">${dir}</td><td style="font-family:monospace;font-size:11px">${fmtLot(lot)}</td><td style="font-family:monospace;font-size:11px">${fmt(ep)}</td><td style="font-family:monospace;font-size:11px">${fmt(xp)}</td><td style="font-size:11px;color:var(--tx3)">${fmtDashboardTime(t.entry_time)}</td><td style="font-size:11px;color:var(--tx3)">${fmtDashboardTime(t.exit_time)}</td><td style="font-size:11px;color:var(--am)">${dur}</td><td style="font-weight:700;color:${pnlCol}">${pnl>=0?'+':''}$${pnl.toFixed(2)}</td><td style="font-size:10px;color:var(--tx3)"><div style="font-size:10px;color:var(--tx2);font-weight:700">${exitLabel}</div>${killReason?`<div style="font-size:10px;color:var(--rd);margin-top:2px">${killReason}</div>`:''}${t.continuation_summary?`<div style="font-size:10px;color:var(--am);margin-top:2px">${t.continuation_summary}</div>`:''}</td><td style="font-size:11px">${t.confidence?Math.round(t.confidence*100)+'%':'—'}</td></tr>`;
    }).join('');
  }catch(e){console.error('[History]',e);}
}
async function clearTradeHistory(){
  if(!confirm('Clear all closed trade history? This cannot be undone.')) return;
  try{
    const json = await fetchProtectedJson('/api/trade-history/clear', {method: 'POST',timeoutMs: 12000});
    if(!json.success){alert('Failed to clear trade history: '+(json.error||'unknown'));return;}
    loadHistory();loadMain();alert('Trade history cleared');
  }catch(e){console.error('[History] clear error', e);alert('Failed to clear trade history');}
}
let _pnlChart = null;
function renderPnlChart(curve, stats = {}){
  const ctx=document.getElementById('pnlChart');
  const statsEl=document.getElementById('pnlStats');
  if(!ctx||!statsEl)return;
  const points=Array.isArray(curve)?curve:[];
  if(!points.length){statsEl.textContent='P&L history unavailable';if(_pnlChart){_pnlChart.destroy();_pnlChart=null;}return;}
  const labels=points.map(p=>p.label||'');
  const data=points.map(p=>Number(p.cumulative_pnl??p.pnl??0));
  const current=Number(data[data.length-1]||0);
  let peak=Number(stats.peak ?? data[0] ?? 0);
  let maxDrawdown=Number(stats.drawdown ?? 0);
  if(!stats.peak && !stats.drawdown){
    let runningPeak = data[0] ?? 0;
    let worstDrawdown = 0;
    data.forEach(v => {runningPeak = Math.max(runningPeak, v);worstDrawdown = Math.min(worstDrawdown, v - runningPeak);});
    peak = runningPeak;maxDrawdown = Math.abs(worstDrawdown);
  }
  statsEl.textContent = `Live ${fmtMoney(current)} | Peak ${fmtMoney(peak)} | Drawdown ${fmtMoney(maxDrawdown)} | ${stats.interval_minutes||30}m intervals`;
  statsEl.style.color=current>=0?'var(--gr)':'var(--rd)';
  const borderColor=current>=0?'#00d084':'#ff5964';
  const fillColor=current>=0?'rgba(0, 208, 132, 0.12)':'rgba(255, 69, 96, 0.12)';
  if(_pnlChart)_pnlChart.destroy();
  _pnlChart = new Chart(ctx, {type: 'line',data: {labels,datasets: [{label: 'Cumulative P&L',data,borderColor,backgroundColor: fillColor,fill: true,tension: 0.3,pointRadius: 2,pointBackgroundColor: borderColor,pointBorderColor: 'transparent',pointHoverRadius: 4}]},options: {responsive: true,maintainAspectRatio: false,plugins: {legend: {display: false},tooltip: {backgroundColor: 'rgba(13, 17, 23, 0.9)',borderColor: '#1e2635',borderWidth: 1,titleColor: '#c9d1e0',bodyColor: '#c9d1e0',padding: 10,callbacks: {title(items){const item=items&&items[0];return item ? `${item.label || ''}` : '';},label(context){return `P&L: ${fmtMoney(Number(context.raw||0))}`;}}}},scales: {y: {beginAtZero: false,grid: {color: 'rgba(30, 38, 53, 0.3)'},ticks: {color: '#7a8899',font: {size: 10},callback(value){return '$' + Number(value).toFixed(0);}},title: {display: false}},x: {grid: {color: 'transparent'},ticks: {color: '#7a8899', font: {size: 10}, maxTicksLimit: 8, autoSkip: true}}}}});
}
let _allPositions = [];
function renderPositions(pos){
  const tbody=document.getElementById('posBody');
  if(!tbody)return;
  if(!pos.length){tbody.innerHTML='<tr><td colspan="11" class="empty">No open positions</td></tr>';return;}
  tbody.innerHTML=pos.map(p=>{
    const dir=(p.direction||p.signal||'BUY').toUpperCase();
    const pnl=Number(p.pnl||0);
    const lot=Number(p.lot_size||0);
    const e=Number(p.entry_price||0);
    const sl=Number(p.stop_loss||0);
    const tp=Number(p.take_profit||0);
    const tpLevels=Array.isArray(p.take_profit_levels)?p.take_profit_levels.map(v=>Number(v||0)).filter(v=>v>0):[];
    const tpHit=Math.max(0, Number(p.tp_hit||0));
    const nextTp=tpLevels.length?Number(tpLevels[Math.min(tpHit, tpLevels.length-1)]||0):tp;
    const runnerTp=tpLevels.length>1 && tpHit < tpLevels.length-1 ? Number(tpLevels[tpLevels.length-1]||0) : 0;
    const cur=Number(p.current_price||0);
    const execQ=Number(p.execution_quality_score||0);
    const execSamples=Number(p.execution_feedback_sample_count||0);
    const rrMult=Number(p.target_rr_multiplier||1);
    const stopMult=Number(p.stop_buffer_multiplier||1);
    const execNotes=Array.isArray(p.execution_notes)?p.execution_notes:[];
    const memoryScore=Number(p.memory_score||0);
    const memorySamples=Number(p.memory_sample_count||0);
    const oppScore=Number(p.opportunity_score||0);
    const oppRank=Number(p.opportunity_rank||0);
    const brokerQ=Number(p.broker_quality_score||0);
    const microQ=Number(p.microstructure_score||0);
    const brokerState=humanToken(p.broker_agreement_state||p.broker_context||'');
    const provenance=provenanceLine(p);
    const crossPeer=p.cross_asset_primary_peer||'';
    const crossAlign=Number(p.cross_asset_alignment||0);
    const patternLine=recentPatternLine(p);
    const regimeMeta=regimeBits(p).length?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">${regimeBits(p).join(' · ')}</div>`:'';
    const sm=e>0&&e<10;
    const fmt=n=>n>0?(sm?n.toFixed(5):n.toFixed(2)):'—';
    const fmtLot=n=>{if(!(n>0)) return '—';if(n>=1) return n.toFixed(2);return n.toFixed(4).replace(/0+$/,'').replace(/\.$/,'');};
    const curCol=cur>0?(dir==='BUY'?(cur>=e?'var(--gr)':'var(--rd)'):(cur<=e?'var(--gr)':'var(--rd)')):'var(--tx2)';
    const tid=p.trade_id||'';
    let openStr='—';
    try{
      const raw=p.open_time||'';
      if(raw){
        const ot=parseDashboardTime(raw);
        if(ot){
          const mins=Math.floor((Date.now()-ot)/60000);
          const absMins=Math.abs(mins);
          const dur=absMins<60?absMins+'m':absMins<1440?Math.floor(absMins/60)+'h '+absMins%60+'m':Math.floor(absMins/1440)+'d';
          const timeStr=fmtDashboardTime(raw);
          openStr=timeStr+' <span style="color:var(--tx3);font-size:10px">('+dur+')</span>';
        }
      }
    }catch(ex){}
    let progress=0;
    if(nextTp&&e&&sl){
      const tpDist=Math.abs(nextTp-e);
      const curDist=dir==='BUY'?cur-e:e-cur;
      progress=tpDist>0?Math.max(0,Math.min(100,(curDist/tpDist*100))):0;
    }
    const pBar=`<div style="height:3px;background:var(--bd);border-radius:2px;margin-top:2px"><div style="height:3px;width:${progress.toFixed(0)}%;background:${progress>80?'var(--gr)':progress>50?'var(--am)':'var(--bl)'};border-radius:2px;transition:width .5s"></div></div>`;
    const execMeta=execSamples?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">Exec ${execQ.toFixed(0)} · n${execSamples}</div>`:'';
    const eliteMeta=qualityBits(p).length?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">${qualityBits(p).slice(0,4).join(' · ')}</div>`:'';
    const readyMeta=`<div style="font-size:10px;color:var(--tx3);margin-top:2px">${readinessBits(p).slice(0,3).join(' · ')}</div>`;
    const memoryMeta=memorySamples?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">Memory ${memoryScore.toFixed(0)} · n${memorySamples}</div>`:'';
    const provenanceMeta=provenance?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">${provenance}</div>`:'';
    const brokerMeta=(brokerQ||brokerState)?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">Broker ${brokerQ.toFixed(2)}${brokerState?` · ${brokerState}`:''}</div>`:'';
    const microMeta=`<div style="font-size:10px;color:var(--tx3);margin-top:2px">Micro ${microQ.toFixed(2)} · ${depthLabel(p)}</div>`;
    const postureMeta=(rrMult!==1||stopMult!==1)?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">RR x${rrMult.toFixed(2)} · SL x${stopMult.toFixed(2)}</div>`:'';
    const rankMeta=oppScore?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">Opp ${oppScore.toFixed(3)}${oppRank?` · #${oppRank}`:''}</div>`:'';
    const noteMeta=execNotes.length?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">${execNotes.slice(0,2).join(' · ')}</div>`:'';
    const crossMeta=crossPeer?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">${crossAlign>=0?'Cross support':'Cross conflict'} via ${crossPeer}</div>`:'';
    const patternMeta=patternLine?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">${patternLine}</div>`:'';
    const targetMeta=runnerTp && Math.abs(runnerTp-nextTp)>1e-9 ? `<div style="font-size:10px;color:var(--tx3);margin-top:2px">Runner ${fmt(runnerTp)}</div>` : '';
    const killReason=knownKillReason(p);
    return`<tr><td><strong>${p.asset||'?'}</strong><br><span style="font-size:10px;color:var(--tx3)">${p.category||''}</span>${memoryMeta}${execMeta}${eliteMeta}${readyMeta}${provenanceMeta}${brokerMeta}${microMeta}${crossMeta}${patternMeta}${regimeMeta}${killReason?`<div style="font-size:10px;color:var(--rd);margin-top:2px">${killReason}</div>`:''}${reviewNotesLine(p)?`<div style="font-size:10px;color:var(--tx3);margin-top:2px">${reviewNotesLine(p)}</div>`:''}${pBar}</td><td style="color:${dir==='BUY'?'var(--gr)':'var(--rd)'};font-weight:700">${dir}</td><td style="font-family:monospace;font-size:11px">${fmtLot(lot)}</td><td style="font-family:monospace">${fmt(e)}</td><td style="font-family:monospace;font-weight:600;color:${curCol}">${fmt(cur)}</td><td style="font-family:monospace;color:var(--rd)">${fmt(sl)}</td><td style="font-family:monospace;color:var(--gr)">${fmt(nextTp||tp)}${targetMeta}${postureMeta}</td><td class="${pnl>=0?'pnl-pos':'pnl-neg'}" style="font-weight:700">${pnl>=0?'+':''}$${pnl.toFixed(2)}</td><td>${(Number(p.confidence||0)*100).toFixed(0)}%${rankMeta}${noteMeta}</td><td style="font-size:11px;white-space:nowrap">${openStr}</td><td><button onclick="confirmClose('${tid}','${p.asset||'?'}')" style="padding:4px 10px;border-radius:6px;border:none;background:rgba(255,69,96,.15);color:var(--rd);cursor:pointer;font-size:11px;font-weight:600;white-space:nowrap" onmouseover="this.style.background='var(--rd)';this.style.color='#fff'" onmouseout="this.style.background='rgba(255,69,96,.15)';this.style.color='var(--rd)'">✕ Close</button></td></tr>`;
  }).join('');
}
async function loadTopOpportunities(forceRefresh=false){
  const panel=document.getElementById('topOpsPanel');
  if(panel) panel.innerHTML='<div class="spinner" style="grid-column:1/-1;padding:18px 0">Refreshing ranked setups…</div>';
  try{
    const qs=new URLSearchParams({limit:'5'});
    if(forceRefresh) qs.set('refresh','1');
    const r=await fetchProtectedJson('/api/opportunities/top?'+qs.toString(), {timeoutMs: 12000});
    if(!r.success){renderTopOpportunities([]);return;}
    renderTopOpportunities(r.opportunities||[]);
  }catch(e){console.error('[TopOpportunities]',e);renderTopOpportunities([]);}
}
async function runWeakReprice(){
  setActionStatus('Repricing weak exits…','info');
  try{
    const r=await fetchProtectedJson('/api/positions/reprice-weak',{method:'POST',timeoutMs: 15000,body:JSON.stringify({limit:3,tighten_only:true})});
    if(!r.success){setActionStatus(r.error||'Weak exit repricing failed.','error');return;}
    if(!r.repriced){setActionStatus('No weak exits needed repricing right now.','warn');return;}
    const first=(r.updates||[])[0]||{};
    _commandCenterCache=null;
    setActionStatus(`Repriced ${r.repriced} weak position(s). ${first.asset||''} ${first.new_stop_loss?'SL updated':''}`.trim(),'success');
    await Promise.all([loadMain(), loadHistory()]);
  }catch(e){console.error('[WeakReprice]',e);setActionStatus('Weak exit repricing failed.','error');}
}
async function runWeakReduction(){
  setActionStatus('Reducing weak positions…','info');
  try{
    const r=await fetchProtectedJson('/api/positions/reduce-weak',{method:'POST',timeoutMs: 15000,body:JSON.stringify({limit:3,reduction_fraction:0.35})});
    if(!r.success){setActionStatus(r.error||'Weak position reduction failed.','error');return;}
    const reduced=Number(r.reduced||0);
    if(!reduced){setActionStatus('No weak positions qualified for reduction right now.','warn');return;}
    const first=(r.actions||[]).find(a=>a.success)||{};
    _commandCenterCache=null;
    setActionStatus(`Reduced ${reduced} weak position(s). ${first.asset||''} realised ${first.realized_pnl!=null?fmtMoney(first.realized_pnl):''}`.trim(),'success');
    await Promise.all([loadMain(), loadHistory(), loadTopOpportunities(false)]);
  }catch(e){console.error('[WeakReduction]',e);setActionStatus('Weak position reduction failed.','error');}
}
function filterPositions(){
  const filter = document.getElementById('posFilter')?.value || 'all';
  if(!_allPositions.length){document.getElementById('posBody').innerHTML='<tr><td colspan="11" class="empty">No open positions</td></tr>';return;}
  const filtered = filter === 'all' ? _allPositions : filter === 'winning' ? _allPositions.filter(p => Number(p.pnl||0) > 0) : filter === 'losing'  ? _allPositions.filter(p => Number(p.pnl||0) <= 0) : _allPositions.filter(p => (p.category||'').toLowerCase() === filter.toLowerCase());
  if(!filtered.length){document.getElementById('posBody').innerHTML=`<tr><td colspan="11" class="empty">No ${filter} positions</td></tr>`;return;}
  renderPositions(filtered);
}
function handleCloseDropdown(sel){
  const val = sel.value;
  if(!val){ return; }
  sel.value = '';
  const labels = {forex:'Forex',crypto:'Crypto',commodities:'Commodities',indices:'Indices',losing:'Losing',winning:'Winning',all:'ALL'};
  _pendingClose = val === 'all' ? {mode:'all'} : ['forex','crypto','commodities','indices'].includes(val) ? {mode:'category',cat:val} : {mode:'filter',filter:val};
  document.getElementById('closeModalText').textContent = `Close all ${labels[val]} positions?`;
  document.getElementById('closeModalSub').textContent = val === 'all' ? 'All open positions will be closed.' : `All ${labels[val]} positions will be closed.`;
  document.getElementById('closeModalConfirm').onclick = executeClose;
  document.getElementById('closeModal').style.display = 'flex';
}
let _pendingClose=null;
function confirmClose(tid,asset){_pendingClose={tid,asset,mode:'single'};document.getElementById('closeModalText').textContent=`Close ${asset}?`;document.getElementById('closeModalSub').textContent='This will close at current market price.';document.getElementById('closeModalConfirm').onclick=executeClose;document.getElementById('closeModal').style.display='flex';}
async function executeClose(){
  document.getElementById('closeModal').style.display='none';
  if(!_pendingClose)return;
  const{mode,tid,cat,filter}=_pendingClose;_pendingClose=null;
  try{
    let r;
    if(mode==='single'){
      r=await fetchProtectedJson('/api/position/close',{method:'POST',timeoutMs:15000,body:JSON.stringify({trade_id:tid})});
    } else {
      const payload={mode:mode==='category'?'category':mode==='filter'?filter:'all'};
      if(mode==='category')payload.category=cat;
      r=await fetchProtectedJson('/api/position/close-bulk',{method:'POST',timeoutMs:15000,body:JSON.stringify(payload)});
    }
    const ok=r.success||(r.closed>0);
    showToast(ok?`✅ ${r.closed!=null?r.closed+' position(s) closed':r.message||'Closed'}`:`❌ ${r.error||'Failed'}`,ok?'success':'error');
    setTimeout(loadMain,800);
    setTimeout(loadHistory,1200);
  }catch(e){showToast('❌ Network error','error');}
}
function showToast(msg,type){const t=document.createElement('div');t.textContent=msg;t.style.cssText=`position:fixed;bottom:24px;right:24px;padding:12px 20px;border-radius:10px;background:${type==='success'?'rgba(0,208,132,.9)':'rgba(255,69,96,.9)'};color:#fff;font-weight:600;font-size:13px;z-index:9999;box-shadow:0 4px 20px rgba(0,0,0,.3)`;document.body.appendChild(t);setTimeout(()=>t.remove(),3500);}

