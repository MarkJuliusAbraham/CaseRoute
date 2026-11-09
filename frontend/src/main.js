import './style.css';
import { generator } from './api/generator.js';
import { clearSavedCases } from './api/simulation.js';
import { Connections } from './connections.js';
import { HealthHistory } from './health-history.js';
import { HealthHistoryView } from './health-history-view.js';
import { CaseReportView } from './case-report.js';
import { DataInspector } from './data-inspector.js';
import { canStartRun } from './run-controls.js';
import { TrafficView } from './visualization/traffic-view.js';
import { SavedCasesPoller } from './saved-cases.js';
import { CheckerView } from './checker.js';
import { ViewSwitcher } from './views.js';

const views = new ViewSwitcher();
if (import.meta.hot) import.meta.hot.dispose(() => views.destroy());

const traffic = new TrafficView(document.getElementById('traffic-visualization'));
if (import.meta.hot) import.meta.hot.dispose(() => traffic.destroy());
const checker = new CheckerView(document.getElementById('checker'), generator);
if (import.meta.hot) import.meta.hot.dispose(() => checker.destroy());
const savedCases = new SavedCasesPoller((runId, stats) => traffic.setStats(runId, stats), runId => checker.autoCheck(runId));
if (import.meta.hot) import.meta.hot.dispose(() => savedCases.stop());

const el = id => document.getElementById(id);
const selectedMode = () => el('use-kafka').checked ? 'KAFKA' : 'DIRECT';
el('use-kafka').addEventListener('change', () => traffic.setMode(selectedMode()));
const caseReport = new CaseReportView(el('received-cases'));
if (import.meta.hot) import.meta.hot.dispose(() => caseReport.destroy());
let historyStorage = null;
try { historyStorage = window.localStorage; } catch { /* Memory-only fallback. */ }
const healthHistory = new HealthHistoryView(el('health-history'), new HealthHistory(historyStorage));
if (import.meta.hot) import.meta.hot.dispose(() => healthHistory.destroy());
const inspector = new DataInspector(el('data-inspector'), () => caseReport.load(), () => healthHistory.render());
if (import.meta.hot) import.meta.hot.dispose(() => inspector.destroy());
el('view-cases').addEventListener('click', event => inspector.open('cases', event.currentTarget));
el('view-responses').addEventListener('click', event => inspector.open('responses', event.currentTarget));
el('view-health').addEventListener('click', event => inspector.open('health', event.currentTarget));
let stream;
let activeId;
let resetBusy = false;
let starting = false;
let runKnown = false;
let activeRun = false;
let services = { generator: false, backend: false };
function updateControls() {
 el('start').disabled = !canStartRun({ generatorLive: services.generator, runKnown, activeRun, starting, resetting: resetBusy });
 el('start').title = !services.generator ? 'Waiting for a live generator connection' : !runKnown ? 'Checking for an existing run' : '';
 el('clear-cases').disabled = resetBusy || activeRun || !runKnown || !services.generator || !services.backend;
}
const connections = new Connections(el('service-connections'), health => {
 services = health;
 traffic.setConnections(health);
 if (!services.generator) {
  runKnown = false;
  if (stream) { stream.close(); stream = null; traffic.disconnect(); }
 }
 updateControls();
 if (!runKnown && services.generator) refreshCurrentRun();
}, observation => healthHistory.record(observation));
if (import.meta.hot) import.meta.hot.dispose(() => connections.destroy());
const terminal = state => ['COMPLETED','STOPPED','ERROR'].includes(state);
function showError(message) { el('error').textContent=message; el('error').hidden=!message; }
function estimate() {
 const planned = Number(el('rate').value) * Number(el('duration').value);
 const warmup = Math.floor(Number(el('rate').value) * Number(el('warmup').value) / 120);
 el('estimate').textContent = `${planned.toLocaleString()} planned submissions` + (warmup ? ` after ${warmup.toLocaleString()} warm-up requests (not stored)` : '');
}
const presets = [...document.querySelectorAll('[data-rate-preset]')];
function markPreset() { for (const b of presets) b.setAttribute('aria-pressed', String(b.dataset.ratePreset === el('rate').value)); }
for (const b of presets) b.addEventListener('click', () => { el('rate').value = b.dataset.ratePreset; estimate(); markPreset(); });
// −50k / +50k adjust the current rate, kept within the input's min and max.
for (const b of document.querySelectorAll('[data-rate-step]')) b.addEventListener('click', () => {
 const rate = el('rate');
 const next = (Number(rate.value) || 0) + Number(b.dataset.rateStep);
 rate.value = Math.max(Number(rate.min), Math.min(Number(rate.max), next));
 estimate(); markPreset();
});
el('rate').addEventListener('input', markPreset);
el('rate').addEventListener('input',estimate);el('duration').addEventListener('input',estimate);el('warmup').addEventListener('input',estimate);
function updateCapacityInput() {
 el('capacity').disabled = activeRun || starting || el('unlimited').checked;
 el('capacity').required = !el('unlimited').checked;
 el('capacity-field').hidden = el('unlimited').checked;
 el('use-kafka').disabled = activeRun || starting;
 el('retry').disabled = activeRun || starting;
}
el('unlimited').addEventListener('change', updateCapacityInput);
updateCapacityInput();
estimate();
function render(run) {
 traffic.update(run);
 savedCases.track(run);
 checker.setRun(run);
 runKnown = true; activeRun = run.finishedAt == null; updateControls();
 activeId=run.runId;
 caseReport.setRunId(run.runId);
 el('state').textContent=run.state;
 const mode = run.settings.submissionMode === 'DIRECT' ? 'Direct to PostgreSQL' : 'Through Kafka';
 const retries = run.settings.retryEnabled ? 'retries on' : 'retries off';
 el('run-id').textContent=`Run ${run.runId} · ${mode} · ${retries}`;
 for(const key of ['dispatched','accepted','failed','unknown','skipped','inFlight','retried','recovered'])el(key).textContent=(run[key] ?? 0).toLocaleString();
 const accounted=run.accepted+run.failed+run.unknown+run.skipped+run.notSent;
 el('progress').max=run.planned;el('progress').value=accounted;
 el('progress-label').textContent = run.state === 'WARMING_UP'
  ? `Warming up: ${(run.warmupSent ?? 0).toLocaleString()} of ${(run.warmupPlanned ?? 0).toLocaleString()} warm-up requests sent (not stored)`
  : `${accounted.toLocaleString()} of ${run.planned.toLocaleString()} scheduled submissions accounted for`
   + (run.warmupSent ? ` · warm-up: ${run.warmupSent.toLocaleString()} sent, ${run.warmupFailed.toLocaleString()} failed` : '');
 el('not-sent').textContent=run.notSent?`${run.notSent.toLocaleString()} submissions were not sent before the run stopped.`:'';
 const finished=terminal(run.state);
 updateControls();el('stop').disabled=finished||run.state==='DRAINING';
 for(const input of el('run-form').querySelectorAll('input, button'))input.disabled=!finished;
 updateCapacityInput();
 el('download').hidden=false;el('download').href=generator.reportUrl(run.runId);
 if(run.error)showError(run.error);
 const body=el('results');body.replaceChildren();
 if(!run.recentResults.length){const row=body.insertRow();const cell=row.insertCell();cell.colSpan=6;cell.className='empty';cell.textContent='Waiting for the first response.';}
 for(const result of run.recentResults){
  const row=body.insertRow();
  const values=[result.clientRequestId,result.outcome,result.httpStatus??'—',result.attempts??1,`${result.latencyMs.toFixed(1)} ms`,`${result.schedulingDelayMs.toFixed(1)} ms`];
  values.forEach((value,index)=>{const cell=row.insertCell();cell.textContent=value;if(index===1){cell.className=result.outcome==='ACCEPTED'?'accepted':'problem';cell.title=result.detail;}});
 }
 if(finished&&stream){stream.close();stream=null;el('connection').textContent='Run finished';}
}
function subscribe(id){
 if(stream)stream.close();
 stream=generator.events(id);
 stream.addEventListener('progress',event=>{el('connection').textContent='Live updates connected';render(JSON.parse(event.data));});
 stream.onerror=()=>{traffic.disconnect();el('connection').textContent='Live updates interrupted · reconnecting';};
}
el('run-form').addEventListener('submit',async event=>{
 event.preventDefault();
 if (!canStartRun({ generatorLive: services.generator, runKnown, activeRun, starting, resetting: resetBusy })) return;
 showError('');starting=true;updateControls();
 const settings=Object.fromEntries(new FormData(event.target));for(const key in settings)settings[key]=Number(settings[key]);
 settings.maxInFlight = el('unlimited').checked ? null : Number(el('capacity').value);
 settings.submissionMode = selectedMode();
 settings.retryEnabled = el('retry').checked;
 try{const run=await generator.start(settings);render(run);subscribe(run.runId);}
 catch(error){showError(error.message);await refreshCurrentRun();}
 finally { starting=false;updateControls();updateCapacityInput(); }
});
el('stop').addEventListener('click',async()=>{el('stop').disabled=true;try{render(await generator.stop(activeId));}catch(error){showError(error.message);el('stop').disabled=false;}});
async function refreshCurrentRun() {
 try {
  const run = await generator.current();
  runKnown = true;
  activeRun = run != null && run.finishedAt == null;
  if (run) { render(run); if (activeRun && !stream) subscribe(run.runId); }
  updateControls();
  if (!resetBusy) el('reset-status').textContent = activeRun ? 'Stop the run and wait for outstanding requests before clearing cases.' : 'Requires confirmation. This cannot be undone.';
 } catch { runKnown = false; updateControls(); }
}
// In-page confirmation: resolves true only when "Delete all cases" is chosen. Escape or Cancel resolve false.
function confirmReset() {
 const dialog = el('confirm-reset');
 dialog.returnValue = '';
 dialog.showModal();
 return new Promise(resolve => dialog.addEventListener('close', () => resolve(dialog.returnValue === 'confirm'), { once: true }));
}
el('clear-cases').addEventListener('click', async () => {
 resetBusy = true; updateControls(); showError('');
 try {
  // Recheck generator state immediately before offering the destructive action.
  const run = await generator.current();
  if (run && run.finishedAt == null) throw new Error('A run is active. Stop it and wait for its requests to finish first.');
  if (!(await confirmReset())) { el('reset-status').textContent = 'Cancelled. No cases were deleted.'; return; }
  el('reset-status').textContent = 'Clearing saved cases...';
  await clearSavedCases();
  if (el('data-inspector').open && !el('received-cases').hidden) await caseReport.load();
  el('reset-status').textContent = 'Saved cases cleared. Previous run counters and reports remain as history.';
 } catch (error) { showError(error.message); el('reset-status').textContent = 'Reset was not confirmed. Check the error above.'; }
 finally { resetBusy = false; updateControls(); }
});
refreshCurrentRun();
