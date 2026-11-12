import test from 'node:test';
import assert from 'node:assert/strict';
import { backendReachable, duringRun, outcomeSummary, retryPeriodStatus, retrySent, totalSent } from '../results-view.js';

// The 1M run with a retry period: 739,711 accepted during the run, 260,289 recovered in two passes.
const run = {
    planned: 1_000_000, dispatched: 739_711, accepted: 1_000_000, failed: 0, unknown: 0, skipped: 0, blocked: 0, notSent: 0,
    retried: 0, recovered: 0, retryRequests: 0, finishedAt: '2026-11-11T18:55:36Z', state: 'COMPLETED',
    settings: { retryEnabled: false, retryPeriod: true, retryPeriodSeconds: 120, httpClient: 'JETTY' },
    retry: { state: 'DONE', reason: 'Nothing left to retry', maxPasses: 10, recovered: 260_289, passes: [
        { pass: 1, planned: 260_289, sent: 260_289, accepted: 248_938, skipped: 11_351, blocked: 0, unknown: 0, failed: 0 },
        { pass: 2, planned: 11_351, sent: 11_351, accepted: 11_351, skipped: 0, blocked: 0, unknown: 0, failed: 0 }] },
};

test('requests sent include the retry period, not only the run', () => {
    assert.equal(retrySent(run), 271_640);
    assert.equal(totalSent(run), 739_711 + 271_640);
    assert.equal(totalSent({ ...run, duplicates: { sent: 5_000, accepted: 5_000, other: 0 } }), 739_711 + 271_640 + 5_000,
        'repeated submissions are requests too');
});

test('the run itself is credited only with what it accepted; the rest is the retry period', () => {
    const d = duringRun(run);
    assert.equal(d.sent, 739_711);
    assert.equal(d.accepted, 739_711);
    assert.equal(d.retriesOn, false);
});

test('the outcome parts always add up to the plan', () => {
    const o = outcomeSummary({ planned: 900, accepted: 600, skipped: 100, blocked: 20, unknown: 30, failed: 5, notSent: 45, finishedAt: null });
    assert.equal(o.accepted + o.unresolved + o.rejected + o.notSent + o.pending, 900);
    assert.equal(o.unresolved, 150);
    assert.equal(o.pending, 100, 'in flight or not yet due');
    assert.equal(outcomeSummary(run).pending, 0);
});

test('backend reachability comes from the circuit breaker', () => {
    assert.equal(backendReachable(null), null, 'off: no row');
    assert.equal(backendReachable({ state: 'CLOSED', since: '2026-11-12T10:00:00Z' }), 'Yes');
    assert.match(backendReachable({ state: 'OPEN', since: '2026-11-12T10:00:00Z' }), /^No: sending paused since /);
    assert.match(backendReachable({ state: 'RECOVERING', since: '2026-11-12T10:00:00Z' }), /^Back: ramping up since /);
});

test('retry period status says what happened or will happen', () => {
    assert.equal(retryPeriodStatus(run), 'Finished: Nothing left to retry');
    assert.match(retryPeriodStatus({ ...run, retry: null, finishedAt: null }), /^On: starts when the run ends, for up to 120 s/);
    assert.match(retryPeriodStatus({ ...run, retry: null, settings: { ...run.settings, retryPeriod: false } }), /^Not used/);
});
