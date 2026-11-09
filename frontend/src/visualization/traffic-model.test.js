import test from 'node:test';
import assert from 'node:assert/strict';
import { TrafficModel } from './traffic-model.js';
const snapshot = (dispatched, runId = 'one') => ({runId, dispatched});

test('initial observations and reconnects do not replay historical traffic', () => {
 const m = new TrafficModel();
 assert.equal(m.update(snapshot(100), 0).count, 0);
 assert.equal(m.update(snapshot(105), 500).count, 5);
 m.disconnect();
 assert.equal(m.update(snapshot(200), 1000).count, 0);
 assert.equal(m.update(snapshot(203), 1500).count, 3);
 assert.equal(m.rate(1500),360);
 assert.equal(m.update(snapshot(500), 5000).count, 0);
});
test('large batches bound particles while preserving observed counts', () => {
 const m = new TrafficModel(); m.update(snapshot(0), 0);
 assert.deepEqual(m.update(snapshot(5000), 500), {count:5000, particles:80});
 assert.equal(m.buckets(500).reduce((a,b)=>a+b,0),5000);
 assert.equal(m.update(snapshot(5000),600).count,0);
});
test('rate measures observations and becomes unavailable for stale data', () => {
 const m = new TrafficModel();m.update(snapshot(0),0);m.update(snapshot(5),500);
 assert.equal(m.rate(500),600);
 assert.equal(m.rate(4000),null);
 m.disconnect();assert.equal(m.rate(600),null);
});
test('new runs reset counts and hidden-tab rebasing prevents catch-up bursts', () => {
 const m = new TrafficModel();m.update(snapshot(0),0);m.update(snapshot(5),500);
 assert.equal(m.update(snapshot(1,'two'),1000).count,0);
 assert.equal(m.buckets(1000).reduce((a,b)=>a+b,0),0);
 m.rebase();assert.equal(m.update(snapshot(100,'two'),1500).count,0);
});
import { DeltaTracker, kafkaBacklog } from './traffic-model.js';
test('delta tracker counts increases and rebaselines after gaps or resets', () => {
 const t = new DeltaTracker();
 assert.equal(t.update(100, 0), 0);
 assert.equal(t.update(130, 1000), 30);
 assert.equal(t.update(200, 5000), 0);
 assert.equal(t.update(10, 6000), 0);
 t.disconnect(); assert.equal(t.update(50, 6500), 0);
 assert.equal(t.update(55, 7000), 5);
});
test('kafka backlog is accepted minus saved, unknown until saved is known', () => {
 assert.equal(kafkaBacklog(100, null), null);
 assert.equal(kafkaBacklog(100, 40), 60);
 assert.equal(kafkaBacklog(100, 120), 0);
});
