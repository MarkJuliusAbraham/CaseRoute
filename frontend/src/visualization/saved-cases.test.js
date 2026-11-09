import test from 'node:test';
import assert from 'node:assert/strict';
import { shouldKeepPolling, settled } from '../saved-cases.js';

const done = { saved: 100, open: 0, routed: 99, failed: 1, awaitingRelay: 0 };

test('polling continues while the run is active', () => {
    assert.equal(shouldKeepPolling({ active: true, stats: done, accepted: 100, finishedFor: 0 }), true);
});
test('after the run, polling continues until every case is saved and processed', () => {
    assert.equal(shouldKeepPolling({ active: false, stats: { ...done, saved: 40, open: 0 }, accepted: 100, finishedFor: 5000 }), true);
    assert.equal(shouldKeepPolling({ active: false, stats: { ...done, open: 12 }, accepted: 100, finishedFor: 5000 }), true);
    assert.equal(shouldKeepPolling({ active: false, stats: { ...done, awaitingRelay: 3 }, accepted: 100, finishedFor: 5000 }), true);
    assert.equal(shouldKeepPolling({ active: false, stats: done, accepted: 100, finishedFor: 5000 }), false);
});
test('polling gives up five minutes after the run if the pipeline never settles', () => {
    assert.equal(shouldKeepPolling({ active: false, stats: { ...done, open: 5 }, accepted: 100, finishedFor: 300_000 }), false);
    assert.equal(shouldKeepPolling({ active: false, stats: null, accepted: 100, finishedFor: 300_000 }), false);
});
test('settled needs all accepted cases saved, none open, and an empty outbox', () => {
    assert.equal(settled(done, 100), true);
    assert.equal(settled(null, 100), false);
    assert.equal(settled({ ...done, saved: 99 }, 100), false);
});
