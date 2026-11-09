import test from 'node:test';
import assert from 'node:assert/strict';
import { shouldKeepPolling } from '../saved-cases.js';

test('polling continues while the run is active', () => {
    assert.equal(shouldKeepPolling({ active: true, saved: 10, accepted: 10, finishedFor: 0 }), true);
});
test('after the run, polling continues until saved catches up with accepted', () => {
    assert.equal(shouldKeepPolling({ active: false, saved: 40, accepted: 100, finishedFor: 5000 }), true);
    assert.equal(shouldKeepPolling({ active: false, saved: 100, accepted: 100, finishedFor: 5000 }), false);
});
test('polling gives up two minutes after the run if saved never catches up', () => {
    assert.equal(shouldKeepPolling({ active: false, saved: 40, accepted: 100, finishedFor: 120_000 }), false);
    assert.equal(shouldKeepPolling({ active: false, saved: null, accepted: 100, finishedFor: 120_000 }), false);
});
