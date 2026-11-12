import test from 'node:test';
import assert from 'node:assert/strict';
import { canRetry, describeRetry, retryable } from '../retry-period.js';

const finished = { finishedAt: '2026-11-11T01:00:00Z', state: 'COMPLETED', skipped: 100, blocked: 20, unknown: 3 };

test('skipped, blocked and unknown are what a retry period sends', () => {
    assert.equal(retryable(finished), 123);
    assert.equal(retryable({ skipped: 0, blocked: 0, unknown: 0, failed: 9 }), 0, 'definite rejections are never retried');
});

test('Retry now needs a finished run that is not already retrying and has something to retry', () => {
    assert.equal(canRetry(finished), true);
    assert.equal(canRetry({ ...finished, finishedAt: null }), false);
    assert.equal(canRetry({ ...finished, state: 'RETRYING' }), false);
    assert.equal(canRetry({ ...finished, skipped: 0, blocked: 0, unknown: 0 }), false);
});

test('status before, during and after a retry period', () => {
    assert.match(describeRetry(finished), /^123 submissions have no definite answer/);
    const pass = { pass: 2, planned: 50, sent: 10, accepted: 8, skipped: 0, blocked: 0, unknown: 2, failed: 0 };
    assert.match(describeRetry({ ...finished, state: 'RETRYING', retry: { state: 'RETRYING', maxPasses: 10, recovered: 81,
        endsBy: '2026-11-11T01:02:00Z', passes: [pass, pass] } }), /^Retry period, pass 2 of up to 10: 10 of 50 sent, 81 recovered so far/);
    assert.equal(describeRetry({ ...finished, skipped: 0, blocked: 0, unknown: 2, retry: { state: 'DONE', maxPasses: 10,
        recovered: 121, reason: 'A pass resolved nothing; the backend may be unavailable', passes: [pass, pass, pass] } }),
        'Retry period finished after 3 passes: 121 recovered, 2 still without a definite answer. A pass resolved nothing; the backend may be unavailable');
    assert.equal(describeRetry({ ...finished, finishedAt: null }), '', 'nothing to say while the run is going');
});
