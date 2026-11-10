import test from 'node:test';
import assert from 'node:assert/strict';
import { checkRows, VERDICT_LABELS } from '../checker.js';

const report = {
    verdict: 'DISCREPANCIES',
    accepted: { total: 100, processed: 97, awaitingProcessing: 1, missing: 2 },
    unknown: { total: 5, saved: 3, notSaved: 2 },
    rejectedButSaved: 0,
    blocked: { total: 4, saved: 0 },
    unexpected: 0,
};

test('rows cover accepted, unknown and impossible outcomes in order', () => {
    const rows = checkRows(report);
    assert.deepEqual(rows.map(([label]) => label), [
        'Accepted replies', 'Saved and processed', 'Saved, awaiting processing', 'Accepted but missing',
        'Outcome unknown', 'Unknown, saved anyway', 'Unknown, never arrived', 'Blocked (backend busy)', 'Blocked but saved',
        'Rejected but saved', 'Unexpected cases',
    ]);
    assert.deepEqual(rows.map(([, value]) => value), [100, 97, 1, 2, 5, 3, 2, 4, 0, 0, 0]);
});

test('only counts that should be zero are flagged, and only when they are not', () => {
    const flagged = checkRows(report).filter(([, , tone]) => tone === 'problem').map(([label]) => label);
    assert.deepEqual(flagged, ['Accepted but missing']);
    const clean = checkRows({ ...report, accepted: { ...report.accepted, missing: 0 } });
    assert.equal(clean.some(([, , tone]) => tone === 'problem'), false);
});

test('every verdict has a label', () => {
    for (const verdict of ['CONSISTENT', 'PENDING', 'DISCREPANCIES']) assert.ok(VERDICT_LABELS[verdict]);
});
