import test from 'node:test';
import assert from 'node:assert/strict';
import { describeShedding } from '../shedding-control.js';

test('on: shows the limit, what is in progress and how many were turned away', () => {
    const text = describeShedding({ enabled: true, maxConcurrent: 1000, inFlight: 37, shed: 12345 });
    assert.match(text, /^On: at most 1,000 submissions at once, 37 now\. 12,345 turned away/);
});

test('off: says everything is accepted', () => {
    assert.match(describeShedding({ enabled: false, maxConcurrent: 1000, inFlight: 0, shed: 0 }), /^Off: every submission is accepted/);
});

test('unreachable backend is reported with what to do', () => {
    assert.match(describeShedding(null), /restart it/);
});
