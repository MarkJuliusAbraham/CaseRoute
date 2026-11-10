import test from 'node:test';
import assert from 'node:assert/strict';
import { describePipeline } from '../pipeline-control.js';

test('ticked only when both apps are running', () => {
    assert.equal(describePipeline({ paused: false }, { paused: false }).checked, true);
    assert.equal(describePipeline({ paused: true }, { paused: true }).checked, false);
    assert.equal(describePipeline({ paused: true }, { paused: false }).checked, false);
});

test('partly paused names which app is paused', () => {
    const state = describePipeline({ paused: false }, { paused: true });
    assert.match(state.text, /backend running, processor paused/);
});

test('an unreachable app is reported, not assumed running', () => {
    const state = describePipeline({ paused: false }, null);
    assert.equal(state.known, false);
    assert.match(state.text, /processor/);
});
