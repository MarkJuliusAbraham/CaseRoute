import test from 'node:test';
import assert from 'node:assert/strict';
import { canStartRun } from '../run-controls.js';

const ready = {generatorLive:true, runKnown:true, activeRun:false, starting:false, resetting:false};
test('start requires live generator and known idle run status', () => {
 assert.equal(canStartRun(ready), true);
 assert.equal(canStartRun({...ready, generatorLive:false}), false);
 assert.equal(canStartRun({...ready, runKnown:false}), false);
});
test('start remains blocked through run, pending start, and reset', () => {
 for (const flag of ['activeRun','starting','resetting']) assert.equal(canStartRun({...ready,[flag]:true}), false);
});
