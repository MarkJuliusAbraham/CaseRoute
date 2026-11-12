import test from 'node:test';
import assert from 'node:assert/strict';
import { PRESETS, presetSettings } from '../run-presets.js';

test('every preset is a run the generator accepts', () => {
    for (const preset of PRESETS) {
        const s = presetSettings(preset);
        assert.ok(s.rate >= 1 && s.rate <= 1_000_000, `${preset.id}: rate`);
        assert.ok(s.duration >= 1 && s.duration <= 60, `${preset.id}: duration`);
        assert.ok(s.warmup >= 0 && s.warmup <= 300, `${preset.id}: warm-up`);
        assert.ok(s.retryPeriodSeconds >= 1 && s.retryPeriodSeconds <= 3600, `${preset.id}: retry period`);
        assert.ok(s.duplicates >= 0 && s.duplicates <= 50, `${preset.id}: duplicates`);
        assert.ok(['KAFKA', 'DIRECT', 'NOOP'].includes(s.intake), `${preset.id}: intake`);
        assert.ok(preset.purpose && preset.lookFor, `${preset.id}: explained`);
    }
});

test('ids are unique, and unset values fall back to the defaults', () => {
    assert.equal(new Set(PRESETS.map(p => p.id)).size, PRESETS.length);
    const smoke = presetSettings(PRESETS.find(p => p.id === 'smoke'));
    assert.equal(smoke.httpClient, 'JETTY');
    assert.equal(smoke.circuitBreaker, true);
    assert.equal(smoke.duplicates, 0);
});

test('the presets that measure a limit leave the retry period off, so the limit is not hidden', () => {
    for (const id of ['capacity', 'connection']) assert.equal(presetSettings(PRESETS.find(p => p.id === id)).retryPeriod, false, id);
});
