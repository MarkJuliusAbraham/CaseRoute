import test from 'node:test';
import assert from 'node:assert/strict';
import { metricRows, readProducerForm } from '../kafka-panel.js';

test('producer form converts KB to bytes and keeps the other values', () => {
    assert.deepEqual(readProducerForm({ lingerMs: '20', batchSizeKb: '64', compression: 'lz4', maxInFlight: '5', producers: '4' }),
        { settings: { lingerMs: 20, batchSizeBytes: 65536, compression: 'lz4', maxInFlight: 5, producers: 4 } });
});

test('producer form refuses values the backend would refuse', () => {
    assert.match(readProducerForm({ lingerMs: '-1', batchSizeKb: '16', compression: 'none', maxInFlight: '5', producers: '1' }).error, /Linger/);
    assert.match(readProducerForm({ lingerMs: '5', batchSizeKb: '0', compression: 'none', maxInFlight: '5', producers: '1' }).error, /Batch/);
    assert.match(readProducerForm({ lingerMs: '5', batchSizeKb: '16', compression: 'none', maxInFlight: '6', producers: '1' }).error, /in flight/);
    assert.match(readProducerForm({ lingerMs: '5', batchSizeKb: '16', compression: 'none', maxInFlight: '5', producers: '9' }).error, /Producers/);
});

test('metrics are formatted, and missing ones show a dash', () => {
    const rows = new Map(metricRows({ 'record-queue-time-avg': 12.345, 'batch-size-avg': 3072, 'compression-rate-avg': 0.42,
        'request-latency-avg': null }));
    assert.equal(rows.get('Waiting in producer (avg)'), '12.3 ms');
    assert.equal(rows.get('Batch size (avg)'), '3.0 KB');
    assert.equal(rows.get('Compressed to'), '42%');
    assert.equal(rows.get('Broker confirmation (avg)'), '—');
    assert.equal(metricRows(null).every(([, text]) => text === '—'), true, 'unreachable backend');
});
