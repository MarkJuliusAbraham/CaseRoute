import test from 'node:test';
import assert from 'node:assert/strict';
import { nextOpen } from '../hints.js';

test('clicking a closed hint opens it, clicking it again closes it, another one replaces it', () => {
    const a = {}, b = {};
    assert.equal(nextOpen(null, a), a);
    assert.equal(nextOpen(a, a), null);
    assert.equal(nextOpen(a, b), b);
});
