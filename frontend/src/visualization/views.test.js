import test from 'node:test';
import assert from 'node:assert/strict';
import { viewFor } from '../views.js';

test('documentation hashes open the docs view, anything else the simulator', () => {
    assert.equal(viewFor('#docs'), 'docs');
    assert.equal(viewFor('#docs-generator'), 'docs');
    assert.equal(viewFor(''), 'simulator');
    assert.equal(viewFor('#'), 'simulator');
    assert.equal(viewFor('#something-else'), 'simulator');
});
