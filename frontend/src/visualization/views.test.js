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

test('the documentation shows one section: the named one, the one holding the target, or the first', async () => {
    const { docsSectionFor } = await import('../views.js');
    const ids = ['docs-overview', 'docs-faq', 'docs-tools'];
    assert.equal(docsSectionFor('#docs', ids), 'docs-overview');
    assert.equal(docsSectionFor('#docs-tools', ids), 'docs-tools');
    assert.equal(docsSectionFor('#faq-kafka', ids, id => (id === 'faq-kafka' ? 'docs-faq' : null)), 'docs-faq');
    assert.equal(docsSectionFor('#nowhere', ids, () => null), 'docs-overview');
});

test('FAQ links open the documentation too', () => {
    assert.equal(viewFor('#faq-kafka'), 'docs');
});
