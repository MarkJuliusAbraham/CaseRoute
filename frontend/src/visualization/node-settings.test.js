import test from 'node:test';
import assert from 'node:assert/strict';
import { nodeContent, readLayout } from '../node-settings.js';
import { nodeAt } from './traffic-view.js';

test('every graph node maps to settings content', () => {
    for (const name of ['generator', 'backend', 'kafka', 'database', 'processor']) {
        assert.ok(nodeContent(name)?.parts.length, name);
    }
    assert.deepEqual(nodeContent('database').parts, ['database-info', 'reset-block']);
    assert.equal(nodeContent('nothing'), null);
});

test('layout defaults to panel and survives storage errors', () => {
    assert.equal(readLayout(null), 'panel');
    assert.equal(readLayout({ getItem: () => 'simple' }), 'simple');
    assert.equal(readLayout({ getItem: () => { throw new Error('blocked'); } }), 'panel');
});

test('a point inside a node box (with a little slack) hits it, empty space hits nothing', () => {
    const nodes = { generator: { name: 'generator', x: 10, y: 10, w: 100, h: 50 }, backend: { name: 'backend', x: 200, y: 10, w: 100, h: 50 } };
    assert.equal(nodeAt(nodes, 50, 30), 'generator');
    assert.equal(nodeAt(nodes, 112, 30), 'generator', 'within the slack');
    assert.equal(nodeAt(nodes, 250, 59), 'backend');
    assert.equal(nodeAt(nodes, 150, 30), null);
});
