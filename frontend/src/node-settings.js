// Simple / Panel layouts, and the settings dialog opened by clicking a node in the graph (Simple view).
//
// Panel view: the settings cards sit above the graph, as before.
// Simple view: only the run bar, the graph and the checker. Clicking a node, or its button under the graph, opens a
// dialog with that component's settings. The dialog shows the real card, moved in while open and put back on close,
// so there is only ever one copy of every control.

const NODES = {
    generator: { title: 'Generator settings', parts: ['generator-card'] },
    backend: { title: 'Backend settings', parts: ['backend-card'] },
    processor: { title: 'Processor settings', parts: ['processor-card'] },
    kafka: { title: 'Kafka', parts: ['kafka-info'] },
    database: { title: 'PostgreSQL', parts: ['database-info', 'reset-block'] },
};
const STORAGE_KEY = 'caseroute.layout';

/** The element ids shown for a node, and the dialog title. */
export function nodeContent(name) {
    return NODES[name] ?? null;
}

/** 'simple' or 'panel' (the default), from browser storage when available. */
export function readLayout(storage) {
    try { return storage?.getItem(STORAGE_KEY) === 'simple' ? 'simple' : 'panel'; } catch { return 'panel'; }
}

function saveLayout(storage, layout) {
    try { storage?.setItem(STORAGE_KEY, layout); } catch { /* the choice just isn't remembered */ }
}

export class NodeSettings {
    constructor(dialog) {
        this.dialog = dialog;
        this.slot = dialog.querySelector('[data-node-slot]');
        this.title = dialog.querySelector('#node-settings-title');
        this.moved = [];
        // The 'close' event arrives asynchronously (also after Escape), so restore right away wherever possible too.
        this.onClose = () => this.restore();
        this.onCloseClick = () => { this.dialog.close(); this.restore(); };
        // A click on the backdrop (outside the dialog's box) closes it. Clicks on the dialog's own padding also
        // target the dialog element, so compare with its box rather than relying on the target alone.
        this.onBackdropClick = event => {
            if (event.target !== this.dialog) return;
            const box = this.dialog.getBoundingClientRect();
            const outside = event.clientX < box.left || event.clientX > box.right
                || event.clientY < box.top || event.clientY > box.bottom;
            if (outside) this.onCloseClick();
        };
        this.dialog.addEventListener('close', this.onClose);
        this.dialog.addEventListener('click', this.onBackdropClick);
        dialog.querySelector('[data-node-close]').addEventListener('click', this.onCloseClick);
    }

    open(name) {
        const content = nodeContent(name);
        if (!content) return;
        if (this.dialog.open) this.dialog.close();
        this.restore(); // whatever a previous open moved in goes back first
        this.title.textContent = content.title;
        for (const id of content.parts) {
            const element = document.getElementById(id);
            if (!element) continue;
            const placeholder = document.createComment(`${id} placeholder`);
            element.before(placeholder);
            this.slot.append(element);
            const wasHidden = element.hidden;
            element.hidden = false;
            this.moved.push({ element, placeholder, wasHidden });
        }
        this.dialog.showModal();
    }

    restore() {
        for (const { element, placeholder, wasHidden } of this.moved) {
            placeholder.replaceWith(element);
            element.hidden = wasHidden;
        }
        this.moved = [];
    }

    destroy() {
        if (this.dialog.open) this.dialog.close();
        this.restore();
        this.dialog.removeEventListener('close', this.onClose);
        this.dialog.removeEventListener('click', this.onBackdropClick);
    }
}

export class LayoutSwitch {
    constructor(main, traffic, settings, storage) {
        this.main = main;
        this.traffic = traffic;
        this.settings = settings;
        this.storage = storage;
        this.buttons = [...main.querySelectorAll('[data-layout]')];
        this.shortcuts = [...main.querySelectorAll('[data-node]')];
        this.onLayoutClick = event => this.apply(event.currentTarget.dataset.layout);
        this.onShortcutClick = event => this.settings.open(event.currentTarget.dataset.node);
        for (const button of this.buttons) button.addEventListener('click', this.onLayoutClick);
        for (const button of this.shortcuts) button.addEventListener('click', this.onShortcutClick);
        this.apply(readLayout(storage), false);
    }

    apply(layout, remember = true) {
        this.main.dataset.layout = layout;
        for (const button of this.buttons) button.setAttribute('aria-pressed', String(button.dataset.layout === layout));
        this.traffic.setNodeClick(layout === 'simple' ? name => this.settings.open(name) : null);
        if (remember) saveLayout(this.storage, layout);
    }

    destroy() {
        for (const button of this.buttons) button.removeEventListener('click', this.onLayoutClick);
        for (const button of this.shortcuts) button.removeEventListener('click', this.onShortcutClick);
        this.traffic.setNodeClick(null);
    }
}
