// Small tab sets: one panel visible at a time, chosen by buttons. Used for the settings column (one service's card at
// a time) and the results (Outcomes or Checker), so the page needs far less scrolling. The choice is remembered.

/**
 * @param buttons  the tab buttons; each has `data-tab` naming the id of the panel it shows
 * @param storage  where to remember the choice (may be null), under `key`
 * @param onShow   called with the id shown, after it is shown
 */
export class Tabs {
    constructor(buttons, { storage = null, key = null, onShow = () => {} } = {}) {
        this.buttons = [...buttons];
        this.storage = storage;
        this.key = key;
        this.onShow = onShow;
        this.onClick = event => this.show(event.currentTarget.dataset.tab);
        this.onKey = event => {
            if (event.key !== 'ArrowRight' && event.key !== 'ArrowLeft') return;
            const i = this.buttons.indexOf(event.currentTarget);
            const next = this.buttons[(i + (event.key === 'ArrowRight' ? 1 : this.buttons.length - 1)) % this.buttons.length];
            this.show(next.dataset.tab);
            next.focus();
        };
        for (const button of this.buttons) {
            button.setAttribute('role', 'tab');
            button.addEventListener('click', this.onClick);
            button.addEventListener('keydown', this.onKey);
        }
        let saved = null;
        try { saved = key && storage ? storage.getItem(key) : null; } catch { /* not remembered */ }
        this.show(this.buttons.some(b => b.dataset.tab === saved) ? saved : this.buttons[0]?.dataset.tab);
    }

    show(id) {
        for (const button of this.buttons) {
            const selected = button.dataset.tab === id;
            button.setAttribute('aria-selected', String(selected));
            button.tabIndex = selected ? 0 : -1;
            const panel = document.getElementById(button.dataset.tab);
            if (panel) panel.classList.toggle('tab-hidden', !selected);
        }
        this.current = id;
        try { if (this.key && this.storage) this.storage.setItem(this.key, id); } catch { /* not remembered */ }
        this.onShow(id);
    }
}
