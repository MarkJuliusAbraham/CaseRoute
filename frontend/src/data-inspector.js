export class DataInspector {
    constructor(dialog, onCasesOpened, onHealthOpened = () => {}) {
        this.dialog = dialog;
        this.onCasesOpened = onCasesOpened;
        this.onHealthOpened = onHealthOpened;
        this.tabs = [...dialog.querySelectorAll('[role="tab"]')];
        this.onClick = event => {
            const tab = event.target.closest('[role="tab"]');
            if (tab) this.select(tab.id);
            if (event.target.closest('[data-inspector-close]')) dialog.close();
        };
        this.onKey = event => {
            if (!event.target.matches('[role="tab"]')) return;
            const index = this.tabs.indexOf(event.target);
            const next = event.key === 'ArrowRight' ? (index + 1) % this.tabs.length
                : event.key === 'ArrowLeft' ? (index + this.tabs.length - 1) % this.tabs.length
                : event.key === 'Home' ? 0 : event.key === 'End' ? this.tabs.length - 1 : null;
            if (next === null) return;
            event.preventDefault();this.select(this.tabs[next].id);this.tabs[next].focus();
        };
        this.onClose = () => {
            document.documentElement.classList.remove('inspector-open');
            this.launcher?.focus({ preventScroll: true });
        };
        dialog.addEventListener('click', this.onClick);
        dialog.addEventListener('keydown', this.onKey);
        dialog.addEventListener('close', this.onClose);
    }

    open(view, launcher) {
        this.launcher = launcher;
        if (!this.dialog.open) {
            document.documentElement.classList.add('inspector-open');
            this.dialog.showModal();
        }
        this.select(({ cases: 'cases-tab', responses: 'responses-tab', health: 'health-tab' })[view]);
        this.tabs.find(tab => tab.getAttribute('aria-selected') === 'true').focus({ preventScroll: true });
    }

    select(id) {
        for (const tab of this.tabs) {
            const selected = tab.id === id;
            tab.setAttribute('aria-selected', String(selected));
            tab.tabIndex = selected ? 0 : -1;
            this.dialog.querySelector('#' + tab.getAttribute('aria-controls')).hidden = !selected;
        }
        if (id === 'cases-tab') this.onCasesOpened();
        if (id === 'health-tab') this.onHealthOpened();
    }

    destroy() {
        this.dialog.close();
        this.dialog.removeEventListener('click', this.onClick);
        this.dialog.removeEventListener('keydown', this.onKey);
        this.dialog.removeEventListener('close', this.onClose);
        document.documentElement.classList.remove('inspector-open');
    }
}
