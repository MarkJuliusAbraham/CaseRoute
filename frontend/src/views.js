// The header tabs: Simulator (the default) and Documentation. The view follows the URL hash, so
// #docs, and #docs-<section> for one application's section, can be linked and survive a reload.

export function viewFor(hash) {
    return hash.startsWith('#docs') ? 'docs' : 'simulator';
}

export class ViewSwitcher {
    constructor(doc = document) {
        this.doc = doc;
        this.onChange = () => this.apply();
        window.addEventListener('hashchange', this.onChange);
        this.apply();
    }

    apply() {
        const hash = window.location.hash;
        const view = viewFor(hash);
        for (const panel of this.doc.querySelectorAll('[data-view]')) panel.hidden = panel.dataset.view !== view;
        for (const tab of this.doc.querySelectorAll('[data-view-tab]')) {
            if (tab.dataset.viewTab === view) tab.setAttribute('aria-current', 'page');
            else tab.removeAttribute('aria-current');
        }
        for (const link of this.doc.querySelectorAll('.docs-nav a')) {
            if (link.getAttribute('href') === hash) link.setAttribute('aria-current', 'location');
            else link.removeAttribute('aria-current');
        }
        // The target was hidden when the browser tried to scroll to it, so scroll now that it is visible.
        const target = hash.length > 1 && hash !== '#docs' ? this.doc.getElementById(hash.slice(1)) : null;
        if (target) target.scrollIntoView();
        else if (view === 'docs') window.scrollTo(0, 0);
    }

    destroy() { window.removeEventListener('hashchange', this.onChange); }
}
