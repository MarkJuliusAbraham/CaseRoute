// The header tabs: Simulator (the default) and Documentation. The view follows the URL hash, so
// #docs, and #docs-<section> for one section, can be linked and survive a reload.
//
// The documentation shows one section at a time, like pages: the menu on the left picks it, and each section ends
// with links to the previous and next one. A hash pointing inside a section (for example #faq-kafka) opens that
// section and scrolls to the spot.

export function viewFor(hash) {
    return hash.startsWith('#docs') || hash.startsWith('#faq-') ? 'docs' : 'simulator';
}

/** The documentation section to show for a hash: the section itself, the one containing the target, or the first. */
export function docsSectionFor(hash, sectionIds, containingSection = () => null) {
    const id = hash.startsWith('#') ? hash.slice(1) : hash;
    if (sectionIds.includes(id)) return id;
    const inside = id ? containingSection(id) : null;
    return inside && sectionIds.includes(inside) ? inside : sectionIds[0];
}

export class ViewSwitcher {
    constructor(doc = document) {
        this.doc = doc;
        this.sections = [...doc.querySelectorAll('.docs-content > section[id^="docs-"]')];
        this.addPageLinks();
        this.onChange = () => this.apply();
        window.addEventListener('hashchange', this.onChange);
        this.apply();
    }

    /** "Previous" and "Next" at the end of every section. */
    addPageLinks() {
        const title = section => section.querySelector('h2')?.textContent ?? section.id;
        this.sections.forEach((section, i) => {
            if (section.querySelector('.docs-pager')) return;
            const pager = this.doc.createElement('nav');
            pager.className = 'docs-pager';
            pager.setAttribute('aria-label', 'Documentation pages');
            const link = (target, label, className) => {
                const a = this.doc.createElement('a');
                a.href = '#' + target.id;
                a.className = className;
                const small = this.doc.createElement('span'); small.textContent = label;
                const strong = this.doc.createElement('strong'); strong.textContent = title(target);
                a.append(small, strong);
                return a;
            };
            if (i > 0) pager.append(link(this.sections[i - 1], 'Previous', 'previous'));
            if (i < this.sections.length - 1) pager.append(link(this.sections[i + 1], 'Next', 'next'));
            section.append(pager);
        });
    }

    apply() {
        const hash = window.location.hash;
        const view = viewFor(hash);
        for (const panel of this.doc.querySelectorAll('[data-view]')) panel.hidden = panel.dataset.view !== view;
        for (const tab of this.doc.querySelectorAll('[data-view-tab]')) {
            if (tab.dataset.viewTab === view) tab.setAttribute('aria-current', 'page');
            else tab.removeAttribute('aria-current');
        }
        if (view !== 'docs') return;
        const ids = this.sections.map(s => s.id);
        const containing = id => this.doc.getElementById(id)?.closest('.docs-content > section[id^="docs-"]')?.id ?? null;
        const current = docsSectionFor(hash, ids, containing);
        for (const section of this.sections) section.hidden = section.id !== current;
        for (const link of this.doc.querySelectorAll('.docs-nav a')) {
            if (link.getAttribute('href') === '#' + current) link.setAttribute('aria-current', 'location');
            else link.removeAttribute('aria-current');
        }
        // A hash naming an FAQ part opens that part's tab.
        const id = hash.slice(1);
        this.doc.querySelector(`.faq-tabs [data-tab="${CSS.escape(id)}"]`)?.click();
        // Scroll to a spot inside the section, or to the top of a newly opened one.
        const target = id && id !== current && id !== 'docs' ? this.doc.getElementById(id) : null;
        if (target && !target.closest('[hidden]')) target.scrollIntoView();
        else window.scrollTo(0, 0);
    }

    destroy() { window.removeEventListener('hashchange', this.onChange); }
}
