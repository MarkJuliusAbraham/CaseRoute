// (i) buttons: each toggles the hint named by its aria-controls. One hint is open at a time; a click elsewhere or
// Escape closes it. Markup: <button class="hint" aria-expanded="false" aria-controls="id">i</button>
//                           <span id="id" class="hint-text" hidden>...</span>

export function nextOpen(current, clicked) {
    return current === clicked ? null : clicked;
}

export class Hints {
    constructor(root = document) {
        this.root = root;
        this.open = null;
        this.onClick = event => {
            const button = event.target.closest?.('.hint');
            if (button && this.root.contains(button)) {
                event.preventDefault();
                this.show(nextOpen(this.open, button));
            } else if (this.open && !event.target.closest?.('.hint-text')) {
                this.show(null);
            }
        };
        this.onKey = event => {
            if (event.key === 'Escape' && this.open) {
                const button = this.open;
                this.show(null);
                button.focus();
            }
        };
        this.root.addEventListener('click', this.onClick);
        this.root.addEventListener('keydown', this.onKey);
    }

    show(button) {
        if (this.open) {
            this.open.setAttribute('aria-expanded', 'false');
            const text = document.getElementById(this.open.getAttribute('aria-controls'));
            if (text) text.hidden = true;
        }
        this.open = button;
        if (button) {
            button.setAttribute('aria-expanded', 'true');
            const text = document.getElementById(button.getAttribute('aria-controls'));
            if (text) text.hidden = false;
        }
    }

    destroy() {
        this.root.removeEventListener('click', this.onClick);
        this.root.removeEventListener('keydown', this.onKey);
    }
}
