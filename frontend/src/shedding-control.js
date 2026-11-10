// "Load shedding" control: when on, the backend works on at most N submissions at once and turns the rest away
// immediately with 503 + Retry-After ("busy, nothing stored"). The generator counts those as Blocked.
// Backend: GET/PUT /api/simulation/shedding { enabled, maxConcurrent }.

const ENDPOINT = '/api/simulation/shedding';
const REFRESH_MS = 3000;

/** The status line for a state from the backend, or null when unreachable. */
export function describeShedding(state) {
    if (!state) return 'Could not read the backend\'s state. If it was started before this setting existed, restart it.';
    const turnedAway = `${state.shed.toLocaleString()} turned away since the backend started`;
    return state.enabled
        ? `On: at most ${state.maxConcurrent.toLocaleString()} submissions at once, ${state.inFlight.toLocaleString()} now. ${turnedAway}.`
        : `Off: every submission is accepted for processing. ${turnedAway}.`;
}

export class SheddingControl {
    constructor(root) {
        this.checkbox = root.querySelector('[data-shedding-toggle]');
        this.limit = root.querySelector('[data-shedding-limit]');
        this.status = root.querySelector('[data-shedding-status]');
        this.busy = false;
        this.onChange = () => this.apply();
        this.checkbox.addEventListener('change', this.onChange);
        this.limit.addEventListener('change', this.onChange);
        this.refresh(true);
        this.timer = setInterval(() => { if (!this.busy) this.refresh(false); }, REFRESH_MS);
    }

    async request(options) {
        try {
            const response = await fetch(ENDPOINT, { ...options, signal: AbortSignal.timeout(3000) });
            return response.ok ? await response.json() : null;
        } catch {
            return null;
        }
    }

    async refresh(initial) {
        const state = await this.request();
        this.show(state, initial);
    }

    async apply() {
        const limit = Number(this.limit.value);
        if (!Number.isInteger(limit) || limit < 1) { this.status.textContent = 'The limit must be a whole number of at least 1.'; return; }
        this.busy = true;
        const state = await this.request({ method: 'PUT', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ enabled: this.checkbox.checked, maxConcurrent: limit }) });
        this.show(state, true);
        if (!state) this.status.textContent = 'Could not change it: the backend did not accept the setting. Restart it if it was started before this setting existed.';
        this.busy = false;
    }

    show(state, syncInputs) {
        if (state && syncInputs) {
            this.checkbox.checked = state.enabled;
            this.limit.value = state.maxConcurrent;
        }
        this.status.textContent = describeShedding(state);
    }

    destroy() {
        clearInterval(this.timer);
        this.checkbox.removeEventListener('change', this.onChange);
        this.limit.removeEventListener('change', this.onChange);
    }
}
