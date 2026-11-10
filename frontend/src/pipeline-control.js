// "Downstream processing" toggle: pauses or resumes everything after intake, in both apps at once.
//   Backend:   PUT /api/simulation/pipeline (case writer, outbox relay, result writer)
//   Processor: PUT /api/processor/pipeline  (proxied to the processor's /api/pipeline)
// Submissions keep being accepted into Kafka while paused, and are caught up on resume.

const ENDPOINTS = { backend: '/api/simulation/pipeline', processor: '/api/processor/pipeline' };
const REFRESH_MS = 5000;

/** What the toggle shows for the two apps' states; a state is { paused } or null when unreachable. */
export function describePipeline(backend, processor) {
    if (!backend || !processor) {
        const missing = [!backend && 'backend', !processor && 'processor'].filter(Boolean).join(' and ');
        return { checked: !(backend?.paused || processor?.paused), known: false,
            text: `Could not read the ${missing}'s state.` };
    }
    if (!backend.paused && !processor.paused) {
        return { checked: true, known: true, text: 'Running: submissions are saved, relayed and processed.' };
    }
    if (backend.paused && processor.paused) {
        return { checked: false, known: true,
            text: 'Paused: submissions are still accepted into Kafka, but nothing is saved or processed until resumed.' };
    }
    return { checked: false, known: true,
        text: `Partly paused: backend ${backend.paused ? 'paused' : 'running'}, processor ${processor.paused ? 'paused' : 'running'}.` };
}

async function read(url, options) {
    try {
        const response = await fetch(url, { ...options, signal: AbortSignal.timeout(3000) });
        return response.ok ? await response.json() : null;
    } catch {
        return null;
    }
}

export class PipelineToggle {
    constructor(root) {
        this.checkbox = root.querySelector('[data-pipeline-toggle]');
        this.status = root.querySelector('[data-pipeline-status]');
        this.busy = false;
        this.onChange = () => this.set(!this.checkbox.checked);
        this.checkbox.addEventListener('change', this.onChange);
        this.refresh();
        this.timer = setInterval(() => { if (!this.busy) this.refresh(); }, REFRESH_MS);
    }

    async refresh() {
        const [backend, processor] = await Promise.all([read(ENDPOINTS.backend), read(ENDPOINTS.processor)]);
        this.show(describePipeline(backend, processor));
    }

    async set(paused) {
        this.busy = true;
        this.checkbox.disabled = true;
        this.status.textContent = paused ? 'Pausing...' : 'Resuming...';
        const options = { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ paused }) };
        const [backend, processor] = await Promise.all([read(ENDPOINTS.backend, options), read(ENDPOINTS.processor, options)]);
        const state = describePipeline(backend, processor);
        if (!state.known) {
            const failed = [!backend && 'backend', !processor && 'processor'].filter(Boolean).join(' and ');
            state.text = `Could not ${paused ? 'pause' : 'resume'}: the ${failed} did not accept the change. ` +
                'If it was started before this toggle existed, restart it.';
        }
        this.show(state);
        this.checkbox.disabled = false;
        this.busy = false;
    }

    show(state) {
        this.checkbox.checked = state.checked;
        this.status.textContent = state.text;
        this.status.dataset.known = String(state.known);
    }

    destroy() {
        clearInterval(this.timer);
        this.checkbox.removeEventListener('change', this.onChange);
    }
}
