// Kafka panel: the backend's intake producer settings (batching, compression), the topics' partition counts, and
// the intake producer's live metrics. Settings can only be changed between runs; the backend refuses them while
// submissions are being sent, and the panel locks them while a run is active.
// Backend: GET /api/simulation/kafka, PUT .../producer, PUT .../partitions, GET .../metrics.

const BASE = '/api/simulation/kafka';
const STATE_MS = 5000;
const METRICS_MS = 1000;

/** Metric name -> [label, how to format the value]. Times are milliseconds, sizes bytes. */
const METRICS = [
    ['record-send-rate', 'Sent / s', v => Math.round(v).toLocaleString()],
    ['record-queue-time-avg', 'Waiting in producer (avg)', ms],
    ['record-queue-time-max', 'Waiting in producer (max)', ms],
    ['request-latency-avg', 'Broker confirmation (avg)', ms],
    ['request-latency-max', 'Broker confirmation (max)', ms],
    ['requests-in-flight', 'Requests in flight (all producers)', v => Math.round(v).toLocaleString()],
    ['records-per-request-avg', 'Records per request', v => v.toFixed(1)],
    ['batch-size-avg', 'Batch size (avg)', bytes],
    ['compression-rate-avg', 'Compressed to', v => `${Math.round(v * 100)}%`],
    ['bufferpool-wait-ratio', 'Waiting for buffer', v => `${(v * 100).toFixed(1)}% of time`],
    ['waiting-threads', 'Threads waiting for buffer', v => Math.round(v).toLocaleString()],
];

function ms(v) { return `${v.toFixed(1)} ms`; }
function bytes(v) { return v >= 1024 ? `${(v / 1024).toFixed(1)} KB` : `${Math.round(v)} B`; }

/** [label, text] pairs for the metrics from the backend; '—' where Kafka has no value yet. */
export function metricRows(metrics) {
    return METRICS.map(([name, label, format]) => {
        const value = metrics?.[name];
        return [label, typeof value === 'number' && Number.isFinite(value) ? format(value) : '—'];
    });
}

/** The producer settings as the backend expects them, from the form's values; or an error message. */
export function readProducerForm({ lingerMs, batchSizeKb, compression, maxInFlight, producers }) {
    const linger = Number(lingerMs), batchKb = Number(batchSizeKb), inFlight = Number(maxInFlight), count = Number(producers);
    if (!Number.isInteger(linger) || linger < 0 || linger > 1000) return { error: 'Linger must be 0 to 1,000 ms.' };
    if (!Number.isInteger(batchKb) || batchKb < 1 || batchKb > 4096) return { error: 'Batch size must be 1 to 4,096 KB.' };
    if (!Number.isInteger(inFlight) || inFlight < 1 || inFlight > 5) return { error: 'Requests in flight must be 1 to 5.' };
    if (!Number.isInteger(count) || count < 1 || count > 8) return { error: 'Producers must be 1 to 8.' };
    return { settings: { lingerMs: linger, batchSizeBytes: batchKb * 1024, compression, maxInFlight: inFlight, producers: count } };
}

async function call(path, options = {}) {
    try {
        const response = await fetch(BASE + path, { ...options, signal: AbortSignal.timeout(15000) });
        const body = await response.json().catch(() => null);
        if (response.ok) return { body };
        // A backend started before this panel existed has no such endpoint; its security rules answer 403.
        if ((response.status === 403 || response.status === 404) && !body?.detail)
            return { error: 'The backend does not offer these settings yet: restart it to load them.' };
        return { error: body?.detail || body?.message || `HTTP ${response.status}` };
    } catch {
        return { error: 'Could not reach the backend. If it was started before this panel existed, restart it.' };
    }
}

export class KafkaPanel {
    constructor(root) {
        this.root = root;
        this.form = root.querySelector('[data-kafka-producer]');
        this.fields = {
            lingerMs: root.querySelector('[name="lingerMs"]'),
            batchSizeKb: root.querySelector('[name="batchSizeKb"]'),
            compression: root.querySelector('[name="compression"]'),
            maxInFlight: root.querySelector('[name="maxInFlight"]'),
            producers: root.querySelector('[name="producers"]'),
        };
        this.producerStatus = root.querySelector('[data-kafka-producer-status]');
        this.partitionRows = root.querySelector('[data-kafka-partitions]');
        this.partitionStatus = root.querySelector('[data-kafka-partition-status]');
        this.metricsList = root.querySelector('[data-kafka-metrics]');
        this.locked = false;
        this.busy = false;
        this.onSubmit = event => { event.preventDefault(); this.applyProducer(); };
        this.form.addEventListener('submit', this.onSubmit);
        this.refresh(true);
        this.stateTimer = setInterval(() => { if (!this.busy && this.visible()) this.refresh(false); }, STATE_MS);
        this.metricsTimer = setInterval(() => { if (this.visible()) this.refreshMetrics(); }, METRICS_MS);
    }

    /** The card is hidden in Simple view unless its dialog is open: no polling then. */
    visible() { return this.root.offsetParent !== null || this.root.closest('dialog[open]') != null; }

    /** Locks every control while a run is active: these settings apply between runs only. */
    setLocked(locked) {
        this.locked = locked;
        for (const control of this.root.querySelectorAll('input, select, button:not(.hint)')) control.disabled = locked || this.busy;
        this.root.querySelector('[data-kafka-lock]').hidden = !locked;
    }

    async refresh(syncForm) {
        const { body, error } = await call('');
        if (error) { this.producerStatus.textContent = error; return; }
        this.show(body, syncForm);
    }

    async refreshMetrics() {
        const { body } = await call('/metrics');
        this.metricsList.replaceChildren();
        for (const [label, text] of metricRows(body)) {
            const row = document.createElement('div');
            const dt = document.createElement('dt'); dt.textContent = label;
            const dd = document.createElement('dd'); dd.textContent = text;
            row.append(dt, dd);
            this.metricsList.append(row);
        }
    }

    show(state, syncForm) {
        const p = state.producer;
        if (syncForm) {
            this.fields.lingerMs.value = p.lingerMs;
            this.fields.batchSizeKb.value = Math.round(p.batchSizeBytes / 1024);
            this.fields.compression.value = p.compression;
            this.fields.maxInFlight.value = p.maxInFlight;
            this.fields.producers.value = p.producers ?? 1;
        }
        const count = p.producers ?? 1;
        this.producerStatus.textContent = `In use: ${count} producer${count === 1 ? '' : 's'}, linger ${p.lingerMs} ms, batch ${Math.round(p.batchSizeBytes / 1024)} KB, `
            + `${p.compression === 'none' ? 'no compression' : p.compression}, ${p.maxInFlight} in flight each. Resets when the backend restarts.`;
        this.renderPartitions(state);
    }

    renderPartitions(state) {
        this.partitionRows.replaceChildren();
        const entries = Object.entries(state.partitions ?? {});
        if (!entries.length) { this.partitionStatus.textContent = 'Could not read the topics from Kafka.'; return; }
        for (const [topic, count] of entries) {
            const row = document.createElement('div');
            row.className = 'partition-row';
            const name = document.createElement('code'); name.textContent = topic;
            const current = document.createElement('span'); current.textContent = `${count}`;
            row.append(name, current);
            if (topic === 'cases.warmup') {
                const note = document.createElement('span'); note.className = 'status-line'; note.textContent = 'follows cases.submitted';
                row.append(note);
            } else {
                const input = document.createElement('input');
                input.type = 'number'; input.min = String(count + 1); input.max = '1000'; input.value = String(count + 1);
                input.setAttribute('aria-label', `New partition count for ${topic}`);
                const add = document.createElement('button');
                add.type = 'button'; add.className = 'secondary'; add.textContent = 'Add';
                add.addEventListener('click', () => this.addPartitions(topic, Number(input.value), count));
                row.append(input, add);
            }
            this.partitionRows.append(row);
        }
        this.setLocked(this.locked);
    }

    async applyProducer() {
        if (this.locked) return;
        const { settings, error } = readProducerForm(Object.fromEntries(Object.entries(this.fields).map(([k, f]) => [k, f.value])));
        if (error) { this.producerStatus.textContent = error; return; }
        this.busy = true; this.setLocked(this.locked);
        this.producerStatus.textContent = 'Replacing the producer...';
        const result = await call('/producer', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(settings) });
        this.busy = false; this.setLocked(this.locked);
        if (result.error) this.producerStatus.textContent = `Not changed: ${result.error}`;
        else this.show(result.body, true);
    }

    async addPartitions(topic, partitions, current) {
        if (this.locked) return;
        if (!Number.isInteger(partitions) || partitions <= current) { this.partitionStatus.textContent = `${topic} has ${current}; enter a larger number. Partitions can only be added.`; return; }
        if (!window.confirm(`Raise ${topic} from ${current} to ${partitions} partitions? Kafka cannot remove partitions again.`)) return;
        this.busy = true; this.setLocked(this.locked);
        this.partitionStatus.textContent = 'Adding partitions...';
        const result = await call('/partitions', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ topic, partitions }) });
        this.busy = false;
        if (result.error) { this.partitionStatus.textContent = `Not changed: ${result.error}`; this.setLocked(this.locked); return; }
        this.partitionStatus.textContent = `${topic} now has ${result.body.partitions[topic]} partitions. Producers and consumers start using them within about 10 s.`;
        this.show(result.body, false);
    }

    destroy() {
        clearInterval(this.stateTimer);
        clearInterval(this.metricsTimer);
        this.form.removeEventListener('submit', this.onSubmit);
    }
}
