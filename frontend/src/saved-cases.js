// Polls how many cases of a run are saved in PostgreSQL. In Kafka mode this trails the
// accepted count; the difference is what is still waiting in Kafka.
const INTERVAL_MS = 1000;
const SETTLE_LIMIT_MS = 120_000;

export function shouldKeepPolling({ active, saved, accepted, finishedFor }) {
    if (active) return true;
    if (saved == null) return finishedFor < SETTLE_LIMIT_MS;
    return saved < accepted && finishedFor < SETTLE_LIMIT_MS;
}

export class SavedCasesPoller {
    constructor(onCount) {
        this.onCount = onCount;
        this.runId = null;
        this.timer = null;
        this.saved = null;
    }

    // Called with every run snapshot.
    track(run) {
        if (run.runId !== this.runId) {
            this.stop();
            this.runId = run.runId;
            this.saved = null;
            this.finishedSeenAt = null;
        }
        this.active = run.finishedAt == null;
        this.accepted = run.accepted;
        if (!this.active && this.finishedSeenAt == null) this.finishedSeenAt = Date.now();
        if (this.timer == null && !this.polling) this.poll();
    }

    async poll() {
        const runId = this.runId;
        this.polling = true;
        try {
            const params = new URLSearchParams({ testRunId: runId, size: '1' });
            const response = await fetch('/api/simulation/cases?' + params, { signal: AbortSignal.timeout(2000) });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const page = await response.json();
            if (runId !== this.runId) return;
            this.saved = page.totalElements;
        } catch {
            if (runId !== this.runId) return;
            this.saved = null;
        } finally {
            this.polling = false;
        }
        this.onCount(runId, this.saved);
        const finishedFor = this.finishedSeenAt == null ? 0 : Date.now() - this.finishedSeenAt;
        if (shouldKeepPolling({ active: this.active, saved: this.saved, accepted: this.accepted, finishedFor })) {
            this.timer = setTimeout(() => { this.timer = null; this.poll(); }, INTERVAL_MS);
        }
    }

    stop() {
        clearTimeout(this.timer);
        this.timer = null;
    }
}
