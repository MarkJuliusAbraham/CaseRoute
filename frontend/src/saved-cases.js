// Polls a run's pipeline counts from the backend: saved cases (and how many are still OPEN, ROUTED or
// FAILED) plus outbox rows not yet published. Saved trails accepted (the gap is still in Kafka), and
// processed trails saved (the gap is in the outbox, the cases.saved topic, or being processed).
const INTERVAL_MS = 1000;
const SETTLE_LIMIT_MS = 300_000;

export function settled(stats, accepted) {
    return stats != null && stats.saved >= accepted && stats.open === 0 && stats.awaitingRelay === 0;
}

export function shouldKeepPolling({ active, stats, accepted, finishedFor }) {
    if (active) return true;
    if (finishedFor >= SETTLE_LIMIT_MS) return false;
    return !settled(stats, accepted);
}

export class SavedCasesPoller {
    // onSettled(runId) fires once polling ends after a finished run: the pipeline settled, or 5 minutes passed.
    constructor(onStats, onSettled = () => {}) {
        this.onStats = onStats;
        this.onSettled = onSettled;
        this.runId = null;
        this.timer = null;
        this.stats = null;
    }

    // Called with every run snapshot.
    track(run) {
        if (run.runId !== this.runId) {
            this.stop();
            this.runId = run.runId;
            this.stats = null;
            this.finishedSeenAt = null;
        }
        this.active = run.finishedAt == null;
        this.accepted = run.accepted;
        // Connection tests store nothing: there are no saved cases to poll for.
        if (run.settings?.submissionMode === 'NOOP') { this.stop(); return; }
        if (!this.active && this.finishedSeenAt == null) this.finishedSeenAt = Date.now();
        if (this.timer == null && !this.polling) this.poll();
    }

    async poll() {
        const runId = this.runId;
        this.polling = true;
        try {
            const params = new URLSearchParams({ testRunId: runId });
            const response = await fetch('/api/simulation/cases/stats?' + params, { signal: AbortSignal.timeout(2000) });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const stats = await response.json();
            if (runId !== this.runId) return;
            this.stats = stats;
        } catch {
            if (runId !== this.runId) return;
            this.stats = null;
        } finally {
            this.polling = false;
        }
        this.onStats(runId, this.stats);
        const finishedFor = this.finishedSeenAt == null ? 0 : Date.now() - this.finishedSeenAt;
        if (shouldKeepPolling({ active: this.active, stats: this.stats, accepted: this.accepted, finishedFor })) {
            this.timer = setTimeout(() => { this.timer = null; this.poll(); }, INTERVAL_MS);
        } else if (!this.active) {
            this.onSettled(runId);
        }
    }

    stop() {
        clearTimeout(this.timer);
        this.timer = null;
    }
}
