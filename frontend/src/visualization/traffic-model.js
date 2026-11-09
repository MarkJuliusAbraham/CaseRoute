// Snapshot-derived telemetry. Particle timing is never treated as request latency.
export class TrafficModel {
    constructor() { this.reset(); }

    reset() {
        this.runId = null;
        this.previous = null;
        this.samples = [];
        this.lastUpdate = null;
        this.startedObserving = null;
        this.connected = true;
        this.snapshot = null;
    }

    update(snapshot, now) {
        if (snapshot.runId !== this.runId) {
            this.reset();
            this.runId = snapshot.runId;
        }
        const elapsed = this.lastUpdate === null ? 0 : now - this.lastUpdate;
        const delta = this.previous === null ? 0 : Math.max(0, snapshot.dispatched - this.previous);
        // Reconnection gaps and counter resets establish a new baseline; don't animate history.
        const continuous = this.connected && elapsed <= 2500 && this.previous !== null && snapshot.dispatched >= this.previous;
        const count = continuous ? delta : 0;
        if (!continuous) { this.startedObserving = now; this.samples = []; }
        if (continuous) this.samples.push({ at: now, count });
        this.samples = this.samples.filter(sample => now - sample.at < 60_000);
        this.previous = snapshot.dispatched;
        this.lastUpdate = now;
        this.snapshot = snapshot;
        this.connected = true;
        return { count, particles: Math.min(count, 80) };
    }

    disconnect() { this.connected = false; }
    rebase() { this.previous = null; this.samples = []; this.startedObserving = null; }
    fresh(now) {
        return this.connected && this.lastUpdate !== null && now - this.lastUpdate <= 2500;
    }
    rate(now) {
        if (!this.fresh(now) || this.startedObserving === null) return null;
        const elapsed = Math.min(10_000, now - this.startedObserving);
        if (elapsed < 500) return null;
        const count = this.samples.filter(s => now - s.at < 10_000).reduce((n, s) => n + s.count, 0);
        return count * 60_000 / elapsed;
    }
    buckets(now) {
        const buckets = Array(60).fill(0);
        for (const sample of this.samples) {
            const age = Math.floor((now - sample.at) / 1000);
            if (age >= 0 && age < 60) buckets[59 - age] += sample.count;
        }
        return buckets;
    }
}

// Turns a cumulative counter into per-observation increases. Gaps, reconnects and resets
// establish a new baseline instead of replaying history as one burst.
export class DeltaTracker {
    constructor() { this.reset(); }
    reset() { this.previous = null; this.lastUpdate = null; this.connected = true; }
    update(value, now) {
        const elapsed = this.lastUpdate === null ? Infinity : now - this.lastUpdate;
        const continuous = this.connected && this.previous !== null && elapsed <= 2500 && value >= this.previous;
        const count = continuous ? value - this.previous : 0;
        this.previous = value;
        this.lastUpdate = now;
        this.connected = true;
        return count;
    }
    disconnect() { this.connected = false; }
}

// Messages accepted into Kafka but not yet saved. Approximate: a timed-out request may also have
// reached Kafka, so this can briefly undercount; it never goes below zero.
export function kafkaBacklog(accepted, saved) {
    if (saved == null) return null;
    return Math.max(0, accepted - saved);
}
