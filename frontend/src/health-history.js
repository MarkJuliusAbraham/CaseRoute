const KEY = 'caseroute.health-history.v1';
const LIMIT = 1000;
const RETENTION_MS = 24 * 60 * 60 * 1000;

export class HealthHistory {
    constructor(storage = null) {
        this.storage = storage;
        this.records = [];
        this.persistent = !!storage;
        try {
            const saved = JSON.parse(storage?.getItem(KEY) || '[]');
            if (Array.isArray(saved)) this.records = saved.filter(value =>
                ['generator', 'backend'].includes(value.service) && typeof value.live === 'boolean'
                && Number.isFinite(value.at) && Number.isFinite(value.durationMs)).slice(-LIMIT);
        } catch { this.persistent = false; }
        this.prune(Date.now());
    }

    prune(now) {
        this.records = this.records.filter(record => now - record.at <= RETENTION_MS && record.at <= now).slice(-LIMIT);
    }

    add(observation) {
        const previous = this.records.findLast(record => record.service === observation.service);
        const record = {
            ...observation,
            transition: !previous ? 'Initial observation' : previous.live === observation.live
                ? 'Unchanged' : observation.live ? 'Recovered' : 'Became unreachable',
            gapMs: previous ? Math.max(0, observation.at - previous.at) : null,
        };
        this.records.push(record);
        this.prune(observation.at);
        try { this.storage?.setItem(KEY, JSON.stringify(this.records)); }
        catch { this.persistent = false; }
    }

    select(service = 'all', changesOnly = false) {
        return this.records.filter(record => (service === 'all' || record.service === service)
                && (!changesOnly || record.transition !== 'Unchanged')).toReversed();
    }
}
