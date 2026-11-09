export class HealthHistoryView {
    constructor(root, history) {
        this.root = root;
        this.history = history;
        this.service = root.querySelector('[data-history-service]');
        this.changes = root.querySelector('[data-history-changes]');
        this.onFilter = () => this.render();
        this.onExport = () => {
            const blob = new Blob([JSON.stringify({
                source: 'Browser-observed health checks; no monitoring while the dashboard is closed.',
                exportedAt: new Date().toISOString(),
                observations: history.select(this.service.value, this.changes.checked),
            }, null, 2)], {type: 'application/json'});
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a'); link.href = url; link.download = 'caseroute-health-history.json';link.click();
            setTimeout(() => URL.revokeObjectURL(url), 1000);
        };
        this.service.addEventListener('change', this.onFilter);
        this.changes.addEventListener('change', this.onFilter);
        this.exportButton = root.querySelector('[data-history-export]');
        this.exportButton.addEventListener('click', this.onExport);
    }

    record(observation) {
        this.history.add(observation);
        if (!this.root.hidden) this.render();
    }

    render() {
        const records = this.history.select(this.service.value, this.changes.checked);
        const body = this.root.querySelector('tbody'); body.replaceChildren();
        for (const record of records.slice(0, 100)) {
            const row = body.insertRow();
            const values = [new Date(record.at).toLocaleString(),
                record.service === 'generator' ? 'Generator' : 'Backend + database',
                record.live ? 'Live' : 'Offline / unhealthy', `${Math.round(record.durationMs)} ms`,
                record.httpStatus ?? 'No response', record.transition,
                record.detail + (record.gapMs > 10000 ? ' · Observation gap; status between checks is unknown.' : '')];
            values.forEach((value, index) => {
                const cell = row.insertCell();cell.textContent = value;
                if (index === 2) cell.className = record.live ? 'accepted' : 'problem';
            });
        }
        if (!records.length) {
            const cell = body.insertRow().insertCell();cell.colSpan = 7;cell.className = 'empty';cell.textContent = 'No health checks match this filter yet.';
        }
        this.root.querySelector('[data-history-summary]').textContent = `${records.length} matching checks retained · Showing latest ${Math.min(100,records.length)}. `
            + (this.history.persistent ? 'Stored in this browser.' : 'Browser storage unavailable; history is in memory only.');
        for (const service of ['generator','backend']) {
            const strip = this.root.querySelector(`[data-health-strip="${service}"]`);strip.replaceChildren();
            const observations = this.history.select(service).slice(0, 60).toReversed();
            for (const record of observations) {
                const dot = document.createElement('span');dot.className = record.live ? 'check-live' : 'check-offline';
                dot.title = `${new Date(record.at).toLocaleString()} · ${record.live ? 'Live' : 'Offline / unhealthy'} · ${Math.round(record.durationMs)} ms`;
                strip.append(dot);
            }
            strip.setAttribute('aria-label', `${service}: latest ${observations.length} checks, ${observations.filter(record=>!record.live).length} failed`);
        }
    }

    destroy() {
        this.service.removeEventListener('change', this.onFilter);
        this.changes.removeEventListener('change', this.onFilter);
        this.exportButton.removeEventListener('click', this.onExport);
    }
}
