// Connectivity is independent of a running load test or an SSE subscription.
export class Connections {
    constructor(root, onChange, onCheck = () => {}) {
        this.root = root;
        this.onChange = onChange;
        this.onCheck = onCheck;
        this.health = { generator: false, backend: false, processor: false };
        this.stopped = false;
        this.controller = new AbortController();
        this.poll();
    }

    async check(name, url) {
        const indicator = this.root.querySelector(`[data-service="${name}"]`);
        let live = false;
        let httpStatus = null;
        let detail = "";
        const started = performance.now();
        try {
            const response = await fetch(url, {
                cache: 'no-store',
                signal: AbortSignal.any([this.controller.signal, AbortSignal.timeout(2000)]),
            });
            httpStatus = response.status;
            if (response.ok) {
                live = (await response.json()).status === 'UP';
                detail = live ? 'Health check passed' : 'Service reported an unhealthy status';
            } else detail = `HTTP ${response.status}`;
        } catch (error) {
            detail = error.name === 'TimeoutError' ? 'Health check timed out after 2 seconds'
                : httpStatus !== null ? 'Invalid health response' : 'Connection failed or was interrupted';
        }
        if (this.stopped) return;
        this.onCheck({ service: name, at: Date.now(), live, httpStatus,
            durationMs: performance.now() - started, detail });
        this.health[name] = live;
        indicator.dataset.live = String(live);
        const label = indicator.querySelector('[data-health-label]');
        const text = live ? 'Live' : 'Offline / unreachable';
        if (label.textContent !== text) label.textContent = text;
        indicator.title = live ? 'Health check succeeded' : 'Health check failed or timed out; checked every 3 seconds';
    }

    async poll() {
        await Promise.all([
            this.check('generator', '/api/runs/health'),
            this.check('backend', '/api/backend/health'),
            this.check('processor', '/api/processor/health'),
        ]);
        if (this.stopped) return;
        this.onChange(this.health);
        this.timer = setTimeout(() => this.poll(), 3000);
    }

    destroy() {
        this.stopped = true;
        clearTimeout(this.timer);
        this.controller.abort();
    }
}
