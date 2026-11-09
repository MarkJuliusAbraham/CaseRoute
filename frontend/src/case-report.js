export class CaseReportView {
    constructor(root) {
        this.root = root;
        this.page = 0;
        this.totalPages = 0;
        this.runId = null;
        this.busy = false;
        this.onFilter = () => { this.page = 0; this.load(); };
        this.onRefresh = () => this.load();
        this.onPrevious = () => { if (this.page > 0) { this.page--; this.load(); } };
        this.onNext = () => { if (this.page + 1 < this.totalPages) { this.page++; this.load(); } };
        this.filter = root.querySelector('select');
        this.filter.addEventListener('change', this.onFilter);
        this.refresh = root.querySelector('[data-report-refresh]');
        this.previous = root.querySelector('[data-report-previous]');
        this.next = root.querySelector('[data-report-next]');
        this.refresh.addEventListener('click', this.onRefresh);
        this.previous.addEventListener('click', this.onPrevious);
        this.next.addEventListener('click', this.onNext);
        this.updateControls();
    }

    setRunId(id) {
        if (id === this.runId) return;
        this.runId = id;
        this.filter.querySelector('[value="current"]').disabled = !id;
        if (this.filter.value === 'current' && !this.root.hidden) { this.page = 0; this.load(); }
    }

    query() {
        const params = new URLSearchParams();
        if (this.filter.value === 'current' && this.runId) params.set('testRunId', this.runId);
        return params;
    }

    updateControls() {
        this.previous.disabled = this.busy || this.page === 0;
        this.next.disabled = this.busy || this.page + 1 >= this.totalPages;
        this.refresh.disabled = this.busy;
        this.root.querySelector('[data-report-export]').href = '/api/simulation/cases/export?' + this.query();
    }

    async load() {
        this.abort?.abort();
        this.abort = new AbortController();
        const controller = this.abort;
        this.busy = true; this.updateControls();
        const status = this.root.querySelector('[data-report-status]');
        status.textContent = 'Reading saved cases from the backend...';
        const body = this.root.querySelector('tbody');
        body.replaceChildren();
        try {
            const params = this.query(); params.set('page', this.page); params.set('size', 25);
            const response = await fetch('/api/simulation/cases?' + params, {
                cache: 'no-store', signal: AbortSignal.any([controller.signal, AbortSignal.timeout(15000)]),
            });
            if (!response.ok) throw new Error(`Backend report unavailable (HTTP ${response.status}). Check the backend connection and restart it after updates.`);
            const report = await response.json();
            this.totalPages = report.totalPages;
            if (this.page >= this.totalPages && this.page > 0) { this.page = Math.max(0, this.totalPages - 1); return this.load(); }
            for (const item of report.cases) {
                const row = body.insertRow();
                for (const value of [item.id, item.clientRequestId ?? 'Legacy case', item.subject,
                        item.status, new Date(item.createdAt).toLocaleString()]) {
                    const cell = row.insertCell(); cell.textContent = value; cell.title = value;
                }
            }
            if (!report.cases.length) {
                const cell = body.insertRow().insertCell();cell.colSpan = 5;cell.className = 'empty';
                cell.textContent = 'No saved cases match this filter. Older cases have no structured test-run ID; try All cases.';
            }
            status.textContent = `${report.totalElements.toLocaleString()} saved cases · Page ${report.totalPages ? this.page + 1 : 0} of ${report.totalPages}. Refresh to see new arrivals.`;
        } catch (error) {
            if (controller.signal.aborted) return;
            status.textContent = error.message;
        } finally {
            if (this.abort === controller) { this.busy = false; this.updateControls(); }
        }
    }

    destroy() {
        this.abort?.abort();
        this.filter.removeEventListener('change', this.onFilter);
        this.refresh.removeEventListener('click', this.onRefresh);
        this.previous.removeEventListener('click', this.onPrevious);
        this.next.removeEventListener('click', this.onNext);
    }
}
