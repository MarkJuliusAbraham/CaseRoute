// The checker panel: compares what the generator was told about each submission with the cases the backend
// saved (POST /api/runs/{id}/check on the generator). It runs automatically once the pipeline settles after a
// run, and can be repeated at any time after the run ends.

export const VERDICT_LABELS = {
    CONSISTENT: 'Consistent',
    PENDING: 'Still processing',
    DISCREPANCIES: 'Discrepancies found',
};

const EXAMPLE_LABELS = {
    acceptedButMissing: 'Accepted but missing',
    unknownNotSaved: 'Unknown, never arrived',
    rejectedButSaved: 'Rejected but saved',
    unexpected: 'Unexpected',
};

/** The rows of the result table: [label, value, tone], where tone 'problem' marks a count that should be 0. */
export function checkRows(report) {
    const { accepted, unknown } = report;
    const problem = count => (count > 0 ? 'problem' : '');
    return [
        ['Accepted replies', accepted.total, ''],
        ['Saved and processed', accepted.processed, ''],
        ['Saved, awaiting processing', accepted.awaitingProcessing, ''],
        ['Accepted but missing', accepted.missing, problem(accepted.missing)],
        ['Outcome unknown', unknown.total, ''],
        ['Unknown, saved anyway', unknown.saved, ''],
        ['Unknown, never arrived', unknown.notSaved, ''],
        ['Rejected but saved', report.rejectedButSaved, problem(report.rejectedButSaved)],
        ['Unexpected cases', report.unexpected, problem(report.unexpected)],
    ];
}

export class CheckerView {
    constructor(root, api) {
        this.root = root;
        this.api = api;
        this.button = root.querySelector('[data-check-run]');
        this.verdict = root.querySelector('[data-check-verdict]');
        this.summary = root.querySelector('[data-check-summary]');
        this.table = root.querySelector('[data-check-results]');
        this.examples = root.querySelector('[data-check-examples]');
        this.runId = null;
        this.finished = false;
        this.busy = false;
        this.onClick = () => this.check();
        this.button.addEventListener('click', this.onClick);
    }

    /** Called with every run snapshot. */
    setRun(run) {
        if (run.runId !== this.runId) {
            this.runId = run.runId;
            this.report = null;
            this.autoChecked = false;
            this.clear('Not checked', 'Runs automatically once processing settles after the run.');
            this.loadLatest(run.runId);
        }
        this.finished = run.finishedAt != null;
        this.updateButton();
    }

    /** Called when the pipeline has settled (or polling gave up) after the run: check once, automatically. */
    autoCheck(runId) {
        if (runId !== this.runId || !this.finished || this.report || this.autoChecked || this.busy) return;
        this.autoChecked = true;
        this.check();
    }

    async loadLatest(runId) {
        try {
            const report = await this.api.latestCheck(runId);
            if (report && runId === this.runId && !this.report) this.show(report);
        } catch { /* No previous check, or the generator is unavailable: the panel stays "Not checked". */ }
    }

    async check() {
        const runId = this.runId;
        if (!runId || !this.finished || this.busy) return;
        this.busy = true;
        this.updateButton();
        this.verdict.textContent = 'Checking...';
        this.verdict.dataset.verdict = '';
        try {
            const report = await this.api.check(runId);
            if (runId === this.runId) this.show(report);
        } catch (error) {
            if (runId === this.runId) this.clear('Check failed', error.message);
        } finally {
            this.busy = false;
            this.updateButton();
        }
    }

    show(report) {
        this.report = report;
        this.verdict.textContent = VERDICT_LABELS[report.verdict] ?? report.verdict;
        this.verdict.dataset.verdict = report.verdict;
        const checkedAt = new Date(report.checkedAt).toLocaleTimeString();
        this.summary.textContent = `${report.summary} Checked at ${checkedAt} against ${report.casesInDatabase.toLocaleString()} saved cases.`;
        this.table.replaceChildren(...checkRows(report).map(([label, value, tone]) => {
            const item = document.createElement('div');
            const term = document.createElement('dt');
            const detail = document.createElement('dd');
            term.textContent = label;
            detail.textContent = value.toLocaleString();
            if (tone) detail.className = tone;
            item.append(term, detail);
            return item;
        }));
        this.table.hidden = false;
        const entries = Object.entries(report.examples ?? {}).filter(([, ids]) => ids.length);
        this.examples.hidden = entries.length === 0;
        const list = this.examples.querySelector('ul');
        list.replaceChildren(...entries.flatMap(([category, ids]) => ids.map(id => {
            const item = document.createElement('li');
            item.textContent = `${EXAMPLE_LABELS[category] ?? category}: ${id}`;
            return item;
        })));
    }

    clear(verdict, summary) {
        this.verdict.textContent = verdict;
        this.verdict.dataset.verdict = '';
        this.summary.textContent = summary;
        this.table.hidden = true;
        this.examples.hidden = true;
    }

    updateButton() {
        this.button.disabled = !this.finished || this.busy;
        this.button.textContent = this.report ? 'Check again' : 'Check run';
    }

    destroy() { this.button.removeEventListener('click', this.onClick); }
}
