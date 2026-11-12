// The run's results, in the order things happen: what became of every planned submission (one bar), then what the run
// itself did, what the retry period after it did, and the connections. All numbers come from the generator's snapshot.

const fmt = n => (n ?? 0).toLocaleString();
const pct = (n, total) => (total > 0 ? `${((n / total) * 100).toFixed(n > 0 && n / total < 0.001 ? 2 : 1)}%` : '0%');

/** Submissions the retry period sent, over all its passes. */
export function retrySent(run) {
    return (run?.retry?.passes ?? []).reduce((sum, pass) => sum + pass.sent, 0);
}

/** Every request the generator sent for this run's submissions: during the run, repeats, and the retry period. */
export function totalSent(run) {
    return (run?.dispatched ?? 0) + (run?.duplicates?.sent ?? 0) + retrySent(run);
}

/**
 * What became of every planned submission, as parts that add up to the plan: accepted, still without a definite
 * answer (skipped, blocked, unknown), rejected, not sent (stopped early), and pending (in flight or not yet due).
 */
export function outcomeSummary(run) {
    const total = run?.planned ?? 0;
    const accepted = run?.accepted ?? 0;
    const skipped = run?.skipped ?? 0, blocked = run?.blocked ?? 0, unknown = run?.unknown ?? 0;
    const unresolved = skipped + blocked + unknown;
    const rejected = run?.failed ?? 0;
    const notSent = run?.notSent ?? 0;
    const pending = Math.max(0, total - accepted - unresolved - rejected - notSent);
    return { total, accepted, unresolved, skipped, blocked, unknown, rejected, notSent, pending };
}

/** What the run itself did, before any retry period. */
export function duringRun(run) {
    const recoveredLater = run?.retry?.recovered ?? 0;
    return {
        sent: run?.dispatched ?? 0,
        accepted: Math.max(0, (run?.accepted ?? 0) - recoveredLater),
        retriesOn: Boolean(run?.settings?.retryEnabled),
        retried: run?.retried ?? 0,
        recovered: run?.recovered ?? 0,
        extraRequests: run?.retryRequests ?? 0,
        circuit: run?.circuit ?? null,
        duplicatesOn: (run?.settings?.duplicatePercent ?? 0) > 0,
        duplicatesSent: run?.duplicates?.sent ?? 0,
        duplicatesAccepted: run?.duplicates?.accepted ?? 0,
    };
}

const CIRCUIT_WORDS = { CLOSED: 'Yes', OPEN: 'No: sending paused', PROBING: 'Checking (probe sent)', RECOVERING: 'Back: ramping up' };

/** Whether the generator currently reaches the backend, in words; null when the circuit breaker is off. */
export function backendReachable(circuit) {
    if (!circuit) return null;
    const words = CIRCUIT_WORDS[circuit.state] ?? circuit.state;
    return circuit.state === 'CLOSED' ? words : `${words} since ${new Date(circuit.since).toLocaleTimeString()}`;
}

/** The retry period's state in words, for its card. */
export function retryPeriodStatus(run) {
    const r = run?.retry;
    const on = Boolean(run?.settings?.retryPeriod);
    if (!r) {
        if (run && run.finishedAt == null) return on ? `On: starts when the run ends, for up to ${run.settings.retryPeriodSeconds} s.` : 'Off for this run. Retry now can start one after it.';
        return on ? 'Not needed: every submission already had a definite answer.' : 'Not used. Retry now starts one by hand.';
    }
    if (r.state === 'RETRYING') return `Running: pass ${r.passes.length || 1} of up to ${r.maxPasses}, ends by ${new Date(r.endsBy).toLocaleTimeString()} at the latest.`;
    return `${r.state === 'STOPPED' ? 'Stopped' : 'Finished'}: ${r.reason ?? ''}`.trim();
}

function cell(label, value, className) {
    const row = document.createElement('div');
    const dt = document.createElement('dt'); dt.textContent = label;
    const dd = document.createElement('dd'); dd.textContent = value;
    if (className) dd.className = className;
    row.append(dt, dd);
    return row;
}

export class ResultsView {
    constructor(root) {
        this.root = root;
        this.bar = root.querySelector('[data-outcome-bar]');
        this.legend = root.querySelector('[data-outcome-legend]');
        this.caption = root.querySelector('[data-outcome-caption]');
        this.during = root.querySelector('[data-during-run]');
        this.retry = root.querySelector('[data-retry-period]');
        this.retryStatus = root.querySelector('[data-retry-period-status]');
        this.retryPasses = root.querySelector('[data-retry-passes]');
        this.connections = root.querySelector('[data-connections]');
    }

    render(run) {
        const o = outcomeSummary(run);
        const parts = [
            ['accepted', 'Accepted', o.accepted],
            ['unresolved', 'No definite answer', o.unresolved],
            ['rejected', 'Rejected', o.rejected],
            ['not-sent', 'Not sent (stopped)', o.notSent],
            ['pending', run?.finishedAt == null ? 'Still to come' : 'Pending', o.pending],
        ];
        this.bar.replaceChildren();
        for (const [kind, label, n] of parts) {
            if (!n) continue;
            const segment = document.createElement('span');
            segment.className = `segment ${kind}`;
            segment.style.flexGrow = String(n);
            segment.title = `${label}: ${fmt(n)} (${pct(n, o.total)})`;
            this.bar.append(segment);
        }
        this.bar.setAttribute('aria-label', parts.map(([, label, n]) => `${label} ${fmt(n)}`).join(', '));

        this.legend.replaceChildren();
        for (const [kind, label, n] of parts) {
            if (kind === 'pending' && !n) continue;
            const item = cell(label, `${fmt(n)}`, kind);
            item.className = `legend-item ${kind}`;
            const share = document.createElement('span'); share.className = 'share'; share.textContent = pct(n, o.total);
            item.append(share);
            if (kind === 'unresolved' && n) {
                const detail = document.createElement('span'); detail.className = 'detail';
                detail.textContent = `${fmt(o.skipped)} skipped · ${fmt(o.blocked)} blocked · ${fmt(o.unknown)} unknown`;
                item.append(detail);
            }
            this.legend.append(item);
        }
        const of = run?.settings?.endless ? `${fmt(o.total)} submissions so far (runs until stopped)` : `${fmt(o.total)} planned submissions`;
        this.caption.textContent = run?.state === 'WARMING_UP'
            ? `Warming up: ${fmt(run.warmupSent)} of ${fmt(run.warmupPlanned)} warm-up requests sent (not stored, not counted below).`
            : `${of}${run?.warmupSent ? ` · warm-up before it: ${fmt(run.warmupSent)} sent, ${fmt(run.warmupFailed)} failed` : ''}.`;

        const d = duringRun(run);
        this.during.replaceChildren(
            cell('Sent', fmt(d.sent)),
            cell('Accepted', fmt(d.accepted)),
            cell('In flight now', fmt(run?.inFlight)),
            ...(d.retriesOn
                ? [cell('Needed a retry', fmt(d.retried)), cell('Recovered by those retries', fmt(d.recovered)), cell('Extra requests', fmt(d.extraRequests))]
                : [cell('Retries during the run', 'Off')]),
            ...(d.circuit
                ? [
                    // Whether the backend is reachable only means something while the run is sending.
                    ...(run?.finishedAt == null
                        ? [cell('Backend reachable', backendReachable(d.circuit), d.circuit.state === 'CLOSED' ? '' : 'problem')] : []),
                    cell('Paused (times)', fmt(d.circuit.opened)), cell('Held back while paused', fmt(d.circuit.heldBack))]
                : []),
            ...(d.duplicatesOn
                ? [cell('Sent twice (repeats)', fmt(d.duplicatesSent)), cell('Repeats acknowledged', fmt(d.duplicatesAccepted))]
                : []));

        const r = run?.retry;
        this.retryStatus.textContent = retryPeriodStatus(run);
        this.retry.replaceChildren(
            cell('Passes', r ? fmt(r.passes.length) : '—'),
            cell('Sent', r ? fmt(retrySent(run)) : '—'),
            cell('Recovered', r ? fmt(r.recovered) : '—', r?.recovered ? 'good' : ''),
            cell('Still no definite answer', fmt(o.unresolved), o.unresolved && r && r.state !== 'RETRYING' ? 'problem' : ''));
        this.retryPasses.replaceChildren(...(r?.passes ?? []).map(p => {
            const li = document.createElement('li');
            const rest = [['skipped', p.skipped], ['blocked', p.blocked], ['unknown', p.unknown], ['rejected', p.failed]]
                .filter(([, n]) => n).map(([word, n]) => `${fmt(n)} ${word}`);
            li.textContent = `Pass ${p.pass}: ${fmt(p.sent)} of ${fmt(p.planned)} sent, ${fmt(p.accepted)} accepted`
                + (rest.length ? `, ${rest.join(', ')}` : '');
            return li;
        }));
        this.retryPasses.hidden = !r?.passes?.length;

        const jetty = run?.settings?.httpClient === 'JETTY';
        this.connections.replaceChildren(
            cell('Open', jetty ? fmt(run.connectionsOpen) : '—'),
            cell('Opened since warm-up', jetty ? fmt(run.connectionsOpened) : '—', jetty && run.connectionsOpened ? 'problem' : ''),
            cell('Closed since warm-up', jetty ? fmt(run.connectionsClosed) : '—', jetty && run.connectionsClosed ? 'problem' : ''));
    }
}
