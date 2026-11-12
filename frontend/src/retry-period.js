// The retry period after a run: passes over every submission still Skipped, Blocked or Unknown (no definite answer,
// and safe to resend with the same clientRequestId). Automatic after a completed run when turned on, or started with
// "Retry now". Generator: snapshot field `retry`, POST /api/runs/{id}/retry?seconds=N.

/** Submissions a retry period would send now. */
export function retryable(run) {
    return (run?.skipped ?? 0) + (run?.blocked ?? 0) + (run?.unknown ?? 0);
}

/** Whether "Retry now" can start a retry period for this run. */
export function canRetry(run) {
    return run != null && run.finishedAt != null && run.state !== 'RETRYING' && retryable(run) > 0;
}

function time(iso) {
    return iso ? new Date(iso).toLocaleTimeString() : '';
}

/** The status line under the run bar. */
export function describeRetry(run) {
    const r = run?.retry;
    const waiting = retryable(run);
    if (!r) {
        if (!run || run.finishedAt == null || !waiting) return '';
        return `${waiting.toLocaleString()} submissions have no definite answer (skipped, blocked or unknown). Retry now sends them again with their original IDs.`;
    }
    const passes = r.passes ?? [];
    const last = passes.at(-1);
    if (r.state === 'RETRYING') {
        if (!last) return 'Retry period starting...';
        return `Retry period, pass ${last.pass} of up to ${r.maxPasses}: ${last.sent.toLocaleString()} of ${last.planned.toLocaleString()} sent, `
            + `${r.recovered.toLocaleString()} recovered so far. Ends by ${time(r.endsBy)} at the latest.`;
    }
    const ended = r.state === 'STOPPED' ? 'stopped' : 'finished';
    return `Retry period ${ended} after ${passes.length} pass${passes.length === 1 ? '' : 'es'}: `
        + `${r.recovered.toLocaleString()} recovered, ${waiting.toLocaleString()} still without a definite answer. ${r.reason ?? ''}`.trim();
}
