/** Downtime test: takes the backend down, 'graceful' (finish requests in progress, then exit) or 'crash' (instantly). */
export async function takeBackendDown(mode) {
    const response = await fetch('/api/simulation/downtime', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Confirm-Downtime': 'TAKE_BACKEND_DOWN' },
        body: JSON.stringify({ mode }),
        signal: AbortSignal.timeout(5000),
    });
    if (!response.ok) throw new Error(`The backend did not accept it (HTTP ${response.status}). If it was started before this control existed, restart it.`);
}

export async function clearSavedCases() {
    const response = await fetch('/api/simulation/cases', {
        method: 'DELETE',
        headers: { 'X-Confirm-Reset': 'DELETE_ALL_CASES' },
        signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) throw new Error(`Could not clear saved cases (HTTP ${response.status}). Restart the backend with simulator reset enabled.`);
}
