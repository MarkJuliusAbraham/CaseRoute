export async function clearSavedCases() {
    const response = await fetch('/api/simulation/cases', {
        method: 'DELETE',
        headers: { 'X-Confirm-Reset': 'DELETE_ALL_CASES' },
        signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) throw new Error(`Could not clear saved cases (HTTP ${response.status}). Restart the backend with simulator reset enabled.`);
}
