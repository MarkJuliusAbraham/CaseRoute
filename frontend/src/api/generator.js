// The frontend talks to a same-origin proxy, not a hardcoded backend address.
const base = '/api/runs';

async function request(path, options = {}) {
    const response = await fetch(path, options);
    if (!response.ok) {
        let message = `Request failed: HTTP ${response.status}`;
        try {
            const body = await response.json();
            message = body.detail || body.message || message;
        } catch { /* Non-JSON errors still include the HTTP status. */ }
        throw new Error(message);
    }
    return response.status === 204 ? null : response.json();
}

export const generator = {
    start: settings => request(base, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(settings),
    }),
    stop: id => request(`${base}/${encodeURIComponent(id)}/stop`, { method: 'POST' }),
    current: () => request(`${base}/current`),
    events: id => new EventSource(`${base}/${encodeURIComponent(id)}/events`),
    retry: (id, seconds) => request(`${base}/${encodeURIComponent(id)}/retry` + (seconds ? `?seconds=${encodeURIComponent(seconds)}` : ''), { method: 'POST' }),
    check: id => request(`${base}/${encodeURIComponent(id)}/check`, { method: 'POST' }),
    latestCheck: id => request(`${base}/${encodeURIComponent(id)}/check`),
    reportUrl: id => `${base}/${encodeURIComponent(id)}/report`,
};
