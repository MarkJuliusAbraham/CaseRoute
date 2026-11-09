# CaseRoute frontend

Shared browser controls and visualization, independent of the backend services.
Currently connected to the generator; processor and checker views will be added when those APIs exist.
The UI uses plain JavaScript modules and Vite. No framework is required.

## Development

Requires Node.js 20.19+ or 22.12+ and npm. From this folder:

```powershell
npm ci
npm run dev
```

Open http://localhost:5173. Start the generator separately on port 8081.
The existing case processing backend runs on port 8080 and PostgreSQL on 5432.

## Service connections

`src/api/generator.js` owns generator requests and SSE connections.
The browser calls relative `/api/runs` URLs on the frontend origin.
Vite forwards these to `http://127.0.0.1:8081`, including SSE and report downloads.
This avoids broad CORS configuration on the generator.

To change the generator address, copy `.env.example` to `.env.local`, edit
`GENERATOR_API_URL`, and restart Vite. This variable configures the local proxy,
not a browser-side secret. Never place secrets in frontend code.

## Build

```powershell
npm run build
npm run preview
```

`dist/` contains the independently deployable frontend. Preview serves it on
http://localhost:4173 with the same local proxy configuration.
For production, serve `dist/` through a web server and configure a reverse proxy
for `/api/runs` to the generator. Vite's dev proxy is not included in the build.
The generator remains local-only and unauthenticated; public deployment is not configured.

## Generator pipeline visualization

`src/visualization/traffic-model.js` derives dispatch deltas from generator SSE snapshots.
`traffic-view.js` draws a Generator, a connecting pipe, and a Backend destination.
Moving dots illustrate newly observed outgoing requests. Large batches are grouped into at
most 80 particles per snapshot. Travel time is illustrative, not latency or completion.
The Generator shows sent requests; Backend shows matching creation replies reported by the
generator. It does not consume backend processing telemetry or imply case resolution.
The pipe stacks vertically on narrow screens. Reduced-motion preferences suppress particles while keeping counts updating. Reconnects establish a baseline rather than replaying
history. Observed rate uses up to 10 seconds of browser-side snapshot observations.
Run telemetry model tests with `npm test`.

## Connectivity and local reset

Green/red dots independently poll the generator (`/api/runs/health`) and backend
(`/actuator/health`, proxied as `/api/backend/health`) every 3 seconds, with a 2-second timeout.
Backend health includes the configured database health check. These indicators show
reachability/health, not processing completion or SSE delivery guarantees.

Clear saved cases calls `DELETE /api/simulation/cases` after confirmation in an in-page dialog (Cancel and Escape do nothing).
The UI disables it until both services are reachable and the known run is finished,
and rechecks generator state before confirmation. Stop other clients before resetting:
this is not a distributed lock against new submissions from other browsers/processes.
Only the `cases` rows are removed; table structure and generator reports are retained.
Old run counters remain historical after reset.

The backend reset endpoint requires `simulation.reset-enabled=true`, a loopback caller,
and the explicit `X-Confirm-Reset: DELETE_ALL_CASES` header. This is enabled in the
current local configuration; turn it off outside the simulator. The frontend proxy
must not be published without appropriate authentication: it connects from loopback.
Restart both Java services and Vite after adding these routes.

Start remains disabled until generator health and current-run status are known, and during an active run, start request, or database reset. Backend health is displayed separately so failed-target simulations remain possible.

## Received cases report

View received cases opens a paginated database-backed table (25 rows/page), with All cases
or Current/latest run filters and CSV download. New generator submissions persist testRunId.
Older rows remain visible under All cases but are not backfilled from description text.
GET `/api/simulation/cases` accepts optional testRunId, page, and size (1–100).
GET `/api/simulation/cases/export` streams CSV in a consistent read-only database transaction.
Both endpoints require loopback access and `simulation.reports-enabled=true` (enabled locally).
CSV downloads are separate requests and may reflect a later snapshot than the table.
Refresh is manual; an empty run report does not prove every request was lost.
Pipeline particle speed and arrival are illustrative, not timing/acceptance measurements.

Received cases and Recent responses now share a fixed-size modal data inspector with keyboard-accessible tabs, internal table scrolling, Escape/Close dismissal, and focus restoration. Opening it does not expand the dashboard layout.

The dashboard separates Generator settings (rate, duration, HTTP concurrency), Backend (health and database reset), and Run controls (start/stop and data inspection). No backend tuning controls are implied or added.

Health history is a third inspector tab. It retains up to 1,000 browser-observed checks for 24 hours in localStorage (memory fallback). Filter by service or status changes and export JSON. Check response time includes the frontend proxy. No observations are made while the dashboard is closed; status gaps are explicitly unknown. Red means a failed check, not proof of a crashed service.
