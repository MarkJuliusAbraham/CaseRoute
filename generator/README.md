# CaseRoute traffic generator

A local Spring Boot API service. Java 25 is required, matching the backend.

From the repository root, start the database and CaseRoute backend first. In another terminal:

```powershell
cd generator
..\backend\mvnw.cmd spring-boot:run
```

The generator API runs at http://localhost:8081 and binds to loopback only. It has no login and is intended for local use.
Browser controls live in the separate [frontend](../frontend/README.md) at http://localhost:5173.
The target is server-configured via `generator.target-url` (default `http://localhost:8080/api/cases`).
The generator does not serve frontend files. Run the frontend development server separately.

## Run controls

- `POST /api/runs`: JSON `{ "requestsPerMinute": 50, "durationMinutes": 1, "maxInFlight": 20 }`.
- `GET /api/runs/current`: restore the latest run after a page refresh (204 if none).
- `GET /api/runs/{id}`: snapshot.
- `GET /api/runs/{id}/events`: SSE snapshots every 500 ms; reconnect returns current state.
- `POST /api/runs/{id}/stop`: stop new submissions and drain outstanding HTTP requests.
- `GET /api/runs/{id}/report`: download JSON Lines report.

Only one active run is allowed. The latest run is kept in memory; older reports remain on disk.
The requested rate can be up to 1,000,000 requests/minute. Runs are limited to 1,000,000 planned submissions, 60 minutes, and 500 in-flight requests.
At the maximum rate, use a one-minute run. This is an offered-load setting, not a throughput guarantee.
Capacity-limited submissions are currently skipped, not durably queued for later processing.

## Scheduling and results

The first request is scheduled immediately, followed by evenly spaced requests using a monotonic clock.
For 50 requests/minute over 10 minutes, 500 submissions are planned. Scheduling does not wait for responses.
When maximum concurrency is reached, a submission is recorded as SKIPPED rather than queued indefinitely.
Scheduling delay is measured. Rates are best effort on the local machine, not a hard real-time guarantee.
Stopping records the remaining count as not sent and waits for in-flight requests (15-second request timeout).
Retries are off by default; see "Retries" below.

Reports are under `reports/<runId>.jsonl`, relative to the generator working directory. One JSON object per line:

- `run`: settings (first line).
- `planned`: every submission with its ID and payload, written before dispatch starts.
- `result`: only submissions that were **not** accepted (FAILED/REJECTED/MISMATCH, UNKNOWN, SKIPPED), with detail.
- `notSent`: IDs never dispatched because the run stopped early.
- `second`: per-second aggregates by response time: counts per outcome, latency avg/p99/max, max scheduling delay.
- `summary`: final counts (last line).

A planned ID with no `result` or `notSent` line was accepted. After a crash (no `summary`), missing lines need
reconciliation instead: those requests may also have been in flight. Lines are queued in memory and written by one
background thread through a single open file, so disk I/O never delays dispatch. Write failures stop scheduling
and are shown as ERROR.
Files survive a process restart, but runs do not resume. Downloads during a run are partial.

ACCEPTED means the expected reply for the run's mode. KAFKA: HTTP 202 with matching clientRequestId,
testRunId and status ACCEPTED; the submission is durably in Kafka and reaches Postgres asynchronously, so the
row may not exist yet. DIRECT: HTTP 201 with matching payload, OPEN status and a case UUID; the row exists.
REJECTED means any other response below 500. MISMATCH means the expected status failed validation.
UNKNOWN means a network failure, interruption, or 5xx: the backend may have accepted the submission.
None of these independently verifies database state or business processing; the checker does (below).

## Retries

With `retryEnabled: true` (the "Retry unknown outcomes" toggle), a submission whose outcome is UNKNOWN is resent
with the same clientRequestId. Up to `generator.retry.max-attempts` (5) attempts in total; the wait before each
retry doubles from `base-delay` (500 ms) up to `max-delay` (8 s) and is randomised between half and all of that
value. REJECTED and MISMATCH are definite answers and are never retried. A submission holds its in-flight slot
until its final outcome. The snapshot counts `retried` (submissions that needed a retry), `recovered` (of those,
accepted in the end) and `retryRequests` (extra HTTP requests); each result records its `attempts`.

## Warm-up

With `warmupSeconds` > 0, the run first ramps from zero to the full rate over that many seconds, sending synthetic
submissions to `POST /api/cases/warmup`. The backend handles them like real ones, including the Kafka write and
its confirmation, but writes to `cases.warmup`, which nothing consumes, so no case is created. It replies 204. That is `requestsPerMinute * warmupSeconds / 120` requests, reported as
`warmupPlanned`, `warmupSent` and `warmupFailed`, never in the run's outcome counts or report lines. The state
is `WARMING_UP` until the measured run starts at full rate on the same HTTP client, so its connections are
already open, both JVMs have compiled the request path, and the backend's Kafka producer is warm. Leave it at 0 to test a cold start (a sudden spike).

## Checker

`POST /api/runs/{id}/check` (after the run ends) reconciles the run: it streams the backend's ledger
(`GET /api/simulation/cases/ledger?testRunId=`, one `clientRequestId,status` line per saved case) and compares it
with the generator's final outcome for every submission. `GET /api/runs/{id}/check` returns the latest result,
which is also written to `reports/<runId>.check.json`.

- Accepted: processed, awaiting processing (OPEN), or **missing** (no case: lost, or still in Kafka if checked early).
- Unknown: saved anyway (only the reply was lost) or never arrived.
- Rejected but saved, unexpected (never sent or never planned): should always be 0.
- Verdict: `CONSISTENT`, `PENDING` (only OPEN cases remain) or `DISCREPANCIES`.

The dashboard runs it automatically once the pipeline settles after a run.

## Backend contract

Each run picks a write path with `submissionMode` (`KAFKA` by default, or `DIRECT`). Each path has its
own endpoint with one fixed response contract:

- `KAFKA` → `POST /api/cases`: replies `202 Accepted` only after Kafka confirms the write (`acks=all`),
  and `503` if it cannot confirm in time. A consumer saves accepted submissions to Postgres in batches.
  A unique constraint on clientRequestId plus `ON CONFLICT DO NOTHING` makes resubmitting safe.
- `DIRECT` → `POST /api/cases/direct`: saves to Postgres before replying `201 Created` with the case.
  A retry of an already saved submission (same clientRequestId and content) returns `200` with the existing
  case; the same clientRequestId with different content returns `409`.

New submissions require a UUID `clientRequestId` in the JSON body. `generator.target-url` points at
`/api/cases`; DIRECT runs append `/direct`.
Restart the backend after schema changes before running a load test.

```json
{
  "clientRequestId": "5801e384-c819-4bdf-8247-982e01b9b005",
  "subject": "Duplicate charge",
  "description": "The same purchase appears twice."
}
```

Run generator tests from this folder with `..\backend\mvnw.cmd test`.

To disable the generator concurrency cap, send `"maxInFlight": null` or check No in-flight limit in the UI. Responses, timeouts, and errors are still tracked. The numeric 1–500 limit applies only when a cap is enabled; pending requests may accumulate in unlimited mode.
