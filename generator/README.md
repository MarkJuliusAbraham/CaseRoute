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
There are no automatic retries.

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
None of these independently verifies database state or business processing. A checker is a future component.

## Backend contract

Each run picks a write path with `submissionMode` (`KAFKA` by default, or `DIRECT`). Each path has its
own endpoint with one fixed response contract:

- `KAFKA` → `POST /api/cases`: replies `202 Accepted` only after Kafka confirms the write (`acks=all`),
  and `503` if it cannot confirm in time. A consumer saves accepted submissions to Postgres in batches.
  A unique constraint on clientRequestId plus `ON CONFLICT DO NOTHING` makes resubmitting safe.
- `DIRECT` → `POST /api/cases/direct`: saves to Postgres before replying `201 Created` with the case.
  A duplicate clientRequestId returns `409`.

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
