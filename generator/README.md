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

## Endless runs and flat memory

With `endless: true` ("Run until stopped") a run keeps sending at its rate until stopped; `durationMinutes` is
ignored, `planned` in the snapshot is the number handled so far, and nothing is ever "not sent". It stops by itself
after 2,147,483,646 submissions (about 35 hours at 1,000,000 per minute).

Nothing in the generator grows with healthy traffic, so a run can last as long as the system holds up:

- Submissions are never listed. Submission n's clientRequestId is the run id with n added to its low 64 bits, so
  any submission is rebuilt from its number when sent or resent, and the checker turns a saved id back into n.
  This also removed the random UUID generation that showed up in profiles during warm-ups.
- Only outcomes other than ACCEPTED are kept, by number. They grow only with problems, which the report records
  one line each anyway.
- The report has one `plan` line instead of a line per submission, and per-second lines are flushed every second.
- The checker marks saved accepted submissions in a bit set: 1 bit per submission while it runs.

The point of an endless run is to see whether anything builds up over time. The connection counters (see
"Running indefinitely: connections") are the main signal.

Reports are under `reports/<runId>.jsonl`, relative to the generator working directory. One JSON object per line:

- `run`: settings (first line).
- `plan`: the planned count (null for an endless run) and how clientRequestIds are formed (see above).
- `result`: only submissions that were **not** accepted (FAILED/REJECTED/MISMATCH, UNKNOWN, SKIPPED), with detail.
- `notSent`: the range of sequence numbers never dispatched because the run stopped early (`fromSequence`,
  `toSequence`, `count`).
- `second`: per-second aggregates by response time: counts per outcome, latency avg/p99/max, max scheduling delay,
  and connections the Jetty client opened and closed in that second.
- `summary`: final counts (last line).

A planned sequence number with no `result` line and outside the `notSent` range was accepted. After a crash (no `summary`), missing lines need
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

## HTTP clients

Submissions are spread over `generator.http-clients` (8) independent Java HttpClients, chosen by clientRequestId
(so a retry uses the same client). Each HttpClient does all network work for its connections on one internal
thread behind one lock; at ~10,000 requests/s a single client's thread was measured busy closing sockets and blocked
on that lock while requests timed out before reaching the backend. Every submission still uses its own connection.

## HTTP client choice

Each run picks `httpClient`: `JDK` (the built-in client above) or `JETTY` (the default), Jetty's HttpClient spread over
`generator.jetty.clients` (8) instances, chosen by clientRequestId. Together they open at most
`generator.jetty.max-connections` (2,000) connections to the backend and queue at most `generator.jetty.max-queued`
(10,000) requests; both limits are split evenly across the instances, and each has `generator.jetty.selectors` (1)
network threads. Extra requests wait instead of opening new connections, which avoids the connection bursts that
collapse the JDK client near 10,000 requests/s. With a single Jetty instance, a backlog left 192 of its 200 worker
threads queued on its one waiting-line lock; separate instances have separate lines and locks.

With a warm-up, a Jetty run also **pre-opens spare connections** gradually over the second half of the warm-up (20
steps up to the full pool). Without it, the switch from warm-up to run opened hundreds of connections at once: the
backend's accept queue overflowed and about 2,500 submissions failed with "Connection refused" in those 2 seconds,
while the remaining 58 seconds ran clean (600k/min: 597,531 accepted, all saved and processed).

### Running indefinitely: connections

Jetty is set up so that nothing about its connections depends on how long a run lasts:

- **A fixed pool.** Connections are opened once, gradually during the warm-up, up to `max-connections` (2,000), and
  never closed for being idle by either side: `generator.jetty.idle-timeout=0`, and the backend's
  `server.tomcat.keep-alive-timeout=-1` and `server.tomcat.connection-timeout=-1`. Idle connections cost Tomcat
  almost nothing. Timers were tried first and failed in both directions:
  - The backend's former 60 s default closed idle pre-opened connections at once, and reopening them caused about
    2,300 refusals in a single second.
  - A 30 s generator idle timeout (shorter than the backend's, to avoid that race) shrank the pool during calm
    periods. In a 700k/min endless run, the next brief slowdown (p99 ~230 ms) opened 874 connections in one second,
    and about 2,750 requests were refused by the backend's accept queue.
- **Jetty's default pool** reuses open connections first. An earlier round-robin pool was the largest CPU cost in a
  profile of a 1M/min connection test (24.8% of samples spent choosing a connection).
- **A short waiting line** (`max-queued`, 10,000: about 1 s of work at ~15,000/s). A request that finds it full is
  **SKIPPED**: never sent, so nothing can have been stored. With 100,000, waits grew until requests hit the 15 s
  timeout; on HTTP/1.1 a timed-out request closes its connection, a new one is opened, and the old port stays
  reserved for 2 minutes (TIME_WAIT). That builds up until Windows' 16,384 ports run out (`WSAENOBUFS`, "No buffer
  space available"), which is why overloaded runs got worse over time instead of leveling off. A retry that finds
  the line full keeps its earlier UNKNOWN outcome.
- **Counted**: the snapshot reports `connectionsOpen`, and `connectionsOpened` / `connectionsClosed` since the
  measured run started. In a healthy run with a warm-up both stay at 0. Growth means connections are being
  replaced (closed by the backend, or a request timed out). The JDK client reports 0.

Warm-ups, runs and resends all use the chosen client; outcomes are classified identically.

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

## Blocked submissions

A `503` **with** a `Retry-After` header comes from the backend's load shedding: it turned the submission away before
doing anything, so nothing was stored. Such submissions are counted as `BLOCKED` (the snapshot's `blocked`), not
UNKNOWN, and are never retried automatically, so a run stays exactly its set length. After the run,
`POST /api/runs/{id}/resend-blocked` resends every still-blocked submission at the run's rate with its original
clientRequestId (state `RESENDING`; progress in the snapshot's `resend` field and the event stream). New outcomes
replace BLOCKED in the run's counts; a summary is written to `reports/<runId>.resend-<n>.json`. The checker reports
blocked submissions separately; "blocked but saved" must be 0.

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
- `NOOP` → `POST /api/cases/noop`: a connection test. Replies `202 Accepted` immediately and stores nothing;
  warm-ups go there too, and the checker does not apply.
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
