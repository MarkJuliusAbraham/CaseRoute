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
(40,000) requests; both limits are split evenly across the instances, and each has `generator.jetty.selectors` (1)
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
- **A waiting line sized by time** (`max-queued`, 40,000: about 4 s of work at ~10,000/s, far below the 15 s request
  timeout). A request that finds it full is **SKIPPED**: never sent, so nothing can have been stored. 10,000 (~1 s)
  was too short: every brief stall, such as the warm-up-to-run switch or a GC pause, became skips even at 500k/min.
  With 100,000, waits grew until requests hit the 15 s
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
UNKNOWN, and are never retried during the run, so a run stays exactly its set length. The retry period after the
run sends them again (below). The checker reports blocked submissions separately; "blocked but saved" must be 0.

## When the backend is unreachable

A refused connection (`ConnectException`) means the request never reached the backend: it is **SKIPPED** with the
detail "Connection refused: backend not reachable; not sent", and the retry period can resend it. A connection that
breaks mid-request, or a timeout, stays **UNKNOWN**.

`circuitBreaker` (on by default, `CircuitBreaker`): after 20 replies in a row without any HTTP status, the generator
stops sending for 1 s (doubling up to 10 s while the backend stays away), then sends one probe. When the probe gets any
HTTP reply, the share of submissions sent ramps from 0 to 100% over 5 s. Any HTTP reply, even an error, resets the
count. Submissions due while paused or ramping are skipped with the reason; the schedule itself never stops. The
snapshot's `circuit` reports the state, since when, how often it opened and how many submissions it held back.
Duplicate sends are skipped while it is not closed.

## Duplicate submissions

`duplicatePercent` (0 to 50, default 0) sends that share of submissions a second time, with the same clientRequestId and
content, 0 to 2 s after the first: sometimes while the first is still in flight, sometimes after it was accepted, like
a customer double-clicking or resubmitting. Which ones is decided from the sequence number alone
(`Submission.isDuplicated`), so no list is kept. Repeats are extra traffic: the submission's outcome is still its first
send's, and the snapshot's `duplicates` counts repeats sent, accepted (the backend acknowledged the repeat) and other.
Submissions skipped at the in-flight cap are not repeated. The checker then verifies that each submission has one case
and that no case was processed twice.

## Retry period

A layer after the run, separate from the in-run retries above: every submission without a definite answer is sent
again with its original clientRequestId.

| Outcome | Retried | Why |
|---|---|---|
| SKIPPED | yes | Never sent (in-flight cap or the HTTP client's waiting line was full): nothing stored. |
| BLOCKED | yes | The backend said busy, nothing stored, retry later. |
| UNKNOWN | yes | May or may not be stored; resending is safe because the backend ignores an id it already has. |
| REJECTED / MISMATCH (FAILED) | no | A definite answer: the same request gets the same answer. |
| Not sent | no | The run was stopped on purpose. |

With `retryPeriod: true` it starts by itself after a run that finishes on its own (not after Stop or an error);
`POST /api/runs/{id}/retry?seconds=N` starts one by hand (`/resend-blocked` is the older name). It sends at the run's
rate, waiting for in-flight slots instead of skipping, in passes: each pass covers whatever is still Skipped, Blocked
or Unknown, and passes are 1 s, 2 s, 4 s ... (at most 30 s) apart. It ends at the first of: nothing left, the time
limit (`retryPeriodSeconds`, default 120, or `seconds`), 10 passes, a pass that resolves nothing (no accepted or
rejected outcomes: the backend may be down), or Stop. The state is `RETRYING` meanwhile and no new run can start.
New outcomes replace the old ones in the run's counts, so the checker sees the final state; the snapshot's `retry`
field holds each pass (planned, sent, accepted, skipped, blocked, unknown, failed), `recovered`, and the `reason` it
ended. A summary is written to `reports/<runId>.retry-<n>.json`. It is not part of the run's measured time: the
run's report and per-second lines end with the run.

## Checker

`POST /api/runs/{id}/check` (after the run ends) reconciles the run: it streams the backend's ledger
(`GET /api/simulation/cases/ledger?testRunId=`, one `clientRequestId,status,resultsReceived` line per saved case) and compares it
with the generator's final outcome for every submission. `GET /api/runs/{id}/check` returns the latest result,
which is also written to `reports/<runId>.check.json`.

- Accepted: processed, awaiting processing (OPEN), or **missing** (no case: lost, or still in Kafka if checked early).
- Unknown: saved anyway (only the reply was lost) or never arrived.
- Rejected but saved, unexpected (never sent or never planned): should always be 0.
- Duplicates: **cases created twice** (a second ledger row for one clientRequestId) and **processed more than once**
  (cases, of all in the run, with more than one processing result): both should be 0. A second row is not counted
  again as a processed submission. "Processed more than once" is -1 against a backend whose ledger has no
  `resultsReceived` column.
- Verdict: `CONSISTENT`, `PENDING` (only OPEN cases remain) or `DISCREPANCIES`.
- The ledger must end with `#end,<rows>` and that count must match the rows read; otherwise the check fails with 502
  ("ended early") and concludes nothing. A streamed download that stops early looks like a normal reply, and the
  backend once cut the ledger off after 30 s (its async request timeout, now 10 minutes): every case left out
  would have been reported as missing.

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
