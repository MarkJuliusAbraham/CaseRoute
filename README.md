# CaseRoute

A system design case study in reliable customer-service case submission and processing.

## Project structure

- `backend/`: Spring Boot case API and PostgreSQL persistence (port 8080).
- `generator/`: Spring Boot traffic-generation API and SSE progress (port 8081).
- `processor/`: Spring Boot app that processes saved cases from `cases.saved` and publishes results to
  `cases.processed` (port 8083). It has no database access; it only talks to Kafka.
- `frontend/`: shared browser controls and visualization (Vite, port 5173).
- `compose.yaml`: local PostgreSQL (port 5432) and Kafka (port 9092).

## Local development

Start Docker Desktop, then from the project root:

```powershell
docker compose up -d postgres kafka
```

The backend has two write paths, chosen per run by the dashboard's Kafka toggle:

- `POST /api/cases` writes each submission to the `cases.submitted` Kafka topic and replies `202 Accepted`;
  a consumer in the backend saves it to Postgres in batches. Unsaveable messages go to `cases.submitted.DLT`.
- `POST /api/cases/direct` saves to Postgres inside the request and replies `201 Created`.

The backend needs both Postgres and Kafka running.

Run each application in a separate terminal:

```powershell
cd backend
.\mvnw.cmd spring-boot:run
```

```powershell
cd generator
..\backend\mvnw.cmd spring-boot:run
```

```powershell
cd processor
..\backend\mvnw.cmd spring-boot:run
```

Start the backend before the processor: the backend creates the `outbox` table and the processing columns.

```powershell
cd frontend
npm ci
npm run dev
```

Open http://localhost:5173. The frontend proxies generator calls and SSE streams to port 8081, and the simulation
controls to the backend (8080) and processor (8083). The generator sends synthetic submissions directly to the backend.
Closing the UI does not stop a run.

See [frontend setup](frontend/README.md) and [generator API and report semantics](generator/README.md).
New case submissions require a UUID `clientRequestId`; restart the backend after schema changes.


## Local management UIs

Start Kafka, Kafbat UI, and pgAdmin from the repository root:

```powershell
docker compose up -d kafka kafka-ui pgadmin
```

- Kafka UI: http://localhost:8082 (CaseRoute cluster preconfigured).
- pgAdmin: http://localhost:5050. Web login: `admin@caseroute.dev` / `localdev`.
- PostgreSQL is pre-registered as CaseRoute PostgreSQL in pgAdmin. When connecting,
  its database username is `caseroutedb` and password is `localdev`.

These are local development credentials. The UIs bind only to loopback.
The pgAdmin web account is separate from the PostgreSQL database account.
Its account/settings persist in `pgadmin_data`; changing the initial login environment
variables later does not reset an existing account.

Kafka has two client addresses: local Java/CLI clients use `localhost:9092`;
Docker services such as Kafka UI use `kafka:29092`. pgAdmin connects to `postgres:5432`
inside Docker, rather than to its own localhost. Kafka's controller port stays internal.
The added Kafka listener takes effect when Compose recreates the Kafka container;
the existing named data volumes remain in use.

## Diagnosing backend stalls

Load runs have shown brief backend freezes where new connections are refused. To find the cause, the backend
records every garbage-collection and safepoint pause to `gc-backend.log` in its working folder (rotated, 5 files of
20 MB). The generator and processor write `gc-generator.log` and `gc-processor.log`: with a shared name, the apps
rotated each other's logs away. (The IntelliJ run configurations used here pass `-Xlog:gc*,safepoint:file=gc.log`,
so each app writes `gc.log` in its working folder instead: the backend's in `backend/`, the generator's in the project
root.)
This is on when started with `.\mvnw.cmd spring-boot:run` or the IntelliJ run configuration
**Backend (GC logging)** (in `.run/`).

After a run with refused connections, list pauses over 50 ms and compare their times with the stall:

```bash
grep -E "Pause|Total: " backend/gc-backend.log | awk 'match($0,/([0-9.]+)ms$/,m) && m[1]>50 {print} match($0,/Total: ([0-9]+) ns/,t) && t[1]>50000000 {print}'
```

A pause of the stall's length at the stall's time confirms GC (or another JVM safepoint). No such pause rules it out.

In a long safepoint line, compare `Reaching safepoint` with `At safepoint`. A large `Reaching safepoint` means the
JVM was waiting for one thread to stop, not doing GC work. The backend also runs with
`-XX:+SafepointTimeout -XX:SafepointTimeoutDelay=500`, which reports any thread that takes more than 500 ms to
stop. Find those reports with:

```bash
grep -n -i -A15 "timeout detected\|safepoint timeout" backend/gc-backend.log
```

The report names the thread (and its state), which shows what kept the whole backend frozen.

Finding so far: on Windows, the frozen-backend stalls were Tomcat's `Catalina-utility` threads. Tomcat runs them at
the lowest Java priority, which Windows honours, so under full CPU load they got no CPU time and could not reach a
safepoint for 0.5–5 s while every other thread waited. The backend therefore runs with `-XX:-UseThreadPriorities`,
which gives all threads normal OS priority. Linux already ignores Java priorities by default, so this matches typical
production behaviour.

The generator is also a Spring Boot/Tomcat app and froze the same way, sending nothing for up to 5 s and then a burst
of overdue requests. Its `pom.xml` uses the same flags (writing `gc-generator.log`). In IntelliJ, set the same VM
options on both run configurations and untick **Enable launch optimization**, which otherwise adds
`-XX:TieredStopAtLevel=1` and keeps the JVM from fully optimizing under load.

## Processing stage (outbox → cases.saved → processor)

When a case is saved (by the Kafka writer or by the direct endpoint), the same database transaction also inserts
a `case.saved` row into the `outbox` table. Saving and "needs processing" therefore always happen together.

- **Relay** (in the backend, `OutboxRelay`): every 100 ms it locks a batch of outbox rows, publishes them to the
  `cases.saved` topic, waits for Kafka's confirmation, then deletes them. If Kafka is down, rows wait in the outbox.
  A crash after publishing but before deleting can send a row twice, never zero times.
- **Processor** (`processor/`, separate app, Kafka only): consumes `cases.saved`, simulates the work on a virtual
  thread per case, and publishes the result (`ROUTED` with category and team, or `FAILED`) to `cases.processed`.
  Failed cases are also copied to `cases.saved.DLT`.
- **Results** (in the backend, `CaseProcessedConsumer`): consumes `cases.processed` in batches and updates the case
  row. The backend is the only writer of the `cases` table. Only `OPEN` cases are updated, so a duplicate result
  changes nothing, but every result is counted on the case (`results_received`) so a duplicate is visible; unusable
  results go to `cases.processed.DLT`.
- **Simulated work** is configured in `processor/src/main/resources/application.properties`: mean duration, a slow
  fraction, transient failures with retries, and permanent failures. Each case's behaviour is derived from its
  `clientRequestId`, so runs are reproducible. `max-in-flight` limits concurrent cases; when full, the processor
  stops pulling from Kafka and the backlog waits there.
- **Intake toggle**: "Buffer submissions through Kafka" only changes how a submission is accepted (`/api/cases`
  vs `/api/cases/direct`). Processing runs through Kafka in both modes.
- **Dashboard**: the pipeline shows a fifth Processor stage. Counts come from
  `GET /api/simulation/cases/stats?testRunId=` (saved, open, routed, failed, awaitingRelay), polled every second.

## Retries, status lookup and checker

- **Idempotent intake.** Both endpoints dedupe by `clientRequestId`: the Kafka path with `ON CONFLICT DO NOTHING`,
  the direct path by replying `200` with the already saved case. Clients may therefore resend when unsure.
- **Status lookup.** `POST /api/cases` replies `202` with `Location: /api/cases/requests/{clientRequestId}`.
  That endpoint returns the case and its processing status, or `404` while no case exists (never received, or
  still waiting in Kafka).
- **Retries** (generator, "Retry unknown outcomes" toggle): resend UNKNOWN outcomes with exponential backoff and
  jitter, up to 5 attempts. See `generator/README.md`.
- **Checker** (generator): after a run, compares every submission's outcome as the client saw it with the
  backend's ledger of saved cases, and reports missing, still-processing and unexpected cases.
- **Warm-up** (generator, "Warm-up in seconds"): ramps traffic up against `POST /api/cases/warmup` before the
  measured run. It takes the real Kafka path but writes to `cases.warmup`, which nothing consumes, so no case is
  created and the run does not start with zero open connections, cold code and a cold Kafka producer.
- **Load-test hygiene**: DevTools auto-restart is off in the backend (a recompile no longer restarts it mid-run),
  and each app writes its own GC log (`gc-backend.log`, `gc-generator.log`, `gc-processor.log`).
- **Kafka panel** (dashboard): between runs, change the backend's intake producers (how many, used in turn; linger,
  batch size, compression, requests in flight) and add partitions to the topics (add-only; Kafka cannot remove them), and watch the intake
  producer's live metrics: time waiting in the producer versus time waiting for the broker's confirmation.
  Producer changes last until the backend restarts.
- **Separate Kafka producers** in the backend: `backend-intake` for customer submissions (each waits for its own
  confirmation) and the default producer for the outbox relay and dead letters, so customer traffic never queues
  behind the relay's bursts. One producer has exactly one network thread; separating work means separate producers.
- **Downstream processing toggle** (dashboard, Processor section): pauses the backend's case writer, outbox relay
  and result writer and the processor (`PUT /api/simulation/pipeline`, `PUT /api/pipeline` on the processor), so
  intake can be measured on its own. Submissions wait in Kafka and are caught up on resume.
- **Connection test** (dashboard, Intake path): the generator sends everything, warm-up included, to
  `POST /api/cases/noop`, which replies 202 instantly with nothing stored. Measures the generator plus the backend's
  web server with zero work behind them.
- **Load shedding** (dashboard, Backend section): when on, the backend works on at most N submissions at once and
  turns the rest away immediately with `503` + `Retry-After: 1` (nothing stored). The generator counts those as
  **Blocked** (a definite answer, unlike Unknown) and never retries them during the run.
- **Duplicate submissions** (generator, "Duplicate submissions (%)"): sends that share of submissions a second time
  with the same ID and content. The checker verifies that each still has exactly one case and none was processed
  twice; the backend counts processing results per case (`results_received`) and reports them in its ledger.
- **Backend downtime** (Backend card, **Downtime test**): stop the backend gracefully (`server.shutdown=graceful`:
  requests in progress finish and reply) or crash it (exit instantly), then restart it from IntelliJ. The generator
  counts refused connections as not sent (Skipped), only broken or timed-out requests as Unknown, and its circuit
  breaker (on by default) pauses sending after 20 unreachable replies in a row, probes, and ramps back up over 5 s.
- **Retry period** (generator, "Retry period after the run" toggle, or **Retry now**): after the run, sends every
  submission still Skipped, Blocked or Unknown again with its original ID, at the run's rate, in passes, until none is
  left, a time limit (default 120 s), 10 passes, a pass that changes nothing, or Stop. Rejected and mismatched
  submissions are not retried: they already have a definite answer.
- **Run presets** (Generator card, "Preset"): one choice fills the settings for a common stress test (smoke test, baseline load, find the ceiling, overload and recover, cold spike, soak (until stopped), duplicates and retries, backend failure drill, generator ceiling (connection test)), with what it tests and what to look for. Live controls (load shedding, pause, Kafka) are not changed.
- **Simple and Panel views** (dashboard): Simple view keeps only the run bar, the pipeline graph and the checker;
  clicking a component in the graph opens its settings. Panel view shows all settings cards.
- **Endless runs** (generator, "Run until stopped"): keeps sending until stopped. The generator's memory stays flat
  (submission IDs are derived from the run id and a sequence number, so no list is kept), and its connections are
  set up so nothing depends on run length: a fixed pool of connections, opened once during the warm-up and never
  closed for being idle by either side (no idle, keep-alive or connection timeouts), and a full waiting line skips a
  request instead of letting waits grow into timeouts, which each cost a new connection and a port held for 2
  minutes. Connections opened and closed are shown on the dashboard; in a healthy run both stay at 0 after the
  warm-up.
- **Documentation** for every application is in the dashboard's Documentation tab (top right).

## Load test findings

Measured on one Windows PC running everything (generator, backend, processor, Kafka and PostgreSQL in Docker),
through Kafka with a 30-second warm-up and the Jetty HTTP client. The dashboard's Documentation tab has the full write-up.

| Run | Result |
|---|---|
| 1,000,000/min, retry period on | 739,711 accepted during the run; the retry period recovered the other 260,289 in 2 passes (23 s). Checker: all 1,000,000 saved and processed. |
| 900,000/min | 520,000–680,000 accepted (about 9,000–11,400/s), the rest skipped by the generator; 0 unknown. Best with 1 producer, linger 25 ms, 64 KB batches. |
| 800,000/min | 681,391 accepted (about 11,350/s), 118,609 skipped, 0 unknown; replies waited 3–4 s in the generator's line, never reaching the timeout. |
| 700,000/min, run until stopped | About 11,650/s for 3½ minutes at ~21 ms per reply. |
| 500,000/min | Keeps up (~30 ms per reply); a few seconds of slow replies right after the warm-up. |
| Connection test (nothing stored) | The generator itself tops out at about 14,600–15,800 requests/s; the backend's web server served 25,210/s to the native tool `oha`. |

- **The ceiling is the backend's Kafka intake on Windows, about 9,000–11,500 accepted/s.** A Java Flight Recorder
  recording showed the intake producer's single network thread blocked ~25% of the time (up to 284 ms) on a lock in
  Java's Windows network selector, taken on every "new batch" wake-up. Confirmations grew from ~5 ms to ~150 ms.
  Running the backend on Linux is the open test. Above the ceiling the generator skips what does not fit; nothing is lost.
- **Ruled out by measurement:** kafka (185,528 confirmed writes/s inside its container with the same safety settings (`acks=all`, idempotent)); docker's port forwarding (316,377 writes/s from Windows through the port, one producer sending as fast as possible); the producer pattern itself (42,337 writes/s from Windows with 2,000 threads, each sending one message and waiting for its confirmation, as the backend does); downstream work (With saving and processing paused, intake stayed at about 8,700/s); more producers, more partitions (4 producers: about 7,500/s (from 9,000). 12 partitions: about 10,000/s (from 11,400)); the backend's cpu and gc (3–4 of 20 cores in use; GC pauses about 40 ms every 2.6 s (~1.5%)).
- **How we got here:** Java's HTTP client in the generator (Collapsed near 10,000 requests/s: lock contention and a burst of new connections exhausted Windows' 16,384 local ports. Replaced by Jetty (8 instances, 2,000 connections)); Connection churn (Request timeouts and idle timeouts each closed connections and reopened them in bursts. Fixed with a fixed pool (no idle closing on either side) and a waiting line sized to ~4 s, far below the timeout); Generator and backend freezes (Tomcat's low-priority utility threads starved under load on Windows; `-XX:-UseThreadPriorities`. Thread dumps froze the generator for up to 12 s; use Java Flight Recorder instead); Warm-up (Without it, the first second at full rate opened thousands of connections at once); Virtual threads and a separate intake producer (The 200-thread pool ran out around 600k/min; customer sends no longer wait behind the relay).
- **Testing reliably:** close heavy programs, restart apps after changes, keep the 30 s warm-up, let the pipeline
  drain between runs, and never take thread dumps of the generator mid-run. Use Java Flight Recorder instead.

## Architecture FAQ

Organised by component. For each: what it is for, then every lever it has: where it is set (the dashboard, or a property for the next restart), what it trades, and what was measured here. Defaults are in brackets.

### Generator

Plays the customers: sends submissions on a fixed schedule, records what each was told, and checks the result.

**What is the generator for, and why is it open-loop?**
It models real customers, who keep submitting whatever the backend's speed. It never waits for replies before sending the next submission, so overload shows up as it would in production: as slow replies, skips and unknown outcomes, not as a politely slower test. It also holds the client's side of the record, which is why the checker lives here.

**Rate, duration and "Run until stopped"**
Dashboard [50/min, 1 min; presets 100k, 200k, 500k]. The demand. Up to 1,000,000 per minute for up to 60 minutes, or until stopped. Memory stays flat however long it runs, so endless runs show whether anything builds up over time. Measured: the system keeps up to about 9,000–11,500 per second (550k–700k per minute); above that the generator skips what does not fit.

**Warm-up**
Dashboard [30 s]. Ramps from zero to the full rate against an endpoint that takes the real Kafka path but stores nothing, so the run starts on open connections, compiled code and a warm producer. 0 tests a cold start. Measured: without it, even 500k failed in the first second, opening thousands of connections at once.

**In-flight limit**
Dashboard [no limit]. Caps submissions awaiting a reply; above it a submission is skipped, not queued. A blunt client-side brake: it protects the backend but turns excess demand into skips at a fixed concurrency rather than a fixed wait.

**HTTP client**
Dashboard [Jetty]. Jetty: 8 instances sharing a fixed pool of 2,000 connections. Java's built-in client: 8 instances, collapses near 10,000 per second (lock contention and connection bursts that exhaust Windows' local ports). Config: `generator.jetty.clients` [8] (16 measured worse), `max-connections` [2,000], `selectors` [1].

**Waiting line (`generator.jetty.max-queued`)**
Config [40,000, about 4 s of work]. Requests wait here for a free connection; with no room they are skipped, never sent. Sized by time, not kept short: it must absorb stalls (the warm-up switch, a GC pause) yet stay far below the 15 s request timeout, because each timeout closes a connection and holds a port for 2 minutes. Measured: 100,000 let waits reach the timeout; 10,000 turned every brief stall into skips, even at 500k.

**Connection lifetime (`generator.jetty.idle-timeout`)**
Config [0: never closed]. A fixed pool: opened gradually during the warm-up, then kept. Measured: a 30 s idle timeout shrank the pool in calm periods, and the next slowdown opened 874 connections in one second; about 2,750 requests were refused. The dashboard's Connections card shows opened and closed after the warm-up; both should stay 0.

**Retry unknown outcomes (during the run)**
Dashboard [off]. Resends an Unknown outcome with the same ID, up to 5 attempts, waits doubling from 0.5 s to 8 s with jitter (`generator.retry.*`). Safe because the backend is idempotent, but it adds load during overload, and a submission holds its in-flight slot until its final outcome.

**Retry period (after the run)**
Dashboard [off; 120 s], or Retry now. Resends everything still Skipped, Blocked or Unknown in passes at the run's rate, until none is left, the time limit, 10 passes, a pass that changes nothing, or Stop. Keeps the run's measured time clean while still finishing the job. Measured: a 1M run accepted 739,711 during the run; the retry period recovered the other 260,289 in 23 s.

**Pause when the backend is down (circuit breaker)**
Dashboard [on]. After 20 replies in a row that never reached the backend (refused, broken or timed out), the generator stops sending, waits 1 s (doubling up to 10 s while the backend stays away), sends one probe, and when the backend answers ramps back up over 5 s. The schedule keeps running: submissions due meanwhile are skipped with the reason, and the retry period can send them later. Without it, a down backend is hit at full rate with requests that all fail, and a returning one with a wall of connections and retries.

**How are connection errors classified?**
Not a lever but a rule: "connection refused" means the request never reached the backend, so it is **Skipped** (not sent, nothing stored) and safe to resend. A connection that breaks mid-request, or a timeout, is **Unknown**: the backend may have it. Only Unknowns need the checker to say what really happened.

**Duplicate submissions**
Dashboard [0%]. Sends up to 50% of submissions a second time with the same ID and content, 0–2 s later, like a double click. Tests the system's idempotency; the checker verifies each still has one case, processed once.

**Intake path and Connection test**
Dashboard (Backend card) [through Kafka]. Chooses the endpoint the generator calls: `POST /api/cases` (Kafka, 202), `/direct` (saved before the reply, 201), or `/noop` (instant 202, nothing stored). The connection test measures the generator and the backend's web server alone: about 15,000 per second from the generator, 25,210 from the native tool `oha`.

### Backend

The only writer of PostgreSQL: accepts submissions, saves them, hands them to processing through the outbox, and records results.

**What is the backend for, and where does it wait?**
It turns a submission into a promise (202 after Kafka confirms) and later into a case. Each intake request waits for Kafka's confirmation, so the backend's capacity is about how many requests can wait at once and how fast confirmations come back, not about CPU: it used 3–4 of 20 cores at its ceiling.

**Load shedding**
Dashboard [off; 1,000 at once]. Over the limit, a filter answers 503 with `Retry-After: 1` before doing any work: **Blocked**, a definite "nothing stored, try later". Turns overload into fast, safe refusals instead of slow replies. The limit should sit just above what the backend can have in progress at full speed.

**Process after intake**
Dashboard (Processor card) [on]. Pauses the case writer, the outbox relay and the result writer (and the processor). Intake keeps accepting; everything waits in Kafka and catches up on resume. Isolates intake for measurement. Measured: pausing it did not raise intake (about 8,700/s either way).

**Request threads (`spring.threads.virtual.enabled`)**
Config [on]. A virtual thread per request, so thousands can wait for Kafka cheaply. Measured: Tomcat's 200-thread pool ran out near 600k per minute (200 threads / ~20 ms per confirmation).

**Connections (`server.tomcat.*`)**
Config. `accept-count` [2,000] is the queue of connections waiting to be accepted (Windows may cap it lower); `max-keep-alive-requests` [-1] and `keep-alive-timeout` / `connection-timeout` [-1] mean a connection is never closed by the backend, matching the generator's fixed pool. Fine for a known client, not for the internet.

**Kafka confirmation timeout (`caseroute.kafka.publish-timeout`)**
Config [10 s]. How long an intake request waits for Kafka before answering 503 (Unknown to the client: the message may still have been written).

**Graceful shutdown (`server.shutdown`)**
Config [graceful, up to 20 s]. On a normal stop the web server refuses new connections, lets requests in progress finish and reply, then Kafka and the database close. A planned restart then leaves no request in doubt; without it, requests in progress were cut off.

**Downtime test**
Dashboard (Backend card): **Stop gracefully** or **Crash**, then restart the backend from IntelliJ. Graceful: every request in progress gets its reply; clients only see refused connections (not sent). Crash: the process exits instantly; requests in progress get no reply and are Unknown, and the checker shows how many were saved anyway (Kafka had them) and that none became a second case after the retry period resent them.

**Case writer (`spring.kafka.listener.concurrency`, `max-poll-records`)**
Config [3 consumers, 500 per batch]. Reads `cases.submitted` and saves each batch in one statement, duplicates ignored. Three consumers means more than 3 partitions add no saving parallelism.

**Outbox relay (`caseroute.outbox.batch-size`, `poll-interval-ms`)**
Config [batches of 1,000, every 100 ms]. Publishes saved-case events to `cases.saved`; while batches come back full it keeps going instead of waiting for the next tick. Bigger batches mean fewer round trips to PostgreSQL and Kafka. The dashboard's "awaiting relay" count shows it falling behind.

### Kafka

The durable buffer between every stage: the promise behind a 202, and the backlog when a later stage is slower or paused.

**What is Kafka for here?**
It decouples speeds. Intake only needs Kafka's confirmation; saving and processing read at their own pace. One broker in Docker; topics `cases.submitted`, `cases.saved`, `cases.processed`, `cases.warmup` and `.DLT` topics for unreadable messages. Measured: the broker took 185,528 confirmed writes per second on its own, so it is not the limit.

**Intake producers: count**
Dashboard (Kafka panel, between runs) [1]. Each producer has one network thread, one connection and up to 5 requests awaiting confirmation; submissions are spread over them in turn. Measured: 4 producers were worse (about 7,500 per second against 9,000).

**Linger and batch size**
Kafka panel [5 ms, 16 KB; resets on backend restart]. How long the producer waits to fill a batch, and how big one may get. Bigger batches mean fewer requests and fewer wake-ups of the producer's network thread, but every submission waits up to the linger time. Measured at 900k: 25 ms with 64 KB gave about 9% more than the defaults; 50 ms with 256 KB was worse.

**Compression**
Kafka panel [none]. Shrinks batches at the cost of CPU on the backend and the broker. Helps when bytes are the limit; here the broker is local and messages are small. Not measured.

**Requests in flight**
Kafka panel [5, the maximum with idempotence]. Requests per producer awaiting confirmation. Lower only to test the effect; 5 is the safe maximum.

**Delivery guarantee (`acks=all`, idempotence)**
Fixed on purpose. A 202 is sent only after the broker has the message, and a producer retry never writes it twice. This is what makes an accepted submission certain to be stored; turning it off would be faster and would break the promise.

**Partitions**
Kafka panel, add-only [6 per topic]. Units of parallelism for consumers, and how producer batches are split. Kafka cannot remove partitions: going back means recreating the topic. Measured: 12 partitions were slower than 6 for intake (smaller batches per request), and the backend reads with 3 consumers, the processor with 6.

**Retention (`caseroute.kafka.retention`, `segment`)**
Config [6 h, 1-hour segments; warm-up 1 h]. How long messages stay, read or not. Long enough to replay a run's backlog, short enough that load tests do not fill the disk.

**Live producer metrics: how do I read them?**
Kafka panel. High "waiting in producer" with requests in flight at the cap: the producer is the bottleneck. High "broker confirmation" with the broker idle: the time is lost between the producer and the broker. Measured: confirmations grew from ~5 ms to ~150 ms under load while Kafka's CPU stayed low; a recording traced it to the producer's network thread blocked on a lock in Java's Windows networking code (about 25% of the time).

### Processing

The slow, failure-prone downstream system: turns a saved case into a routed or failed one, through Kafka only.

**What is the processor for, and why is it separate?**
It models real downstream work with its own speed and failures, so the rest of the system must tolerate both. It has no database access: it reads `cases.saved`, works, and publishes to `cases.processed`; the backend records the result, only on a case still OPEN, and counts every result it receives.

**Concurrency (`caseroute.processing.consumers`, `max-in-flight`)**
Config [6 consumers, 20,000 cases in flight]. Each case runs on its own virtual thread, so throughput is about in-flight / time per case (~18,000 per second at ~1.1 s). When the limit is reached it stops fetching and the backlog waits in Kafka. Offsets only advance past finished cases, so a crash redelivers unfinished ones.

**Simulated work (`caseroute.processing.*`)**
Config [~1 s per case (0.5–1.5 s); 1% take 10 s; 2% transient errors retried after 500 ms, up to 3 attempts; 0.1% fail permanently]. Each case's behaviour is derived from its ID, so a redelivered case behaves the same and runs are reproducible.

**Process after intake**
Dashboard [on]. Pauses fetching from `cases.saved`; cases in progress finish and the backlog waits in Kafka. Driven together with the backend's pause.

**Failure handling**
Not a lever but a design choice: transient errors are retried inside the processor; permanent failures produce a FAILED result and a copy in `cases.saved.DLT`. A duplicate result is harmless for the case, and visible to the checker as "processed more than once".

### Across the system

Promises that no single component keeps alone.

**What does a 202 promise, and how is it checked?**
That the submission is durably in Kafka and will become a case. The checker's "Accepted but missing" counts broken promises and must be 0; "Unknown, saved anyway" shows replies that were lost while the case was not.

**How do duplicates stay harmless end to end?**
The client chooses the ID. A unique index keeps one case; only a newly saved case gets an outbox row, so a repeat never reaches processing; results update only an OPEN case. The checker counts cases created twice and cases processed more than once; both must be 0.

**What happens when the backend goes down?**
With one backend instance, intake stops: Kafka sits behind the backend, so nothing reaches it. Clients see refused connections (not sent) and, for requests in progress at the moment of a crash, no reply (Unknown). What limits the damage: graceful shutdown for planned stops, the generator's circuit breaker (pause, probe, ramp up), the retry period resending with the original IDs, and idempotency, so a resend of a request that was in fact saved does not create a second case. Staying available during an outage needs a second instance behind a load balancer.

**Why the outbox?**
Saving a case and announcing it are writes to two systems. The outbox row commits with the case, and a relay publishes it afterwards, so a saved case is never left unannounced. The price is at-least-once delivery, which every consumer tolerates.

**What is deliberately not production-grade?**
One Kafka broker with replication factor 1: while it is down, intake stops (503, Unknown to the client), and losing its disk would lose accepted submissions not yet saved. One backend instance. Everything, including the load generator, on one Windows machine. Connections never closed for being idle. No authentication. Schema managed by Hibernate (`ddl-auto=update`), not migrations.

**How would it scale beyond one machine?**
Several backend instances behind a load balancer, each with its own producers. A Kafka cluster with replication 3 and `min.insync.replicas=2`. More partitions together with more consumers. The load generator on separate machines. Servers on Linux.
