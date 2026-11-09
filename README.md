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

Open http://localhost:5173. The frontend proxies generator API calls and SSE streams to port 8081.
The generator sends synthetic submissions directly to the backend on port 8080.
Closing the UI does not stop a run. Only the generator currently has dashboard controls;
processing and checking are future capabilities.

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
records every garbage-collection and safepoint pause to `backend/gc.log` (rotated, 5 files of 20 MB).
This is on when started with `.\mvnw.cmd spring-boot:run` or the IntelliJ run configuration
**Backend (GC logging)** (in `.run/`).

After a run with refused connections, list pauses over 50 ms and compare their times with the stall:

```bash
grep -E "Pause|Total: " backend/gc.log | awk 'match($0,/([0-9.]+)ms$/,m) && m[1]>50 {print} match($0,/Total: ([0-9]+) ns/,t) && t[1]>50000000 {print}'
```

A pause of the stall's length at the stall's time confirms GC (or another JVM safepoint). No such pause rules it out.

In a long safepoint line, compare `Reaching safepoint` with `At safepoint`. A large `Reaching safepoint` means the
JVM was waiting for one thread to stop, not doing GC work. The backend also runs with
`-XX:+SafepointTimeout -XX:SafepointTimeoutDelay=500`, which reports any thread that takes more than 500 ms to
stop. Find those reports with:

```bash
grep -n -i -A15 "timeout detected\|safepoint timeout" backend/gc.log
```

The report names the thread (and its state), which shows what kept the whole backend frozen.

Finding so far: on Windows, the frozen-backend stalls were Tomcat's `Catalina-utility` threads. Tomcat runs them at
the lowest Java priority, which Windows honours, so under full CPU load they got no CPU time and could not reach a
safepoint for 0.5–5 s while every other thread waited. The backend therefore runs with `-XX:-UseThreadPriorities`,
which gives all threads normal OS priority. Linux already ignores Java priorities by default, so this matches typical
production behaviour.

The generator is also a Spring Boot/Tomcat app and froze the same way, sending nothing for up to 5 s and then a burst
of overdue requests. Its `pom.xml` uses the same flags (writing `generator/gc.log`). In IntelliJ, set the same VM
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
  changes nothing; unusable results go to `cases.processed.DLT`.
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
- **Documentation** for every application is in the dashboard's Documentation tab (top right).
