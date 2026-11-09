# CaseRoute

A system design case study in reliable customer-service case submission and processing.

## Project structure

- `backend/`: Spring Boot case API and PostgreSQL persistence (port 8080).
- `generator/`: Spring Boot traffic-generation API and SSE progress (port 8081).
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
