# CaseRoute processor

Consumes `cases.saved` events (published by the backend's outbox relay), simulates processing each case, and
publishes the result to `cases.processed`: `ROUTED` with a category and team, or `FAILED`. The backend consumes
`cases.processed` and records the result on the case row. The processor has no database access.

Run from this folder (start the backend first, so the schema exists):

```powershell
..\backend\mvnw.cmd spring-boot:run
```

Port 8083 (health: `/actuator/health`). Needs only Kafka; the backend creates the topics.

## How it behaves

- `CaseConsumerLoop` runs `caseroute.processing.consumers` Kafka consumers (one platform thread each, useful up to
  the partition count). Each loops: fetch, start every case on its own virtual thread, commit, fetch again.
- **Fetching never waits for slow cases.** Cases finish in any order. Kafka stores one committed position per
  partition ("everything before this is done"), so `PartitionProgress` commits only up to the oldest case still
  running. A slow case delays the commit, not other cases. After a crash, everything from the committed position
  is redelivered: unfinished cases are never skipped, and a few finished ones may run again (harmless, below).
- `caseroute.processing.max-in-flight` (20,000) caps cases in progress. When a full fetch no longer fits, the
  consumers pause their partitions and the backlog waits in Kafka. Throughput is about
  max-in-flight / average case time (~1.1 s), so ~18,000 cases/s before Kafka, the backend or the CPU limit it.
- A case is finished only after its result is confirmed by Kafka. If Kafka is unavailable, publishing is retried
  every second, so results are delayed, not lost. A duplicate result is harmless: the backend only updates `OPEN`
  cases.
- Timing and failures come from `ProcessingPlan`, derived from the case's `clientRequestId` (reproducible).
  Defaults: ~1 s per case (0.5-1.5 s), 1% take 10 s, 2% hit a transient error and retry (up to 3 attempts),
  0.1% fail permanently and go to `cases.saved.DLT`.

Earlier versions used Spring Kafka's async acks, which pause each consumer until every case from its previous
fetch is done. One 10-second case then held up its whole batch, capping throughput at ~250 cases/s.
