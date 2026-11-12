package org.example.generator.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Counts are per submission, except {@code retryRequests}, which counts extra HTTP requests sent by retries.
 * {@code retried} submissions needed at least one retry; {@code recovered} of them ended accepted.
 * Warm-up requests (state WARMING_UP) store nothing and are counted only in the {@code warmup*} fields.
 * {@code blocked}: turned away by the backend's load shedding (503 + Retry-After), so definitely not stored;
 * {@code duplicates}: second sends of the same submission (see {@code duplicatePercent}): how many were sent, accepted
 * (the backend acknowledged the repeat, which must not create a second case), and anything else.
 * {@code circuit}: the circuit breaker (null when turned off): its state, since when, how often it opened, and how
 * many submissions it held back (counted as skipped).
 * {@code retry} is the latest retry period after the run (state RETRYING while it runs), or null: passes over every
 * submission still Skipped, Blocked or Unknown, which replace those outcomes in the counts above.
 * {@code connectionsOpened}/{@code connectionsClosed}: connections the Jetty client opened and closed since the
 * measured run started (after the warm-up); both stay flat in a healthy run. {@code connectionsOpen}: open now.
 * Always 0 with the built-in Java client, which does not report them. In an endless run {@code planned} is the
 * number of submissions handled so far.
 */
public record RunSnapshot(UUID runId, RunSettings settings, String state, Instant startedAt,
        Instant finishedAt, long planned, long dispatched, long accepted, long failed,
        long unknown, long skipped, long notSent, long blocked, long retried, long recovered, long retryRequests,
        long warmupPlanned, long warmupSent, long warmupFailed,
        int inFlight, long connectionsOpened, long connectionsClosed, long connectionsOpen, String error, Duplicates duplicates,
        org.example.generator.service.CircuitBreaker.Status circuit, org.example.generator.service.LoadRun.RetryPeriod retry,
        List<SubmissionResult> recentResults) {

    /** Second sends of submissions: sent, accepted, and any other reply (skipped, unknown, rejected...). */
    public record Duplicates(long sent, long accepted, long other) { }
}
