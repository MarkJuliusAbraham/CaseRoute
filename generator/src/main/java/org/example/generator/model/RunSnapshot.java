package org.example.generator.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Counts are per submission, except {@code retryRequests}, which counts extra HTTP requests sent by retries.
 * {@code retried} submissions needed at least one retry; {@code recovered} of them ended accepted.
 * Warm-up requests (state WARMING_UP) store nothing and are counted only in the {@code warmup*} fields.
 */
public record RunSnapshot(UUID runId, RunSettings settings, String state, Instant startedAt,
        Instant finishedAt, long planned, long dispatched, long accepted, long failed,
        long unknown, long skipped, long notSent, long retried, long recovered, long retryRequests,
        long warmupPlanned, long warmupSent, long warmupFailed,
        int inFlight, String error, List<SubmissionResult> recentResults) {
}
