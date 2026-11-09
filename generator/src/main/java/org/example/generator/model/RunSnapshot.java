package org.example.generator.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record RunSnapshot(UUID runId, RunSettings settings, String state, Instant startedAt,
        Instant finishedAt, long planned, long dispatched, long accepted, long failed,
        long unknown, long skipped, long notSent, int inFlight, String error,
        List<SubmissionResult> recentResults) {
}
