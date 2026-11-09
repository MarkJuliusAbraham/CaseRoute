package org.example.generator.model;

import java.time.Instant;
import java.util.UUID;

/**
 * The final outcome of one submission. {@code attempts} is how many times it was sent (more than 1 when
 * retries resent an unknown outcome); latency and status describe the last attempt.
 */
public record SubmissionResult(UUID clientRequestId, String outcome, Integer httpStatus,
        String caseId, double latencyMs, double schedulingDelayMs, String detail, Instant recordedAt,
        int attempts) {

    public SubmissionResult withAttempts(int attempts) {
        return new SubmissionResult(clientRequestId, outcome, httpStatus, caseId, latencyMs, schedulingDelayMs,
                detail, recordedAt, attempts);
    }
}
