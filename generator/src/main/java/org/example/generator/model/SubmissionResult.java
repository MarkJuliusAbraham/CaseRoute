package org.example.generator.model;

import java.time.Instant;
import java.util.UUID;

public record SubmissionResult(UUID clientRequestId, String outcome, Integer httpStatus,
        String caseId, double latencyMs, double schedulingDelayMs, String detail, Instant recordedAt) {
}
