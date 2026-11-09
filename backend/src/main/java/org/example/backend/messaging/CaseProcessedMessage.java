package org.example.backend.messaging;

import java.time.Instant;
import java.util.UUID;

/** A processing result published by the processor application to cases.processed. */
public record CaseProcessedMessage(UUID caseId, UUID clientRequestId, UUID testRunId, String outcome, String category,
        String team, int attempts, String failureReason, Instant processedAt) {
}
