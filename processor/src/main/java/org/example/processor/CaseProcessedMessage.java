package org.example.processor;

import java.time.Instant;
import java.util.UUID;

/**
 * The result of processing one case, published to cases.processed. The backend consumes it and records it on
 * the case row.
 *
 * @param outcome       ROUTED or FAILED
 * @param team          assigned team when ROUTED, otherwise null
 * @param failureReason why it FAILED, otherwise null
 */
public record CaseProcessedMessage(UUID caseId, UUID clientRequestId, UUID testRunId, String outcome, String category,
        String team, int attempts, String failureReason, Instant processedAt) {
}
