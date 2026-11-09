package org.example.backend.model.dto.cases;

import java.time.Instant;
import java.util.UUID;

/**
 * Returned with 202 Accepted: the submission is durably stored in Kafka and will be saved as a case.
 * Resubmitting the same clientRequestId is safe and never creates a second case.
 */
public record SubmissionAcceptedResponse(
        UUID clientRequestId,
        UUID testRunId,
        String status,
        Instant acceptedAt
) {
    public static final String ACCEPTED = "ACCEPTED";
}
