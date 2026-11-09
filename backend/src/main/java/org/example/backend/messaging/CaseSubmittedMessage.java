package org.example.backend.messaging;

import java.time.Instant;
import java.util.UUID;

/** A case submission the API has accepted, as stored in Kafka until a consumer saves it. */
public record CaseSubmittedMessage(
        UUID clientRequestId,
        UUID testRunId,
        String subject,
        String description,
        Instant acceptedAt
) {
}
