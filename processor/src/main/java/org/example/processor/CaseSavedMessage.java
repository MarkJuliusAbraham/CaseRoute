package org.example.processor;

import java.time.Instant;
import java.util.UUID;

/** The cases.saved event published by the backend's outbox relay. */
public record CaseSavedMessage(long outboxId, UUID caseId, UUID clientRequestId, UUID testRunId, Instant savedAt) {
}
