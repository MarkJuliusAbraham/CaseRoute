package org.example.backend.model.dto.cases;

/**
 * Pipeline counts for one run (or all cases): saved rows by status, plus outbox rows not yet published.
 * {@code open} cases are saved but not yet processed; {@code awaitingRelay} have not reached Kafka yet.
 */
public record CaseStats(long saved, long open, long routed, long failed, long awaitingRelay) {
}
