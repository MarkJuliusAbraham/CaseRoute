package org.example.generator.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The checker's comparison of what the client was told against what the system recorded.
 *
 * @param verdict           CONSISTENT: every accepted submission is saved and processed, and nothing else is
 *                          wrong. PENDING: no discrepancies, but some accepted cases are still being processed.
 *                          DISCREPANCIES: something the client was promised is missing, or something was saved
 *                          that the client was told was rejected or never sent.
 * @param accepted          submissions the client received an accepted reply for
 * @param unknown           submissions whose final outcome the client never learned
 * @param rejectedButSaved  told "rejected" (4xx or mismatched reply), yet a case exists. Should be 0.
 * @param blocked           turned away by load shedding: total, and how many have a case anyway (should be 0)
 * @param unexpected        cases for this run that were never sent, or that the generator never planned. Should be 0.
 * @param examples          up to 10 client request IDs per problem category, for looking up individually
 */
public record CheckReport(UUID runId, Instant checkedAt, String verdict, String summary, long casesInDatabase,
        Accepted accepted, Unknown unknown, long rejectedButSaved, Blocked blocked, long unexpected,
        Map<String, List<UUID>> examples) {

    /**
     * @param processed          saved and processed (ROUTED or FAILED)
     * @param awaitingProcessing saved, still OPEN: in the outbox, in cases.saved, or being processed
     * @param missing            no case saved: lost, or (if checked too early) still waiting in Kafka
     */
    public record Accepted(long total, long processed, long awaitingProcessing, long missing) { }

    /**
     * @param saved    the backend has it after all; the client just never got the reply
     * @param notSaved never received; only a retry would have recovered it
     */
    public record Unknown(long total, long saved, long notSaved) { }

    /** Blocked by load shedding: definitely not stored, so {@code saved} should always be 0. */
    public record Blocked(long total, long saved) { }
}
