package org.example.generator.model;

import java.util.UUID;

/**
 * A submission is fully determined by its run and its sequence number (1, 2, 3, ...), so a run never has to keep a
 * list of them: {@link #forRun} rebuilds any submission on demand, and {@link #sequenceOf} turns a clientRequestId
 * back into its sequence number. That keeps memory flat however long a run lasts.
 *
 * <p>The clientRequestId is the run id with the sequence number added to its low 64 bits. Different runs have
 * different random run ids, so their ids cannot collide; within a run, ids are distinct by construction.
 */
public record Submission(UUID clientRequestId, UUID testRunId, String subject, String description) {
    public static Submission forRun(UUID runId, long sequence) {
        return new Submission(idFor(runId, sequence), runId, "Load test case " + sequence,
                "Synthetic submission " + sequence + " for run " + runId);
    }

    public static UUID idFor(UUID runId, long sequence) {
        return new UUID(runId.getMostSignificantBits(), runId.getLeastSignificantBits() + sequence);
    }

    /** The sequence number of a clientRequestId from this run, or -1 if it did not come from this run's numbering. */
    public static long sequenceOf(UUID runId, UUID clientRequestId) {
        if (clientRequestId.getMostSignificantBits() != runId.getMostSignificantBits()) return -1;
        long sequence = clientRequestId.getLeastSignificantBits() - runId.getLeastSignificantBits();
        return sequence >= 1 ? sequence : -1;
    }
}
