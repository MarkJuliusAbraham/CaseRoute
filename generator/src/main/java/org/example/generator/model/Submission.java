package org.example.generator.model;

import java.util.UUID;

public record Submission(UUID clientRequestId, UUID testRunId, String subject, String description) {
    public static Submission forRun(UUID runId, long sequence) {
        return new Submission(UUID.randomUUID(), runId, "Load test case " + sequence,
                "Synthetic submission " + sequence + " for run " + runId);
    }
}
