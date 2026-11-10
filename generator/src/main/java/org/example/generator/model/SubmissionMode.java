package org.example.generator.model;

/** Which backend write path a run exercises; each has its own endpoint. */
public enum SubmissionMode {
    /** {@code POST /api/cases}: stored in Kafka, replies 202; a consumer saves it to Postgres later. */
    KAFKA,
    /** {@code POST /api/cases/direct}: saved to Postgres inside the request, replies 201 with the case. */
    DIRECT,
    /**
     * Connection test, {@code POST /api/cases/noop}: the backend replies 202 immediately and does nothing else
     * (no Kafka, no database). Measures how much traffic the generator and the backend's HTTP layer can carry.
     */
    NOOP;

    /** Path appended to the configured target URL ({@code generator.target-url}). */
    public String pathSuffix() {
        return switch (this) {
            case KAFKA -> "";
            case DIRECT -> "/direct";
            case NOOP -> "/noop";
        };
    }
}
