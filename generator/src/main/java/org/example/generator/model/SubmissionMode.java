package org.example.generator.model;

/** Which backend write path a run exercises; each has its own endpoint. */
public enum SubmissionMode {
    /** {@code POST /api/cases}: stored in Kafka, replies 202; a consumer saves it to Postgres later. */
    KAFKA,
    /** {@code POST /api/cases/direct}: saved to Postgres inside the request, replies 201 with the case. */
    DIRECT;

    /** Path appended to the configured target URL ({@code generator.target-url}). */
    public String pathSuffix() { return this == DIRECT ? "/direct" : ""; }
}
