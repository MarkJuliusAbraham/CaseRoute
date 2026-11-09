package org.example.backend.exception;

/** Kafka did not confirm the write in time, so the submission was not acknowledged. */
public class SubmissionUnavailableException extends RuntimeException {
    public SubmissionUnavailableException(Throwable cause) {
        super("The submission could not be stored. Retry with the same clientRequestId.", cause);
    }
}
