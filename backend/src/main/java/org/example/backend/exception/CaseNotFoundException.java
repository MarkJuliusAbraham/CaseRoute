package org.example.backend.exception;

import java.util.UUID;

public class CaseNotFoundException extends RuntimeException {

    public CaseNotFoundException(UUID id) {
        super("Case not found: " + id);
    }

    private CaseNotFoundException(String message) {
        super(message);
    }

    /** No case was saved for this submission (yet): it was never received, rejected, or is still queued. */
    public static CaseNotFoundException forRequest(UUID clientRequestId) {
        return new CaseNotFoundException("No case has been saved for client request " + clientRequestId
                + ". If the submission was accepted (202), it may still be waiting in Kafka; check again shortly.");
    }
}
