package org.example.backend.exception;

import java.util.UUID;

public class CaseNotFoundException extends RuntimeException {

    public CaseNotFoundException(UUID id) {
        super("Case not found: " + id);
    }
}
