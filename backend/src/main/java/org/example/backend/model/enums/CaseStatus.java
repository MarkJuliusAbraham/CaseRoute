package org.example.backend.model.enums;

public enum CaseStatus {
    OPEN,
    /** Classified and assigned to a team by the processor. */
    ROUTED,
    /** The processor gave up on it; details are in the cases.saved.DLT topic. */
    FAILED,
    IN_PROGRESS,
    RESOLVED
}
