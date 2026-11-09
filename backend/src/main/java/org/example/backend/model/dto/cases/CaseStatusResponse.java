package org.example.backend.model.dto.cases;

import org.example.backend.model.entity.ServiceCase;
import org.example.backend.model.enums.CaseStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * What a client learns by looking up its submission: whether it became a case, and how far that case got.
 * OPEN means saved and waiting for processing; ROUTED or FAILED means the processor's result is recorded.
 */
public record CaseStatusResponse(
        UUID caseId,
        UUID clientRequestId,
        UUID testRunId,
        CaseStatus status,
        String category,
        String assignedTeam,
        Integer processingAttempts,
        Instant createdAt,
        Instant processedAt
) {
    public static CaseStatusResponse from(ServiceCase value) {
        return new CaseStatusResponse(value.getId(), value.getClientRequestId(), value.getTestRunId(),
                value.getStatus(), value.getCategory(), value.getAssignedTeam(), value.getProcessingAttempts(),
                value.getCreatedAt(), value.getProcessedAt());
    }
}
