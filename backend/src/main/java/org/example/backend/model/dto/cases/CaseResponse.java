package org.example.backend.model.dto.cases;

import org.example.backend.model.enums.CaseStatus;

import java.time.Instant;
import java.util.UUID;

public record CaseResponse(
        UUID id,
        UUID clientRequestId,
        UUID testRunId,
        String subject,
        String description,
        CaseStatus status,
        Instant createdAt
) {
    public static CaseResponse from(org.example.backend.model.entity.ServiceCase value) {
        return new CaseResponse(value.getId(), value.getClientRequestId(), value.getTestRunId(),
                value.getSubject(), value.getDescription(), value.getStatus(), value.getCreatedAt());
    }
}
