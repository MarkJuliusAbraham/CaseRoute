package org.example.backend.model.dto.cases;

import org.example.backend.model.enums.CaseStatus;

import java.time.Instant;
import java.util.UUID;

public record CaseResponse(
        UUID id,
        String subject,
        String description,
        CaseStatus status,
        Instant createdAt
) {
}
