package org.example.backend.model.dto.cases;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.NotNull;
import java.util.UUID;

public record CreateCaseRequest(
        @NotNull UUID clientRequestId,
        UUID testRunId,
        @NotBlank
        @Size(max = 200)
        String subject,

        @NotBlank
        String description
) {
}
