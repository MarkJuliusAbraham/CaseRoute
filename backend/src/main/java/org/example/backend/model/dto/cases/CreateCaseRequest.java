package org.example.backend.model.dto.cases;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateCaseRequest(
        @NotBlank
        @Size(max = 200)
        String subject,

        @NotBlank
        String description
) {
}
