package org.example.generator.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public record RunSettings(
        @Min(1) @Max(1_000_000) int requestsPerMinute,
        @Min(1) @Max(60) int durationMinutes,
        @Min(1) @Max(500) Integer maxInFlight,
        SubmissionMode submissionMode) {

    public RunSettings {
        if (submissionMode == null) submissionMode = SubmissionMode.KAFKA;
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight) {
        this(requestsPerMinute, durationMinutes, maxInFlight, SubmissionMode.KAFKA);
    }

    public long planned() {
        return (long) requestsPerMinute * durationMinutes;
    }
}
