package org.example.generator.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/**
 * @param retryEnabled resend submissions whose outcome is unknown (timeout, connection error, 5xx), with
 *                     backoff; see {@link org.example.generator.client.RetryPolicy}. Off by default.
 * @param warmupSeconds before the measured run, ramp traffic from zero to the full rate over this many seconds
 *                      against the backend's warm-up endpoint, which stores nothing. 0 (default) skips it.
 */
public record RunSettings(
        @Min(1) @Max(1_000_000) int requestsPerMinute,
        @Min(1) @Max(60) int durationMinutes,
        @Min(1) @Max(500) Integer maxInFlight,
        SubmissionMode submissionMode,
        Boolean retryEnabled,
        @Min(0) @Max(300) Integer warmupSeconds) {

    public RunSettings {
        if (submissionMode == null) submissionMode = SubmissionMode.KAFKA;
        if (retryEnabled == null) retryEnabled = false; // older clients omit it
        if (warmupSeconds == null) warmupSeconds = 0;
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight) {
        this(requestsPerMinute, durationMinutes, maxInFlight, SubmissionMode.KAFKA, false, 0);
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight, SubmissionMode mode) {
        this(requestsPerMinute, durationMinutes, maxInFlight, mode, false, 0);
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight, SubmissionMode mode,
            boolean retryEnabled) {
        this(requestsPerMinute, durationMinutes, maxInFlight, mode, retryEnabled, 0);
    }

    /** Warm-up requests: the ramp averages half the full rate, so half a full-rate interval's worth. */
    public long warmupPlanned() {
        return (long) requestsPerMinute * warmupSeconds / 120;
    }

    public long planned() {
        return (long) requestsPerMinute * durationMinutes;
    }
}
