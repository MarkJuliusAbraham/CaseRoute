package org.example.generator.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/**
 * @param retryEnabled resend submissions whose outcome is unknown (timeout, connection error, 5xx), with
 *                     backoff; see {@link org.example.generator.client.RetryPolicy}. Off by default.
 * @param httpClient    which HTTP client sends the run: JETTY (default) or JDK, Java's built-in client, which
 *                      collapses near 10,000 requests/s.
 * @param warmupSeconds before the measured run, ramp traffic from zero to the full rate over this many seconds
 *                      against the backend's warm-up endpoint, which stores nothing. 0 (default) skips it.
 * @param endless       keep sending at the rate until stopped; {@code durationMinutes} is then ignored.
 */
public record RunSettings(
        @Min(1) @Max(1_000_000) int requestsPerMinute,
        @Min(1) @Max(60) int durationMinutes,
        @Min(1) @Max(500) Integer maxInFlight,
        SubmissionMode submissionMode,
        Boolean retryEnabled,
        @Min(0) @Max(300) Integer warmupSeconds,
        HttpClientKind httpClient,
        Boolean endless) {

    public RunSettings {
        if (submissionMode == null) submissionMode = SubmissionMode.KAFKA;
        if (retryEnabled == null) retryEnabled = false; // older clients omit it
        if (warmupSeconds == null) warmupSeconds = 0;
        if (httpClient == null) httpClient = HttpClientKind.JETTY;
        if (endless == null) endless = false;
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight, SubmissionMode mode,
            Boolean retryEnabled, Integer warmupSeconds, HttpClientKind httpClient) {
        this(requestsPerMinute, durationMinutes, maxInFlight, mode, retryEnabled, warmupSeconds, httpClient, false);
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight) {
        this(requestsPerMinute, durationMinutes, maxInFlight, SubmissionMode.KAFKA, false, 0, null);
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight, SubmissionMode mode) {
        this(requestsPerMinute, durationMinutes, maxInFlight, mode, false, 0, null);
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight, SubmissionMode mode,
            boolean retryEnabled) {
        this(requestsPerMinute, durationMinutes, maxInFlight, mode, retryEnabled, 0, null);
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight, SubmissionMode mode,
            boolean retryEnabled, int warmupSeconds) {
        this(requestsPerMinute, durationMinutes, maxInFlight, mode, retryEnabled, warmupSeconds, null);
    }

    /** Warm-up requests: the ramp averages half the full rate, so half a full-rate interval's worth. */
    public long warmupPlanned() {
        return (long) requestsPerMinute * warmupSeconds / 120;
    }

    public long planned() {
        return (long) requestsPerMinute * durationMinutes;
    }
}
