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
 * @param duplicatePercent share of submissions (0 to 50%) that are also sent a second time, with the same clientRequestId
 *                      and content, 0 to 2 s after the first: a customer double-clicking or resubmitting. The
 *                      duplicate is extra traffic; the submission's outcome is still its first send's. Default 0.
 * @param circuitBreaker pause sending while the backend is unreachable (20 replies in a row without any HTTP
 *                      status), probe it, and ramp back up when it answers; submissions due meanwhile are skipped with
 *                      the reason. On by default; see {@link org.example.generator.service.CircuitBreaker}.
 * @param retryPeriod   after a run that finished by itself, retry every submission still Skipped, Blocked or Unknown
 *                      in passes, until none is left or {@code retryPeriodSeconds} pass (default 120). Off by default.
 */
public record RunSettings(
        @Min(1) @Max(1_000_000) int requestsPerMinute,
        @Min(1) @Max(60) int durationMinutes,
        @Min(1) @Max(500) Integer maxInFlight,
        SubmissionMode submissionMode,
        Boolean retryEnabled,
        @Min(0) @Max(300) Integer warmupSeconds,
        HttpClientKind httpClient,
        Boolean endless,
        Boolean retryPeriod,
        @Min(1) @Max(3600) Integer retryPeriodSeconds,
        @Min(0) @Max(50) Integer duplicatePercent,
        Boolean circuitBreaker) {

    public RunSettings {
        if (submissionMode == null) submissionMode = SubmissionMode.KAFKA;
        if (retryEnabled == null) retryEnabled = false; // older clients omit it
        if (warmupSeconds == null) warmupSeconds = 0;
        if (httpClient == null) httpClient = HttpClientKind.JETTY;
        if (endless == null) endless = false;
        if (retryPeriod == null) retryPeriod = false;
        if (retryPeriodSeconds == null) retryPeriodSeconds = 120;
        if (duplicatePercent == null) duplicatePercent = 0;
        if (circuitBreaker == null) circuitBreaker = true;
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight, SubmissionMode submissionMode,
            Boolean retryEnabled, Integer warmupSeconds, HttpClientKind httpClient, Boolean endless, Boolean retryPeriod,
            Integer retryPeriodSeconds, Integer duplicatePercent) {
        this(requestsPerMinute, durationMinutes, maxInFlight, submissionMode, retryEnabled, warmupSeconds, httpClient,
                endless, retryPeriod, retryPeriodSeconds, duplicatePercent, null);
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight, SubmissionMode submissionMode,
            Boolean retryEnabled, Integer warmupSeconds, HttpClientKind httpClient, Boolean endless, Boolean retryPeriod,
            Integer retryPeriodSeconds) {
        this(requestsPerMinute, durationMinutes, maxInFlight, submissionMode, retryEnabled, warmupSeconds, httpClient,
                endless, retryPeriod, retryPeriodSeconds, 0);
    }

    public RunSettings(int requestsPerMinute, int durationMinutes, Integer maxInFlight, SubmissionMode mode,
            Boolean retryEnabled, Integer warmupSeconds, HttpClientKind httpClient) {
        this(requestsPerMinute, durationMinutes, maxInFlight, mode, retryEnabled, warmupSeconds, httpClient, false, false, 120);
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
