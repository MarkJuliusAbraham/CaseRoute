package org.example.generator.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * When and how long to wait before resending a submission whose outcome is unknown.
 *
 * <p>Only unknown outcomes are retried: a timeout, a dropped connection or a 5xx reply. The backend may or may
 * not have the submission, and resending is safe because both endpoints are idempotent by clientRequestId.
 * A 4xx or a mismatched reply is a definite answer, so it is never retried.
 *
 * <p>Waits grow exponentially (base, 2x base, 4x base, ... up to the cap) so an overloaded backend gets room
 * to recover, and each wait is randomised between half and all of that value ("jitter") so thousands of
 * clients that failed together do not all retry in the same instant.
 */
@Component
public class RetryPolicy {
    private final int maxAttempts;
    private final long baseMs;
    private final long maxMs;

    public RetryPolicy(@Value("${generator.retry.max-attempts:5}") int maxAttempts,
            @Value("${generator.retry.base-delay:500ms}") Duration base,
            @Value("${generator.retry.max-delay:8s}") Duration max) {
        this.maxAttempts = maxAttempts;
        this.baseMs = base.toMillis();
        this.maxMs = max.toMillis();
    }

    /** Whether a submission that has been sent {@code attempts} times with an unknown outcome is resent. */
    public boolean allowsAnother(int attempts) { return attempts < maxAttempts; }

    /** Milliseconds to wait before attempt number {@code attempt} (2 is the first retry). */
    public long delayBefore(int attempt) {
        long ceiling = Math.min(maxMs, baseMs << Math.min(attempt - 2, 20));
        return ceiling / 2 + ThreadLocalRandom.current().nextLong(ceiling / 2 + 1);
    }

    public int maxAttempts() { return maxAttempts; }
}
