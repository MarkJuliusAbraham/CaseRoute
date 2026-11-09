package org.example.processor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** How long processing takes and how often it fails. See application.properties. */
@Component
public record ProcessingProfile(
        @Value("${caseroute.processing.mean-ms}") long meanMs,
        @Value("${caseroute.processing.slow-fraction}") double slowFraction,
        @Value("${caseroute.processing.slow-ms}") long slowMs,
        @Value("${caseroute.processing.transient-failure-rate}") double transientFailureRate,
        @Value("${caseroute.processing.max-attempts}") int maxAttempts,
        @Value("${caseroute.processing.retry-backoff-ms}") long retryBackoffMs,
        @Value("${caseroute.processing.permanent-failure-rate}") double permanentFailureRate) {
}
