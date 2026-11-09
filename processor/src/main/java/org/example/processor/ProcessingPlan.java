package org.example.processor;

import java.util.SplittableRandom;
import java.util.UUID;

/**
 * What will happen to one case, decided up front from its clientRequestId so every run is reproducible.
 *
 * @param durationMs     simulated work per attempt (waiting, like a call to an external service)
 * @param attempts       attempts made in total; when ROUTED, all but the last failed with a transient error
 * @param outcome        ROUTED, or FAILED (permanent error, or every attempt failed)
 * @param failureReason  why it FAILED, otherwise null
 */
public record ProcessingPlan(long durationMs, int attempts, String outcome, String category, String team,
        String failureReason) {

    private static final String[][] ROUTES = {
            { "BILLING", "Billing Support" }, { "TECHNICAL", "Technical Support" }, { "ACCOUNT", "Account Services" } };

    public static ProcessingPlan forCase(UUID clientRequestId, ProcessingProfile profile) {
        var random = new SplittableRandom(clientRequestId.getMostSignificantBits() ^ clientRequestId.getLeastSignificantBits());
        boolean slow = random.nextDouble() < profile.slowFraction();
        // Uniform between 0.5x and 1.5x the mean, or the slow duration for the slow fraction.
        long duration = slow ? profile.slowMs() : Math.round(profile.meanMs() * (0.5 + random.nextDouble()));
        int failed = 0;
        while (failed < profile.maxAttempts() && random.nextDouble() < profile.transientFailureRate()) failed++;
        boolean permanent = random.nextDouble() < profile.permanentFailureRate();
        String[] route = ROUTES[random.nextInt(ROUTES.length)];
        if (permanent)
            return new ProcessingPlan(duration, 1, "FAILED", route[0], null, "Permanent error: case cannot be routed");
        if (failed >= profile.maxAttempts())
            return new ProcessingPlan(duration, failed, "FAILED", route[0], null,
                    "Gave up after " + failed + " failed attempts");
        return new ProcessingPlan(duration, failed + 1, "ROUTED", route[0], route[1], null);
    }

    public boolean routed() { return "ROUTED".equals(outcome); }
}
