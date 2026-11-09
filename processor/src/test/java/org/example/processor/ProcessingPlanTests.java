package org.example.processor;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ProcessingPlanTests {
    private final ProcessingProfile profile = new ProcessingProfile(1000, 0.01, 10_000, 0.02, 3, 500, 0.001);

    @Test void sameCaseAlwaysGetsTheSamePlan() {
        UUID id = UUID.randomUUID();
        assertEquals(ProcessingPlan.forCase(id, profile), ProcessingPlan.forCase(id, profile));
    }

    @Test void ratesMatchTheProfileOverManyCases() {
        int n = 200_000, slow = 0, retried = 0, failed = 0;
        for (int i = 0; i < n; i++) {
            ProcessingPlan plan = ProcessingPlan.forCase(UUID.randomUUID(), profile);
            if (plan.durationMs() == 10_000) slow++;
            else assertTrue(plan.durationMs() >= 500 && plan.durationMs() <= 1500);
            if (plan.routed() && plan.attempts() > 1) retried++;
            if (!plan.routed()) failed++;
            assertEquals(plan.routed(), plan.team() != null);
        }
        assertEquals(0.01, slow / (double) n, 0.002);
        assertEquals(0.02, retried / (double) n, 0.003);  // about one case in 50 needs a retry
        assertEquals(0.001, failed / (double) n, 0.0005); // mostly permanent errors; exhausted retries are very rare
    }

    @Test void exhaustedRetriesFailAfterMaxAttempts() {
        var alwaysTransient = new ProcessingProfile(10, 0, 0, 1.0, 3, 0, 0);
        ProcessingPlan plan = ProcessingPlan.forCase(UUID.randomUUID(), alwaysTransient);
        assertEquals("FAILED", plan.outcome());
        assertEquals(3, plan.attempts());
        assertNull(plan.team());
    }

    @Test void permanentErrorsFailOnTheFirstAttempt() {
        var alwaysPermanent = new ProcessingProfile(10, 0, 0, 0, 3, 0, 1.0);
        ProcessingPlan plan = ProcessingPlan.forCase(UUID.randomUUID(), alwaysPermanent);
        assertEquals("FAILED", plan.outcome());
        assertEquals(1, plan.attempts());
    }
}
