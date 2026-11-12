package org.example.generator.service;

import org.example.generator.client.CaseClient;
import org.example.generator.client.RetryPolicy;
import org.example.generator.model.HttpClientKind;
import org.example.generator.model.RunSettings;
import org.example.generator.model.SubmissionMode;
import org.example.generator.report.RunReport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.example.generator.service.CircuitBreaker.*;
import static org.junit.jupiter.api.Assertions.*;

class CircuitBreakerTests {
    private static final long SECOND = 1_000_000_000L;

    @Test
    void opensAfterTwentyUnreachableRepliesInARowAndAnyHttpReplyResetsTheCount() {
        var breaker = new CircuitBreaker(() -> 0.5);
        for (int i = 0; i < FAILURES_TO_OPEN - 1; i++) breaker.onResult(true, 0);
        breaker.onResult(false, 0); // a 503 is still a reply: the backend is there
        for (int i = 0; i < FAILURES_TO_OPEN - 1; i++) breaker.onResult(true, 0);
        assertEquals(State.CLOSED, breaker.state());
        breaker.onResult(true, 0);
        assertEquals(State.OPEN, breaker.state());
        assertNotNull(breaker.admit(SECOND / 2), "paused for the first second");
        assertEquals(1, breaker.status().opened());
    }

    @Test
    void probesOnceAfterThePauseAndBacksOffLongerWhileTheBackendStaysAway() {
        var breaker = new CircuitBreaker(() -> 0.5);
        for (int i = 0; i < FAILURES_TO_OPEN; i++) breaker.onResult(true, 0);
        assertNull(breaker.admit(SECOND), "the first submission after the pause is the probe");
        assertEquals(State.PROBING, breaker.state());
        assertNotNull(breaker.admit(SECOND), "everything else waits for the probe");
        breaker.onResult(true, SECOND); // the probe failed too
        assertEquals(State.OPEN, breaker.state());
        assertNotNull(breaker.admit(SECOND + 2 * SECOND - 1), "paused twice as long");
        assertNull(breaker.admit(SECOND + 2 * SECOND), "then probes again");
    }

    @Test
    void rampsBackUpAfterASuccessfulProbeInsteadOfSendingEverythingAtOnce() {
        double[] draw = { 0.5 };
        var breaker = new CircuitBreaker(() -> draw[0]);
        for (int i = 0; i < FAILURES_TO_OPEN; i++) breaker.onResult(true, 0);
        assertNull(breaker.admit(SECOND));
        breaker.onResult(false, SECOND); // the backend answered
        assertEquals(State.RECOVERING, breaker.state());
        long start = SECOND;
        assertNotNull(breaker.admit(start + RAMP_NANOS / 4), "a quarter of the way: about a quarter is sent");
        draw[0] = 0.2;
        assertNull(breaker.admit(start + RAMP_NANOS / 4));
        assertNull(breaker.admit(start + RAMP_NANOS), "ramp done");
        assertEquals(State.CLOSED, breaker.state());
    }

    @Test
    void aProbeThatCouldNotBeSentIsRetried() {
        var breaker = new CircuitBreaker(() -> 0.5);
        for (int i = 0; i < FAILURES_TO_OPEN; i++) breaker.onResult(true, 0);
        assertNull(breaker.admit(SECOND));
        breaker.probeNotSent(SECOND);
        assertNull(breaker.admit(SECOND), "the next submission becomes the probe");
    }

    /** A run against a port nobody listens on: refusals are "not sent", and the breaker stops the flood. */
    @Test
    void aRunAgainstADownBackendSkipsInsteadOfFloodingAndHasNoUnknownOutcomes(@TempDir Path reports) throws Exception {
        int closedPort;
        try (var socket = new java.net.ServerSocket(0)) { closedPort = socket.getLocalPort(); } // free, then closed
        var json = new JsonMapper();
        var client = new CaseClient(json, URI.create("http://127.0.0.1:" + closedPort + "/api/cases"), 8, 2, 20, 1000, 1, Duration.ZERO);
        var settings = new RunSettings(6000, 1, null, SubmissionMode.KAFKA, false, 0, HttpClientKind.JETTY, true, false, 120, 0, true);
        UUID id = UUID.randomUUID();
        var run = new LoadRun(id, settings, client, new RetryPolicy(1, Duration.ofMillis(10), Duration.ofMillis(10)),
                new RunReport(reports, id, settings, json));
        Thread runner = Thread.ofVirtual().start(run);
        try {
            long deadline = System.nanoTime() + 15 * SECOND;
            while (run.snapshot().circuit().heldBack() < 20 && System.nanoTime() < deadline) Thread.sleep(50);
        } finally {
            run.stop();
            runner.join(20_000);
            client.close();
        }
        var s = run.snapshot();
        assertEquals(0, s.unknown(), "a refused connection is never in doubt");
        assertTrue(s.circuit().opened() >= 1, "the breaker opened: " + s.circuit());
        assertTrue(s.circuit().heldBack() >= 20, "and held submissions back instead of sending them: " + s.circuit());
        assertEquals(s.planned(), s.skipped(), "everything was skipped: refused or held back");
        assertTrue(s.recentResults().stream().anyMatch(r -> r.detail().startsWith(CaseClient.REFUSED)));
    }
}
