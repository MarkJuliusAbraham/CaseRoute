package org.example.generator.service;

import org.example.generator.client.CaseClient;
import org.example.generator.client.RetryPolicy;
import org.example.generator.model.*;
import org.example.generator.report.RunReport;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

/**
 * One load run: an optional warm-up, then submissions at a fixed rate, for a set number of minutes or, when
 * {@link RunSettings#endless()} is set, until stopped.
 *
 * <p>Memory stays flat however long the run lasts. Submissions are rebuilt from their sequence number when needed
 * ({@link Submission#forRun}) instead of being kept in a list, and only the outcomes that are not ACCEPTED are
 * stored, by sequence number. In a healthy run almost everything is accepted, so that map stays small; it grows
 * only with problems, which the report records one line each anyway.
 */
public class LoadRun implements Runnable {
    /** Final outcome codes, as returned by {@link Outcomes#of}. */
    public static final byte NOT_SENT = 0, ACCEPTED = 1, FAILED = 2, UNKNOWN = 3, SKIPPED = 4, BLOCKED = 5;
    /** Endless runs stop after this many submissions (about 35 hours at 1,000,000 per minute). */
    public static final long ENDLESS_LIMIT = Integer.MAX_VALUE - 1;

    private final UUID id;
    private final RunSettings settings;
    private final CaseClient client;
    private final RetryPolicy retry;
    private final RunReport report;
    private final long limit;
    private final ArrayDeque<SubmissionResult> recent = new ArrayDeque<>();
    /** Outcomes other than ACCEPTED, by sequence number (guarded by this). */
    private final Map<Long, Byte> problems = new HashMap<>();
    /** Sequence numbers 1..handled have been dispatched or skipped (guarded by this). */
    private long handled;
    private String state;
    private Instant startedAt;
    private Instant finishedAt;
    private long dispatched, accepted, failed, unknown, skipped, notSent;
    private long retried, recovered, retryRequests;
    private long warmupSent, warmupFailed;
    private long blocked;
    private int inFlight;
    // Connections the HTTP client had opened and closed when the measured run started (after the warm-up).
    private long openedBefore = -1, closedBefore = -1;
    private String error;
    private volatile boolean stopping;
    // Resend passes over blocked submissions, after the run (guarded by this).
    private boolean resending;
    private volatile boolean resendStopping;
    private String stateBeforeResend;
    private int resendPasses;
    private ResendProgress resend;

    public LoadRun(UUID id, RunSettings settings, CaseClient client, RetryPolicy retry, RunReport report) {
        this.id = id;
        this.settings = settings;
        this.client = client;
        this.retry = retry;
        this.report = report;
        this.limit = settings.endless() ? ENDLESS_LIMIT : settings.planned();
        this.state = settings.warmupSeconds() > 0 ? "WARMING_UP" : "RUNNING";
    }

    @Override
    public void run() {
        synchronized (this) { startedAt = Instant.now(); }
        long sequence = 1;
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            if (settings.warmupSeconds() > 0) {
                warmUp(workers);
                synchronized (this) { if (state.equals("WARMING_UP")) state = "RUNNING"; }
            }
            synchronized (this) {
                openedBefore = client.connectionsOpened();
                closedBefore = client.connectionsClosed();
            }
            client.onConnectionEvents(report::connectionEvent);
            // The measured run starts at full rate right after the warm-up, on the connections it opened.
            long origin = System.nanoTime();
            long interval = 60_000_000_000L / settings.requestsPerMinute();
            for (; sequence <= limit; sequence++) {
                long due = origin + (sequence - 1) * interval;
                while (!stopping && System.nanoTime() < due) {
                    LockSupport.parkNanos(Math.min(due - System.nanoTime(), 50_000_000));
                    if (Thread.currentThread().isInterrupted()) stopping = true;
                }
                if (stopping) break;
                Submission submission = Submission.forRun(id, sequence);
                long seq = sequence;
                double delay = Math.max(0, (System.nanoTime() - due) / 1_000_000.0);
                boolean capacity;
                synchronized (this) {
                    handled = seq;
                    capacity = settings.maxInFlight() == null || inFlight < settings.maxInFlight();
                    if (capacity) { inFlight++; dispatched++; }
                }
                if (!capacity) {
                    record(seq, new SubmissionResult(submission.clientRequestId(), "SKIPPED", null, null,
                            0, delay, "Maximum in-flight requests reached", Instant.now(), 0));
                    continue;
                }
                workers.submit(() -> {
                    try {
                        SubmissionResult result = send(submission, delay);
                        // The HTTP client's own queue was full: never sent after all.
                        if ("SKIPPED".equals(result.outcome())) synchronized (this) { dispatched--; }
                        record(seq, result);
                    }
                    finally { synchronized (this) { inFlight--; } }
                });
            }
            synchronized (this) {
                notSent = settings.endless() ? 0 : settings.planned() - handled;
                if (!state.equals("ERROR")) state = "DRAINING";
            }
            if (!settings.endless() && handled < settings.planned()) report.recordNotSent(handled + 1, settings.planned());
        } finally {
            client.onConnectionEvents(null);
            RunSnapshot finalSnapshot;
            synchronized (this) {
                finishedAt = Instant.now();
                if (!state.equals("ERROR")) state = stopping ? "STOPPED" : "COMPLETED";
                finalSnapshot = snapshot();
            }
            // Outside the lock: waits for the report writer to drain and close the file.
            try { report.finish(finalSnapshot); }
            catch (IOException ex) { synchronized (this) { failReport(ex); } }
        }
    }

    /**
     * Ramps from zero to the full rate over {@code warmupSeconds}, sending synthetic submissions to the
     * backend's warm-up endpoint, which stores nothing. The rate grows linearly, so request k is due at
     * sqrt(2 * T * k / R) for ramp length T and full rate R. Warm-up requests share the in-flight cap and
     * HTTP client with the run, but not its counters or report.
     */
    private void warmUp(java.util.concurrent.ExecutorService workers) {
        long ramp = settings.warmupSeconds() * 1_000_000_000L;
        double perNano = settings.requestsPerMinute() / 60_000_000_000.0;
        long total = settings.warmupPlanned();
        if (settings.httpClient() == HttpClientKind.JETTY) {
            // Open spare connections gradually over the second half of the warm-up, so the run never needs a burst
            // of new connections at the switch. Spares the run does not use are closed after the idle timeout.
            Duration half = Duration.ofSeconds(settings.warmupSeconds()).dividedBy(2);
            Thread.ofVirtual().name("preopen-" + id).start(() -> {
                try { Thread.sleep(half); } catch (InterruptedException ex) { return; }
                if (!stopping) client.preOpenJettyConnections(half);
            });
        }
        long start = System.nanoTime();
        for (long k = 1; k <= total; k++) {
            long due = start + (long) Math.sqrt(2.0 * ramp * k / perNano);
            while (!stopping && System.nanoTime() < due) {
                LockSupport.parkNanos(Math.min(due - System.nanoTime(), 50_000_000));
                if (Thread.currentThread().isInterrupted()) stopping = true;
            }
            if (stopping) return;
            synchronized (this) {
                if (settings.maxInFlight() != null && inFlight >= settings.maxInFlight()) continue;
                inFlight++;
                warmupSent++;
            }
            // Negative sequence numbers: never mistaken for run submissions.
            Submission submission = Submission.forRun(id, -k);
            workers.submit(() -> {
                boolean ok = client.warmUp(submission, settings.submissionMode(), settings.httpClient());
                synchronized (this) { inFlight--; if (!ok) warmupFailed++; }
            });
        }
    }

    /**
     * Sends a submission and, when retries are enabled, resends it while its outcome stays unknown. The same
     * clientRequestId is reused, so the backend saves at most one case however many attempts reach it.
     * A submission occupies its in-flight slot until its final outcome, including the waits between attempts.
     */
    private SubmissionResult send(Submission submission, double delay) {
        SubmissionResult result = client.send(submission, delay, settings.submissionMode(), settings.httpClient());
        int attempts = 1;
        while (settings.retryEnabled() && "UNKNOWN".equals(result.outcome()) && retry.allowsAnother(attempts)) {
            try {
                Thread.sleep(retry.delayBefore(attempts + 1));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt(); // shutting down: keep the unknown outcome
                break;
            }
            attempts++;
            synchronized (this) { retryRequests++; }
            SubmissionResult next = client.send(submission, delay, settings.submissionMode(), settings.httpClient());
            // A retry the client's queue had no room for was not sent, but an earlier attempt was: still unknown.
            if ("SKIPPED".equals(next.outcome())) break;
            result = next;
        }
        return result.withAttempts(attempts);
    }

    private static byte code(String outcome) {
        return switch (outcome) {
            case "ACCEPTED" -> ACCEPTED;
            case "UNKNOWN" -> UNKNOWN;
            case "SKIPPED" -> SKIPPED;
            case "BLOCKED" -> BLOCKED;
            default -> FAILED;
        };
    }

    private void setOutcome(long sequence, byte code) {
        if (code == ACCEPTED) problems.remove(sequence);
        else problems.put(sequence, code);
    }

    private void record(long sequence, SubmissionResult result) {
        synchronized (this) {
            byte code = code(result.outcome());
            switch (code) {
                case ACCEPTED -> accepted++;
                case UNKNOWN -> unknown++;
                case SKIPPED -> skipped++;
                case BLOCKED -> blocked++;
                default -> failed++;
            }
            setOutcome(sequence, code);
            if (result.attempts() > 1) {
                retried++;
                if ("ACCEPTED".equals(result.outcome())) recovered++;
            }
            recent.addFirst(result);
            if (recent.size() > 100) recent.removeLast();
        }
        // Outside the lock the dispatcher needs; the report only queues work in memory.
        report.record(result);
        IOException failure = report.failure();
        if (failure != null) synchronized (this) { if (!state.equals("ERROR")) failReport(failure); }
    }

    private void failReport(IOException ex) {
        stopping = true;
        state = "ERROR";
        error = "Report could not be written; run stopped. " + ex.getClass().getSimpleName();
    }

    public synchronized void stop() {
        if (active()) { stopping = true; if (!state.equals("ERROR")) state = "DRAINING"; }
        else if (resending) resendStopping = true;
    }

    /**
     * Progress of the latest resend pass. Counts are of the blocked submissions resent in that pass; the run's own
     * counts are updated as their new outcomes arrive.
     */
    public record ResendProgress(int pass, String state, long planned, long sent, long accepted, long blocked,
            long unknown, long failed, Instant startedAt, Instant finishedAt) {
        ResendProgress with(String newState, long newSent, long a, long b, long u, long f, Instant end) {
            return new ResendProgress(pass, newState, planned, newSent, a, b, u, f, startedAt, end);
        }
    }

    /**
     * Resends every submission still BLOCKED, after the run, at the run's rate, with the same clientRequestIds
     * (they were never stored, so this cannot create duplicates). Not part of the run's measured time: it is a
     * separate pass, started by hand. No automatic retries inside it; press again for any blocked a second time.
     *
     * @return false if the run is still going, a pass is already running, or nothing is blocked
     */
    public synchronized boolean startResend() {
        if (finishedAt == null || resending || blocked == 0) return false;
        long[] toSend = problems.entrySet().stream().filter(e -> e.getValue() == BLOCKED)
                .mapToLong(Map.Entry::getKey).sorted().toArray();
        resending = true;
        resendStopping = false;
        stateBeforeResend = state;
        state = "RESENDING";
        resend = new ResendProgress(++resendPasses, "RESENDING", toSend.length, 0, 0, 0, 0, 0, Instant.now(), null);
        Thread.ofVirtual().name("resend-" + id).start(() -> resendAll(toSend));
        return true;
    }

    private void resendAll(long[] sequences) {
        long interval = 60_000_000_000L / settings.requestsPerMinute();
        long origin = System.nanoTime();
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int k = 0; k < sequences.length && !resendStopping; k++) {
                long due = origin + k * interval;
                while (!resendStopping && System.nanoTime() < due) {
                    LockSupport.parkNanos(Math.min(due - System.nanoTime(), 50_000_000));
                }
                while (!resendStopping && settings.maxInFlight() != null && inFlightNow() >= settings.maxInFlight()) {
                    LockSupport.parkNanos(1_000_000); // wait for a slot rather than skip
                }
                if (resendStopping) break;
                long sequence = sequences[k];
                Submission submission = Submission.forRun(id, sequence);
                synchronized (this) { inFlight++; resend = resend.with(resend.state(), resend.sent() + 1,
                        resend.accepted(), resend.blocked(), resend.unknown(), resend.failed(), null); }
                double delay = Math.max(0, (System.nanoTime() - due) / 1_000_000.0);
                workers.submit(() -> {
                    try { recordResend(sequence, client.send(submission, delay, settings.submissionMode(), settings.httpClient())); }
                    finally { synchronized (this) { inFlight--; } }
                });
            }
        } finally {
            ResendProgress done;
            synchronized (this) {
                resend = resend.with(resendStopping ? "STOPPED" : "COMPLETED", resend.sent(), resend.accepted(),
                        resend.blocked(), resend.unknown(), resend.failed(), Instant.now());
                state = stateBeforeResend;
                resending = false;
                done = resend;
            }
            report.writeSidecar(".resend-" + done.pass() + ".json", done);
        }
    }

    /** A resent submission's new outcome replaces its BLOCKED outcome in the run's counts. */
    private void recordResend(long sequence, SubmissionResult result) {
        synchronized (this) {
            blocked--;
            long a = resend.accepted(), b = resend.blocked(), u = resend.unknown(), f = resend.failed();
            byte code = code(result.outcome());
            if (code == SKIPPED) code = BLOCKED; // the client's queue was full: still not sent, still resendable
            switch (code) {
                case ACCEPTED -> { accepted++; a++; }
                case UNKNOWN -> { unknown++; u++; }
                case BLOCKED -> { blocked++; b++; }
                default -> { failed++; f++; }
            }
            setOutcome(sequence, code);
            resend = resend.with(resend.state(), resend.sent(), a, b, u, f, null);
            recent.addFirst(result);
            if (recent.size() > 100) recent.removeLast();
        }
    }

    private synchronized int inFlightNow() { return inFlight; }

    public synchronized boolean resending() { return resending; }

    public synchronized boolean active() { return finishedAt == null; }
    public UUID id() { return id; }
    public RunReport report() { return report; }

    /**
     * Every submission's final outcome: sequence numbers 1..handled were sent or skipped, the rest of the plan was
     * never sent. Only non-accepted outcomes are listed; any other handled sequence number was accepted.
     */
    public record Outcomes(long handled, Map<Long, Byte> problems) {
        public byte of(long sequence) {
            if (sequence < 1 || sequence > handled) return NOT_SENT;
            return problems.getOrDefault(sequence, ACCEPTED);
        }
    }

    /** A copy of the outcomes, for the checker. */
    public synchronized Outcomes outcomes() { return new Outcomes(handled, Map.copyOf(problems)); }

    public synchronized RunSnapshot snapshot() {
        long opened = openedBefore < 0 ? 0 : client.connectionsOpened() - openedBefore;
        long closed = closedBefore < 0 ? 0 : client.connectionsClosed() - closedBefore;
        long open = client.connectionsOpened() - client.connectionsClosed();
        return new RunSnapshot(id, settings, state, startedAt, finishedAt,
                settings.endless() ? handled : settings.planned(),
                dispatched, accepted, failed, unknown, skipped, notSent, blocked, retried, recovered, retryRequests,
                settings.warmupPlanned(), warmupSent, warmupFailed, inFlight, opened, closed, open, error, resend,
                new ArrayList<>(recent));
    }
}
