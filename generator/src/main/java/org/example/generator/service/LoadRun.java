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
import java.util.List;
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
    /** Pauses sending while the backend is unreachable; null when turned off. */
    private final CircuitBreaker circuit;
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
    private long duplicatesSent, duplicatesAccepted, duplicatesOther;
    private int inFlight;
    // Connections the HTTP client had opened and closed when the measured run started (after the warm-up).
    private long openedBefore = -1, closedBefore = -1;
    private String error;
    private volatile boolean stopping;
    // Retry periods after the run (guarded by this).
    private boolean retrying;
    private volatile boolean retryStopping;
    private String stateBeforeRetry;
    private int retryPeriods;
    private RetryPeriod retryProgress;

    public LoadRun(UUID id, RunSettings settings, CaseClient client, RetryPolicy retry, RunReport report) {
        this.id = id;
        this.settings = settings;
        this.client = client;
        this.retry = retry;
        this.report = report;
        this.limit = settings.endless() ? ENDLESS_LIMIT : settings.planned();
        this.circuit = settings.circuitBreaker() ? new CircuitBreaker() : null;
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
                String held = circuit == null ? null : circuit.admit(System.nanoTime());
                boolean capacity;
                synchronized (this) {
                    handled = seq;
                    capacity = held == null && (settings.maxInFlight() == null || inFlight < settings.maxInFlight());
                    if (capacity) { inFlight++; dispatched++; }
                }
                if (!capacity) {
                    if (held == null && circuit != null) circuit.probeNotSent(System.nanoTime());
                    record(seq, new SubmissionResult(submission.clientRequestId(), "SKIPPED", null, null,
                            0, delay, held != null ? held : "Maximum in-flight requests reached", Instant.now(), 0));
                    continue;
                }
                workers.submit(() -> {
                    try {
                        SubmissionResult result = send(submission, delay);
                        if (circuit != null) circuit.onResult(CaseClient.unreachable(result), System.nanoTime());
                        // The HTTP client's own queue was full: never sent after all.
                        if ("SKIPPED".equals(result.outcome())) synchronized (this) { dispatched--; }
                        record(seq, result);
                    }
                    finally { synchronized (this) { inFlight--; } }
                });
                if (Submission.isDuplicated(seq, settings.duplicatePercent())
                        && (circuit == null || circuit.state() == CircuitBreaker.State.CLOSED)) {
                    // The same submission again, 0 to 2 s later: sometimes while the first is still in flight,
                    // sometimes after it was accepted. Extra traffic, not a new submission.
                    long wait = java.util.concurrent.ThreadLocalRandom.current().nextLong(2_000);
                    workers.submit(() -> sendDuplicate(submission, wait));
                }
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
                // Automatic retry period: only after a run that finished by itself (not stopped, no error). Switched
                // to RETRYING in the same step as finishing, so no observer sees the run as done in between.
                if (settings.retryPeriod() && "COMPLETED".equals(state) && retryableCount() > 0) {
                    retrying = true; retryStopping = false; stateBeforeRetry = state; state = "RETRYING";
                }
            }
            // Outside the lock: waits for the report writer to drain and close the file.
            try { report.finish(finalSnapshot); }
            catch (IOException ex) { synchronized (this) { failReport(ex); } }
        }
        if (retrying()) retryPeriod(settings.retryPeriodSeconds());
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

    /** Sends a submission a second time after {@code waitMs}; counted only in the duplicate counters. */
    private void sendDuplicate(Submission submission, long waitMs) {
        try {
            Thread.sleep(waitMs);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return;
        }
        synchronized (this) {
            if (settings.maxInFlight() != null && inFlight >= settings.maxInFlight()) { duplicatesOther++; return; }
            inFlight++;
            duplicatesSent++;
        }
        try {
            SubmissionResult result = client.send(submission, 0, settings.submissionMode(), settings.httpClient());
            synchronized (this) {
                if ("ACCEPTED".equals(result.outcome())) duplicatesAccepted++; else duplicatesOther++;
                if ("SKIPPED".equals(result.outcome())) duplicatesSent--; // never left the generator
            }
        } finally {
            synchronized (this) { inFlight--; }
        }
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
        else if (retrying) retryStopping = true;
    }

    /** Outcomes the retry period sends again: no definite answer yet, and safe to resend with the same id. */
    static boolean retryable(byte outcome) {
        return outcome == SKIPPED || outcome == BLOCKED || outcome == UNKNOWN;
    }

    /** One pass of a retry period: what it sent and how those submissions ended. */
    public record RetryPass(int pass, long planned, long sent, long accepted, long skipped, long blocked, long unknown,
            long failed) {
        RetryPass sent1() { return new RetryPass(pass, planned, sent + 1, accepted, skipped, blocked, unknown, failed); }
        RetryPass add(byte outcome) {
            return new RetryPass(pass, planned, sent,
                    accepted + (outcome == ACCEPTED ? 1 : 0), skipped + (outcome == SKIPPED ? 1 : 0),
                    blocked + (outcome == BLOCKED ? 1 : 0), unknown + (outcome == UNKNOWN ? 1 : 0),
                    failed + (outcome == FAILED ? 1 : 0));
        }
    }

    /**
     * A retry period after the run: passes over every submission still Skipped, Blocked or Unknown, at the run's rate,
     * with the original clientRequestIds (resending is safe: the backend ignores an id it already has). Ends when
     * nothing is left to retry, the time limit is reached, {@link #MAX_RETRY_PASSES} passes are done, a pass resolves
     * nothing, or it is stopped. {@code state} is RETRYING while it runs, then DONE or STOPPED; {@code reason} says
     * why it ended.
     */
    public record RetryPeriod(String state, String reason, Instant startedAt, Instant endsBy, Instant finishedAt,
            int maxPasses, long recovered, List<RetryPass> passes) {
        RetryPeriod withPasses(List<RetryPass> next, long recoveredNow) {
            return new RetryPeriod(state, reason, startedAt, endsBy, finishedAt, maxPasses, recoveredNow, next);
        }
        RetryPeriod end(String endState, String why) {
            return new RetryPeriod(endState, why, startedAt, endsBy, Instant.now(), maxPasses, recovered, passes);
        }
    }

    public static final int MAX_RETRY_PASSES = 10;

    /** Submissions that would be retried now. */
    public synchronized long retryableCount() {
        return problems.values().stream().filter(LoadRun::retryable).count();
    }

    /**
     * Starts a retry period by hand, after the run, for {@code seconds} (null: the run's setting). False if the run is
     * still going, a retry period is already running, or nothing needs a retry.
     */
    public boolean startRetryPeriod(Integer seconds) {
        synchronized (this) {
            if (finishedAt == null || retrying || retryableCount() == 0) return false;
            retrying = true;
            retryStopping = false;
            stateBeforeRetry = state;
            state = "RETRYING";
        }
        int limitSeconds = seconds != null ? seconds : settings.retryPeriodSeconds();
        Thread.ofVirtual().name("retry-" + id).start(() -> retryPeriod(limitSeconds));
        return true;
    }

    /** Runs a retry period on the calling thread; {@code retrying} and the RETRYING state are already set. */
    private void retryPeriod(int limitSeconds) {
        Instant began = Instant.now();
        long endsBy = System.nanoTime() + limitSeconds * 1_000_000_000L;
        synchronized (this) {
            retryProgress = new RetryPeriod("RETRYING", null, began, began.plusSeconds(limitSeconds), null,
                    MAX_RETRY_PASSES, 0, List.of());
        }
        String endState = "DONE", reason = null;
        try {
            for (int pass = 1; ; pass++) {
                long[] sequences;
                synchronized (this) {
                    sequences = problems.entrySet().stream().filter(e -> retryable(e.getValue()))
                            .mapToLong(Map.Entry::getKey).sorted().toArray();
                }
                if (sequences.length == 0) { reason = "Nothing left to retry"; break; }
                if (retryStopping) { endState = "STOPPED"; reason = "Stopped"; break; }
                if (System.nanoTime() >= endsBy) { reason = "Time limit reached"; break; }
                if (pass > MAX_RETRY_PASSES) { reason = "Maximum of " + MAX_RETRY_PASSES + " passes reached"; break; }
                int index = pass - 1;
                synchronized (this) {
                    var next = new ArrayList<>(retryProgress.passes());
                    next.add(new RetryPass(pass, sequences.length, 0, 0, 0, 0, 0, 0));
                    retryProgress = retryProgress.withPasses(List.copyOf(next), retryProgress.recovered());
                }
                retryPass(index, sequences, endsBy);
                RetryPass done;
                synchronized (this) { done = retryProgress.passes().get(index); }
                if (retryStopping) { endState = "STOPPED"; reason = "Stopped"; break; }
                if (done.accepted() + done.failed() == 0 && done.sent() == done.planned()) {
                    reason = "A pass resolved nothing; the backend may be unavailable";
                    break;
                }
                // Wait between passes: 1 s, 2 s, 4 s ... up to 30 s, never past the time limit. Blocked submissions
                // were told to retry after 1 s, so even the first wait honours that.
                long wait = Math.min(30_000L, 1_000L << Math.min(index, 5)) * 1_000_000L;
                long until = Math.min(System.nanoTime() + wait, endsBy);
                while (!retryStopping && System.nanoTime() < until) LockSupport.parkNanos(Math.min(until - System.nanoTime(), 50_000_000));
            }
        } finally {
            RetryPeriod ended;
            synchronized (this) {
                retryProgress = retryProgress.end(endState, reason);
                state = stateBeforeRetry;
                retrying = false;
                ended = retryProgress;
            }
            report.writeSidecar(".retry-" + (++retryPeriods) + ".json", ended);
        }
    }

    /** Sends one pass at the run's rate, waiting for in-flight slots, and waits until every reply is recorded. */
    private void retryPass(int index, long[] sequences, long endsBy) {
        long interval = 60_000_000_000L / settings.requestsPerMinute();
        long origin = System.nanoTime();
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int k = 0; k < sequences.length && !retryStopping && System.nanoTime() < endsBy; k++) {
                long due = origin + k * interval;
                while (!retryStopping && System.nanoTime() < due) {
                    LockSupport.parkNanos(Math.min(due - System.nanoTime(), 50_000_000));
                }
                while (!retryStopping && settings.maxInFlight() != null && inFlightNow() >= settings.maxInFlight()) {
                    LockSupport.parkNanos(1_000_000); // wait for a slot rather than skip
                }
                if (retryStopping) break;
                long sequence = sequences[k];
                Submission submission = Submission.forRun(id, sequence);
                synchronized (this) {
                    inFlight++;
                    updatePass(index, retryProgress.passes().get(index).sent1());
                }
                double delay = Math.max(0, (System.nanoTime() - due) / 1_000_000.0);
                workers.submit(() -> {
                    try { recordRetry(index, sequence, client.send(submission, delay, settings.submissionMode(), settings.httpClient())); }
                    finally { synchronized (this) { inFlight--; } }
                });
            }
        }
    }

    private void updatePass(int index, RetryPass pass) {
        var next = new ArrayList<>(retryProgress.passes());
        next.set(index, pass);
        retryProgress = retryProgress.withPasses(List.copyOf(next), retryProgress.recovered());
    }

    /** A retried submission's new outcome replaces its old one in the run's counts. */
    private void recordRetry(int index, long sequence, SubmissionResult result) {
        synchronized (this) {
            byte before = problems.getOrDefault(sequence, ACCEPTED);
            switch (before) {
                case SKIPPED -> skipped--;
                case BLOCKED -> blocked--;
                case UNKNOWN -> unknown--;
                default -> { }
            }
            byte after = code(result.outcome());
            switch (after) {
                case ACCEPTED -> accepted++;
                case UNKNOWN -> unknown++;
                case SKIPPED -> skipped++;
                case BLOCKED -> blocked++;
                default -> failed++;
            }
            setOutcome(sequence, after);
            updatePass(index, retryProgress.passes().get(index).add(after));
            if (after == ACCEPTED) retryProgress = retryProgress.withPasses(retryProgress.passes(), retryProgress.recovered() + 1);
            recent.addFirst(result);
            if (recent.size() > 100) recent.removeLast();
        }
    }

    private synchronized int inFlightNow() { return inFlight; }

    /** True while a retry period runs (state RETRYING). */
    public synchronized boolean retrying() { return retrying; }

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
                settings.warmupPlanned(), warmupSent, warmupFailed, inFlight, opened, closed, open, error,
                new RunSnapshot.Duplicates(duplicatesSent, duplicatesAccepted, duplicatesOther),
                circuit == null ? null : circuit.status(), retryProgress,
                new ArrayList<>(recent));
    }
}
