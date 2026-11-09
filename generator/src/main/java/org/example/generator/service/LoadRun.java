package org.example.generator.service;

import org.example.generator.client.CaseClient;
import org.example.generator.client.RetryPolicy;
import org.example.generator.model.*;
import org.example.generator.report.RunReport;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

public class LoadRun implements Runnable {
    /** Final outcome per submission, kept for the checker. Indexed like {@link #submissions()}. */
    public static final byte NOT_SENT = 0, ACCEPTED = 1, FAILED = 2, UNKNOWN = 3, SKIPPED = 4;

    private final UUID id;
    private final RunSettings settings;
    private final List<Submission> submissions;
    private final CaseClient client;
    private final RetryPolicy retry;
    private final RunReport report;
    private final ArrayDeque<SubmissionResult> recent = new ArrayDeque<>();
    private final byte[] outcomes; // guarded by this
    private String state;
    private Instant startedAt;
    private Instant finishedAt;
    private long dispatched, accepted, failed, unknown, skipped, notSent;
    private long retried, recovered, retryRequests;
    private long warmupSent, warmupFailed;
    private int inFlight;
    private String error;
    private volatile boolean stopping;

    public LoadRun(UUID id, RunSettings settings, List<Submission> submissions, CaseClient client,
            RetryPolicy retry, RunReport report) {
        this.id = id;
        this.settings = settings;
        this.submissions = submissions;
        this.client = client;
        this.retry = retry;
        this.report = report;
        this.outcomes = new byte[submissions.size()];
        this.state = settings.warmupSeconds() > 0 ? "WARMING_UP" : "RUNNING";
    }

    @Override
    public void run() {
        synchronized (this) { startedAt = Instant.now(); }
        int i = 0;
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            if (settings.warmupSeconds() > 0) {
                warmUp(workers);
                synchronized (this) { if (state.equals("WARMING_UP")) state = "RUNNING"; }
            }
            // The measured run starts at full rate right after the warm-up, on the connections it opened.
            long origin = System.nanoTime();
            long interval = 60_000_000_000L / settings.requestsPerMinute();
            for (; i < submissions.size(); i++) {
                long due = origin + i * interval;
                while (!stopping && System.nanoTime() < due) {
                    LockSupport.parkNanos(Math.min(due - System.nanoTime(), 50_000_000));
                    if (Thread.currentThread().isInterrupted()) stopping = true;
                }
                if (stopping) break;
                Submission submission = submissions.get(i);
                int index = i;
                double delay = Math.max(0, (System.nanoTime() - due) / 1_000_000.0);
                boolean capacity;
                synchronized (this) {
                    capacity = settings.maxInFlight() == null || inFlight < settings.maxInFlight();
                    if (capacity) { inFlight++; dispatched++; }
                }
                if (!capacity) {
                    record(index, new SubmissionResult(submission.clientRequestId(), "SKIPPED", null, null,
                            0, delay, "Maximum in-flight requests reached", Instant.now(), 0));
                    continue;
                }
                workers.submit(() -> {
                    try { record(index, send(submission, delay)); }
                    finally { synchronized (this) { inFlight--; } }
                });
            }
            synchronized (this) {
                notSent = settings.planned() - dispatched - skipped;
                if (!state.equals("ERROR")) state = "DRAINING";
            }
            report.recordNotSent(submissions.subList(i, submissions.size()));
        } finally {
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
            Submission submission = Submission.forRun(id, -k);
            workers.submit(() -> {
                boolean ok = client.warmUp(submission);
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
        SubmissionResult result = client.send(submission, delay, settings.submissionMode());
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
            result = client.send(submission, delay, settings.submissionMode());
        }
        return result.withAttempts(attempts);
    }

    private void record(int index, SubmissionResult result) {
        synchronized (this) {
            switch (result.outcome()) {
                case "ACCEPTED" -> { accepted++; outcomes[index] = ACCEPTED; }
                case "UNKNOWN" -> { unknown++; outcomes[index] = UNKNOWN; }
                case "SKIPPED" -> { skipped++; outcomes[index] = SKIPPED; }
                default -> { failed++; outcomes[index] = FAILED; }
            }
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
    }

    public synchronized boolean active() { return finishedAt == null; }
    public UUID id() { return id; }
    public RunReport report() { return report; }
    public List<Submission> submissions() { return submissions; }

    /** A copy of each submission's final outcome ({@link #ACCEPTED}, {@link #UNKNOWN}, ...). */
    public synchronized byte[] outcomes() { return outcomes.clone(); }

    public synchronized RunSnapshot snapshot() {
        return new RunSnapshot(id, settings, state, startedAt, finishedAt, settings.planned(),
                dispatched, accepted, failed, unknown, skipped, notSent, retried, recovered, retryRequests,
                settings.warmupPlanned(), warmupSent, warmupFailed, inFlight, error, new ArrayList<>(recent));
    }
}
