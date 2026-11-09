package org.example.generator.service;

import org.example.generator.client.CaseClient;
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
    private final UUID id;
    private final RunSettings settings;
    private final List<Submission> submissions;
    private final CaseClient client;
    private final RunReport report;
    private final ArrayDeque<SubmissionResult> recent = new ArrayDeque<>();
    private String state = "RUNNING";
    private Instant startedAt;
    private Instant finishedAt;
    private long dispatched, accepted, failed, unknown, skipped, notSent;
    private int inFlight;
    private String error;
    private volatile boolean stopping;

    public LoadRun(UUID id, RunSettings settings, List<Submission> submissions, CaseClient client, RunReport report) {
        this.id = id;
        this.settings = settings;
        this.submissions = submissions;
        this.client = client;
        this.report = report;
    }

    @Override
    public void run() {
        synchronized (this) { startedAt = Instant.now(); }
        long origin = System.nanoTime();
        long interval = 60_000_000_000L / settings.requestsPerMinute();
        int i = 0;
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            for (; i < submissions.size(); i++) {
                long due = origin + i * interval;
                while (!stopping && System.nanoTime() < due) {
                    LockSupport.parkNanos(Math.min(due - System.nanoTime(), 50_000_000));
                    if (Thread.currentThread().isInterrupted()) stopping = true;
                }
                if (stopping) break;
                Submission submission = submissions.get(i);
                double delay = Math.max(0, (System.nanoTime() - due) / 1_000_000.0);
                boolean capacity;
                synchronized (this) {
                    capacity = settings.maxInFlight() == null || inFlight < settings.maxInFlight();
                    if (capacity) { inFlight++; dispatched++; }
                }
                if (!capacity) {
                    record(new SubmissionResult(submission.clientRequestId(), "SKIPPED", null, null,
                            0, delay, "Maximum in-flight requests reached", Instant.now()));
                    continue;
                }
                workers.submit(() -> {
                    try { record(client.send(submission, delay, settings.submissionMode())); }
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

    private void record(SubmissionResult result) {
        synchronized (this) {
            switch (result.outcome()) {
                case "ACCEPTED" -> accepted++;
                case "UNKNOWN" -> unknown++;
                case "SKIPPED" -> skipped++;
                default -> failed++;
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

    public synchronized RunSnapshot snapshot() {
        return new RunSnapshot(id, settings, state, startedAt, finishedAt, settings.planned(),
                dispatched, accepted, failed, unknown, skipped, notSent, inFlight, error,
                new ArrayList<>(recent));
    }
}
