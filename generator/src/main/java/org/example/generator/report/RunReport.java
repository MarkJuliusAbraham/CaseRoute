package org.example.generator.report;

import org.example.generator.model.RunSettings;
import org.example.generator.model.RunSnapshot;
import org.example.generator.model.Submission;
import org.example.generator.model.SubmissionResult;
import tools.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE_NEW;

/**
 * Writes {@code <runId>.jsonl}. Record types, one JSON object per line:
 * <ul>
 *   <li>{@code run}: settings, first line.</li>
 *   <li>{@code planned}: every submission, written before dispatch starts.</li>
 *   <li>{@code result}: only submissions that were NOT accepted (failed, unknown, skipped).
 *       A planned submission with no result or notSent line was accepted.</li>
 *   <li>{@code notSent}: submissions never dispatched because the run stopped early.</li>
 *   <li>{@code second}: per-second aggregates by the time responses were recorded.</li>
 *   <li>{@code summary}: final counts, last line.</li>
 * </ul>
 * Callers never touch the disk: lines go to a queue and one background thread writes them through a single
 * open, buffered file, so slow I/O can't delay request dispatch.
 */
public class RunReport {
    private static final String END = "\u0000end";

    private final Path path;
    private final ObjectMapper json;
    private final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
    private final TreeMap<Long, SecondStats> seconds = new TreeMap<>(); // guarded by this
    private final Thread writer;
    private volatile IOException failure;

    public RunReport(Path directory, UUID runId, RunSettings settings, List<Submission> submissions,
            ObjectMapper json) throws IOException {
        this.json = json;
        Files.createDirectories(directory);
        this.path = directory.resolve(runId + ".jsonl");
        try (BufferedWriter out = Files.newBufferedWriter(path, CREATE_NEW)) {
            out.write(json.writeValueAsString(Map.of("type", "run", "runId", runId, "settings", settings)));
            out.newLine();
            for (Submission submission : submissions) {
                out.write(json.writeValueAsString(Map.of("type", "planned", "submission", submission)));
                out.newLine();
            }
        }
        BufferedWriter out = Files.newBufferedWriter(path, APPEND);
        writer = Thread.ofPlatform().daemon().name("report-writer-" + runId).start(() -> writeLoop(out));
    }

    /** Called for every response. Cheap: memory only, no disk I/O on the caller's thread. */
    public void record(SubmissionResult result) {
        if (!"ACCEPTED".equals(result.outcome())) {
            lines.add(json.writeValueAsString(Map.of("type", "result", "result", result)));
        }
        synchronized (this) {
            seconds.computeIfAbsent(result.recordedAt().getEpochSecond(), SecondStats::new).add(result);
        }
    }

    /** Submissions that were never dispatched because the run stopped. */
    public void recordNotSent(List<Submission> submissions) {
        for (Submission submission : submissions) {
            lines.add(json.writeValueAsString(Map.of("type", "notSent", "clientRequestId", submission.clientRequestId())));
        }
    }

    /** Writes the remaining aggregates and the summary, then closes the file. */
    public void finish(RunSnapshot snapshot) throws IOException {
        queueSeconds(Long.MAX_VALUE);
        lines.add(json.writeValueAsString(Map.of("type", "summary", "summary", snapshot)));
        lines.add(END);
        try {
            writer.join(30_000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        if (writer.isAlive()) throw new IOException("Report writer did not finish within 30 seconds");
        if (failure != null) throw failure;
    }

    /** Set if the background writer failed; the run should stop. */
    public IOException failure() { return failure; }

    public Path path() { return path; }

    private void writeLoop(BufferedWriter out) {
        List<String> batch = new ArrayList<>();
        try (out) {
            while (true) {
                String first = lines.poll(250, TimeUnit.MILLISECONDS);
                // Seconds that ended more than a second ago are complete; late responses are rare.
                queueSeconds(Instant.now().getEpochSecond() - 1);
                if (first == null) { out.flush(); continue; }
                batch.add(first);
                lines.drainTo(batch);
                for (String line : batch) {
                    if (line == END) { out.flush(); return; }
                    out.write(line);
                    out.newLine();
                }
                batch.clear();
                out.flush();
            }
        } catch (IOException ex) {
            failure = ex;
        } catch (InterruptedException ex) {
            failure = new IOException("Report writer interrupted", ex);
        }
    }

    /** Moves aggregates for seconds before {@code before} from memory to the write queue. */
    private void queueSeconds(long before) {
        List<SecondStats> done = new ArrayList<>();
        synchronized (this) {
            while (!seconds.isEmpty() && seconds.firstKey() < before) done.add(seconds.pollFirstEntry().getValue());
        }
        for (SecondStats stats : done) lines.add(json.writeValueAsString(stats.toRecord()));
    }

    private static final class SecondStats {
        private final long second;
        private long accepted, failed, unknown, skipped;
        private double latencySum, latencyMax, schedulingDelayMax;
        private double[] latencies = new double[256];
        private int latencyCount;

        SecondStats(long second) { this.second = second; }

        void add(SubmissionResult result) {
            switch (result.outcome()) {
                case "ACCEPTED" -> accepted++;
                case "UNKNOWN" -> unknown++;
                case "SKIPPED" -> skipped++;
                default -> failed++;
            }
            schedulingDelayMax = Math.max(schedulingDelayMax, result.schedulingDelayMs());
            if ("SKIPPED".equals(result.outcome())) return; // never sent, so no latency
            if (latencyCount == latencies.length) latencies = Arrays.copyOf(latencies, latencyCount * 2);
            latencies[latencyCount++] = result.latencyMs();
            latencySum += result.latencyMs();
            latencyMax = Math.max(latencyMax, result.latencyMs());
        }

        Map<String, Object> toRecord() {
            var latency = new LinkedHashMap<String, Object>();
            if (latencyCount > 0) {
                double[] sorted = Arrays.copyOf(latencies, latencyCount);
                Arrays.sort(sorted);
                latency.put("avg", round(latencySum / latencyCount));
                latency.put("p99", round(sorted[Math.max(0, (int) Math.ceil(latencyCount * 0.99) - 1)]));
                latency.put("max", round(latencyMax));
            }
            var record = new LinkedHashMap<String, Object>();
            record.put("type", "second");
            record.put("second", Instant.ofEpochSecond(second).toString());
            record.put("responses", accepted + failed + unknown);
            record.put("accepted", accepted);
            record.put("failed", failed);
            record.put("unknown", unknown);
            record.put("skipped", skipped);
            record.put("latencyMs", latency);
            record.put("maxSchedulingDelayMs", round(schedulingDelayMax));
            return record;
        }

        private static double round(double value) { return Math.round(value * 10) / 10.0; }
    }
}
