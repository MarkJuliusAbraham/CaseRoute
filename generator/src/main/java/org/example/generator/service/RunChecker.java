package org.example.generator.service;

import org.example.generator.model.CheckReport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Reconciliation: compares the client's ledger (what the generator was told about each submission) with the
 * system's ledger (the cases the backend saved for the run, with their status).
 *
 * <p>It lives in the generator because only the client knows what it was promised. It reads the system's side
 * through a backend endpoint, never the database directly, so it audits the system from the outside.
 * Each result is also written next to the run report as {@code <runId>.check.json}.
 */
@Service
public class RunChecker {
    private static final byte NONE = 0, OPEN = 1, PROCESSED = 2;
    private static final int EXAMPLES = 10;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final URI ledgerUrl;
    private final Path directory;
    private final ObjectMapper json;
    private final Map<UUID, CheckReport> latest = new ConcurrentHashMap<>();

    public RunChecker(@Value("${generator.target-url}") URI target,
            @Value("${generator.report-directory}") Path directory, ObjectMapper json) {
        this.ledgerUrl = target.resolve("/api/simulation/cases/ledger");
        this.directory = directory;
        this.json = json;
    }

    public CheckReport check(LoadRun run) {
        if (run.active()) throw new ResponseStatusException(HttpStatus.CONFLICT, "Check the run after it finishes");
        var submissions = run.submissions();
        byte[] told = run.outcomes();
        Map<UUID, Integer> index = HashMap.newHashMap(submissions.size());
        for (int i = 0; i < submissions.size(); i++) index.put(submissions.get(i).clientRequestId(), i);

        byte[] saved = new byte[submissions.size()];
        long inDatabase = 0, unexpected = 0;
        List<UUID> unexpectedExamples = new ArrayList<>();
        try (Stream<String> lines = fetchLedger(run.id())) {
            for (Iterator<String> it = lines.iterator(); it.hasNext(); ) {
                String line = it.next();
                int comma = line.indexOf(',');
                if (comma < 0) continue;
                inDatabase++;
                UUID id = UUID.fromString(line.substring(0, comma));
                Integer i = index.get(id);
                if (i == null) { unexpected++; addExample(unexpectedExamples, id); continue; }
                saved[i] = "OPEN".equals(line.substring(comma + 1)) ? OPEN : PROCESSED;
            }
        }

        long acceptedTotal = 0, processed = 0, awaiting = 0, missing = 0;
        long unknownTotal = 0, unknownSaved = 0, rejectedButSaved = 0;
        var examples = new LinkedHashMap<String, List<UUID>>();
        for (int i = 0; i < told.length; i++) {
            UUID id = submissions.get(i).clientRequestId();
            switch (told[i]) {
                case LoadRun.ACCEPTED -> {
                    acceptedTotal++;
                    if (saved[i] == PROCESSED) processed++;
                    else if (saved[i] == OPEN) awaiting++;
                    else { missing++; example(examples, "acceptedButMissing", id); }
                }
                case LoadRun.UNKNOWN -> {
                    unknownTotal++;
                    if (saved[i] != NONE) unknownSaved++;
                    else example(examples, "unknownNotSaved", id);
                }
                case LoadRun.FAILED -> {
                    if (saved[i] != NONE) { rejectedButSaved++; example(examples, "rejectedButSaved", id); }
                }
                default -> { // skipped or never sent: nothing should exist
                    if (saved[i] != NONE) { unexpected++; example(examples, "unexpected", id); }
                }
            }
        }
        for (UUID id : unexpectedExamples) example(examples, "unexpected", id);

        String verdict = missing > 0 || rejectedButSaved > 0 || unexpected > 0 ? "DISCREPANCIES"
                : awaiting > 0 ? "PENDING" : "CONSISTENT";
        var report = new CheckReport(run.id(), Instant.now(), verdict,
                summary(verdict, acceptedTotal, processed, awaiting, missing, unknownTotal, unknownSaved),
                inDatabase, new CheckReport.Accepted(acceptedTotal, processed, awaiting, missing),
                new CheckReport.Unknown(unknownTotal, unknownSaved, unknownTotal - unknownSaved),
                rejectedButSaved, unexpected, examples);
        latest.put(run.id(), report);
        save(report);
        return report;
    }

    public Optional<CheckReport> latest(UUID runId) { return Optional.ofNullable(latest.get(runId)); }

    private Stream<String> fetchLedger(UUID runId) {
        var request = HttpRequest.newBuilder(URI.create(ledgerUrl + "?testRunId=" + runId))
                .timeout(Duration.ofMinutes(2)).GET().build();
        try {
            HttpResponse<Stream<String>> response = http.send(request, HttpResponse.BodyHandlers.ofLines());
            if (response.statusCode() != 200) {
                response.body().close();
                throw new ResponseStatusException(HttpStatus.BAD_GATEWAY,
                        "Backend ledger returned HTTP " + response.statusCode()
                                + ". Is the backend running with simulation.reports-enabled=true?");
            }
            return response.body();
        } catch (IOException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Could not read the backend ledger: " + ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Interrupted");
        }
    }

    private static String summary(String verdict, long accepted, long processed, long awaiting, long missing,
            long unknown, long unknownSaved) {
        String base = switch (verdict) {
            case "CONSISTENT" -> "Every accepted submission was saved and processed.";
            case "PENDING" -> String.format("No discrepancies so far; %,d accepted cases are still being processed.", awaiting);
            default -> missing > 0
                    ? String.format("%,d accepted submissions have no saved case. If the pipeline is still draining, they may be waiting in Kafka: check again.", missing)
                    : "The system holds cases the client was told were rejected or never sent.";
        };
        if (unknown == 0) return base;
        return base + String.format(" Of %,d unknown outcomes, %,d were saved anyway and %,d never arrived.",
                unknown, unknownSaved, unknown - unknownSaved);
    }

    private static void example(Map<String, List<UUID>> examples, String category, UUID id) {
        addExample(examples.computeIfAbsent(category, key -> new ArrayList<>()), id);
    }

    private static void addExample(List<UUID> list, UUID id) {
        if (list.size() < EXAMPLES) list.add(id);
    }

    private void save(CheckReport report) {
        try {
            Files.createDirectories(directory);
            Files.writeString(directory.resolve(report.runId() + ".check.json"), json.writeValueAsString(report));
        } catch (IOException ignored) {
            // The result is still returned and kept in memory; the file is a convenience.
        }
    }
}
