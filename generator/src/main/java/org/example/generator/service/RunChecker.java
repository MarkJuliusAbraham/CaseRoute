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
        if (run.active() || run.retrying())
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Check the run after it finishes");
        if (run.snapshot().settings().submissionMode() == org.example.generator.model.SubmissionMode.NOOP)
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Connection tests store nothing, so there is nothing to check");
        // Memory: the run lists only non-accepted outcomes, and saved accepted submissions are marked in a bit set
        // (1 bit per submission), so a check of a long run needs no per-submission objects.
        UUID runId = run.id();
        LoadRun.Outcomes told = run.outcomes();
        long acceptedTotal = told.handled() - told.problems().size();
        java.util.BitSet acceptedSaved = new java.util.BitSet();
        long inDatabase = 0, unexpected = 0, processed = 0, awaiting = 0;
        long unknownSaved = 0, rejectedButSaved = 0, blockedButSaved = 0;
        java.util.Set<Long> problemsSaved = new java.util.HashSet<>();
        // Ledger rows per sequence number: a second row for one clientRequestId is a case created twice.
        java.util.BitSet seen = new java.util.BitSet();
        long createdTwice = 0, processedMoreThanOnce = 0;
        boolean resultsReported = false;
        long ledgerEnd = -1; // the ledger's own row count, from its last line; -1 until seen
        var examples = new LinkedHashMap<String, List<UUID>>();
        List<UUID> unexpectedExamples = new ArrayList<>();
        try (Stream<String> lines = fetchLedger(runId)) {
            for (Iterator<String> it = lines.iterator(); it.hasNext(); ) {
                String line = it.next();
                if (line.startsWith("#end,")) { ledgerEnd = Long.parseLong(line.substring(5).strip()); continue; }
                // clientRequestId,status[,resultsReceived]
                String[] fields = line.split(",", 3);
                if (fields.length < 2) continue;
                inDatabase++;
                UUID id = UUID.fromString(fields[0]);
                long sequence = org.example.generator.model.Submission.sequenceOf(runId, id);
                boolean isProcessed = !"OPEN".equals(fields[1]);
                if (fields.length == 3) {
                    resultsReported = true;
                    if (Integer.parseInt(fields[2].strip()) > 1) { processedMoreThanOnce++; example(examples, "processedMoreThanOnce", id); }
                }
                if (sequence > 0 && sequence <= Integer.MAX_VALUE) {
                    if (seen.get((int) sequence)) { createdTwice++; example(examples, "createdTwice", id); continue; }
                    seen.set((int) sequence);
                }
                switch (sequence < 0 ? LoadRun.NOT_SENT : told.of(sequence)) {
                    case LoadRun.ACCEPTED -> {
                        acceptedSaved.set((int) sequence);
                        if (isProcessed) processed++; else awaiting++;
                    }
                    case LoadRun.UNKNOWN -> { unknownSaved++; problemsSaved.add(sequence); }
                    case LoadRun.BLOCKED -> { blockedButSaved++; problemsSaved.add(sequence); example(examples, "blockedButSaved", id); }
                    case LoadRun.FAILED -> { rejectedButSaved++; problemsSaved.add(sequence); example(examples, "rejectedButSaved", id); }
                    default -> { unexpected++; addExample(unexpectedExamples, id); } // skipped, never sent, or not ours
                }
            }
        }
        // A streamed response that stops early still looks like a normal reply. Conclude nothing from a ledger that did
        // not end with its own row count: every case it left out would be reported as missing.
        if (ledgerEnd < 0 || ledgerEnd != inDatabase)
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, ledgerEnd < 0
                    ? String.format("The backend's ledger ended early, after %,d cases, so nothing was concluded. Check again; if it repeats, the backend may be cutting long downloads short (or predates the ledger's end line).", inDatabase)
                    : String.format("The backend's ledger listed %,d cases but said it sent %,d, so nothing was concluded. Check again.", inDatabase, ledgerEnd));
        long missing = acceptedTotal - processed - awaiting;
        // Examples of accepted-but-missing: the first accepted sequence numbers with no saved case.
        for (long sequence = 1; sequence <= told.handled() && missing > 0
                && examples.getOrDefault("acceptedButMissing", List.of()).size() < EXAMPLES; sequence++) {
            if (told.of(sequence) == LoadRun.ACCEPTED && !acceptedSaved.get((int) sequence))
                example(examples, "acceptedButMissing", org.example.generator.model.Submission.idFor(runId, sequence));
        }
        long unknownTotal = 0, blockedTotal = 0;
        for (var entry : new java.util.TreeMap<>(told.problems()).entrySet()) {
            if (entry.getValue() == LoadRun.UNKNOWN) {
                unknownTotal++;
                if (!problemsSaved.contains(entry.getKey()))
                    example(examples, "unknownNotSaved", org.example.generator.model.Submission.idFor(runId, entry.getKey()));
            } else if (entry.getValue() == LoadRun.BLOCKED) blockedTotal++;
        }
        for (UUID id : unexpectedExamples) example(examples, "unexpected", id);

        String verdict = missing > 0 || rejectedButSaved > 0 || blockedButSaved > 0 || unexpected > 0
                || createdTwice > 0 || processedMoreThanOnce > 0 ? "DISCREPANCIES"
                : awaiting > 0 ? "PENDING" : "CONSISTENT";
        var sent = run.snapshot().duplicates();
        var duplicates = new CheckReport.Duplicates(sent.sent(), sent.accepted(), createdTwice,
                resultsReported ? processedMoreThanOnce : -1);
        var report = new CheckReport(run.id(), Instant.now(), verdict,
                summary(verdict, acceptedTotal, processed, awaiting, missing, unknownTotal, unknownSaved, duplicates),
                inDatabase, new CheckReport.Accepted(acceptedTotal, processed, awaiting, missing),
                new CheckReport.Unknown(unknownTotal, unknownSaved, unknownTotal - unknownSaved),
                rejectedButSaved, new CheckReport.Blocked(blockedTotal, blockedButSaved), unexpected, duplicates, examples);
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
            long unknown, long unknownSaved, CheckReport.Duplicates duplicates) {
        String base = baseSummary(verdict, accepted, processed, awaiting, missing, unknown, unknownSaved, duplicates);
        if (duplicates.requestsSent() == 0) return base;
        return base + (duplicates.casesCreatedTwice() == 0 && duplicates.processedMoreThanOnce() <= 0
                ? String.format(" %,d repeated submissions left no extra cases and no double processing.", duplicates.requestsSent())
                : String.format(" Of %,d repeated submissions: %,d extra cases, %,d cases processed more than once.",
                        duplicates.requestsSent(), duplicates.casesCreatedTwice(), Math.max(0, duplicates.processedMoreThanOnce())));
    }

    private static String baseSummary(String verdict, long accepted, long processed, long awaiting, long missing,
            long unknown, long unknownSaved, CheckReport.Duplicates duplicates) {
        String base = switch (verdict) {
            case "CONSISTENT" -> "Every accepted submission was saved and processed.";
            case "PENDING" -> String.format("No discrepancies so far; %,d accepted cases are still being processed.", awaiting);
            default -> missing > 0
                    ? String.format("%,d accepted submissions have no saved case. If the pipeline is still draining, they may be waiting in Kafka: check again.", missing)
                    : duplicates.casesCreatedTwice() > 0 || duplicates.processedMoreThanOnce() > 0
                    ? "Some submissions were stored or processed more than once."
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
