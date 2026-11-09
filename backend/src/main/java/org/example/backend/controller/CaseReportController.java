package org.example.backend.controller;

import org.example.backend.model.dto.cases.CaseReportPage;
import org.example.backend.model.dto.cases.CaseStats;
import org.example.backend.service.CaseReportService;
import org.example.backend.service.CaseStatsService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@RestController
@RequestMapping("/api/simulation/cases")
@ConditionalOnProperty(name = "simulation.reports-enabled", havingValue = "true")
public class CaseReportController {
    private final CaseReportService reports;
    private final CaseStatsService stats;
    public CaseReportController(CaseReportService reports, CaseStatsService stats) { this.reports = reports; this.stats = stats; }

    /** Pipeline counts for the dashboard: saved, processed (routed/failed) and waiting in the outbox. */
    @GetMapping("/stats")
    public CaseStats stats(@RequestParam(required = false) UUID testRunId) {
        return stats.stats(testRunId);
    }

    /** Every saved case of a run as {@code clientRequestId,status} lines, for the generator's checker. */
    @GetMapping(value = "/ledger", produces = "text/plain")
    public ResponseEntity<StreamingResponseBody> ledger(@RequestParam UUID testRunId) {
        StreamingResponseBody body = output -> {
            var writer = new java.io.BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8), 1 << 16);
            stats.writeLedger(testRunId, writer);
            writer.flush();
        };
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(body);
    }

    @GetMapping
    public CaseReportPage page(@RequestParam(required = false) UUID testRunId,
            @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "25") int size) {
        if (page < 0 || size < 1 || size > 100)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Use page >= 0 and size between 1 and 100");
        return reports.page(testRunId, page, size);
    }

    @GetMapping(value = "/export", produces = "text/csv")
    public ResponseEntity<StreamingResponseBody> export(@RequestParam(required = false) UUID testRunId) {
        StreamingResponseBody body = output -> {
            var writer = new OutputStreamWriter(output, StandardCharsets.UTF_8);
            reports.export(testRunId, writer);
            writer.flush();
        };
        return ResponseEntity.ok().header("Content-Disposition",
                ContentDisposition.attachment().filename("received-cases.csv").build().toString()).body(body);
    }
}
