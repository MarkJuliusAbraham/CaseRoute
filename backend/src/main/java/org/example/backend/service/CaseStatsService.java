package org.example.backend.service;

import org.example.backend.model.dto.cases.CaseStats;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.UUID;

@Service
public class CaseStatsService {
    private final JdbcTemplate jdbc;
    private final JdbcTemplate streaming;

    public CaseStatsService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        // PostgreSQL streams rows in chunks only inside a transaction and with a fetch size set.
        this.streaming = new JdbcTemplate(jdbc.getDataSource());
        this.streaming.setFetchSize(5000);
    }

    /**
     * The run's ledger for the checker: one {@code clientRequestId,status} line per saved case, streamed so
     * a million-case run never sits in memory.
     */
    @Transactional(readOnly = true)
    public void writeLedger(UUID testRunId, Writer out) {
        streaming.query("SELECT client_request_id, status FROM cases WHERE test_run_id = ?", rs -> {
            try {
                out.write(rs.getString(1));
                out.write(',');
                out.write(rs.getString(2));
                out.write('\n');
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }, testRunId);
    }

    public CaseStats stats(UUID testRunId) {
        String filter = testRunId == null ? "" : " WHERE test_run_id = ?";
        Object[] args = testRunId == null ? new Object[0] : new Object[] { testRunId };
        long[] byStatus = new long[3]; // open, routed, failed
        long[] saved = { 0 };
        jdbc.query("SELECT status, count(*) AS n FROM cases" + filter + " GROUP BY status", rs -> {
            long n = rs.getLong("n");
            saved[0] += n;
            switch (rs.getString("status")) {
                case "ROUTED" -> byStatus[1] += n;
                case "FAILED" -> byStatus[2] += n;
                default -> byStatus[0] += n;
            }
        }, args);
        Long awaiting = jdbc.queryForObject("SELECT count(*) FROM outbox" + filter, Long.class, args);
        return new CaseStats(saved[0], byStatus[0], byStatus[1], byStatus[2], awaiting == null ? 0 : awaiting);
    }
}
