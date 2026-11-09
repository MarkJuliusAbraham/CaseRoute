package org.example.backend.repository;

import org.example.backend.messaging.CaseSubmittedMessage;
import org.example.backend.model.enums.CaseStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

/**
 * Saves a batch of accepted submissions in one transaction. A clientRequestId that already exists
 * is skipped, so redelivered or resubmitted messages never create a second case.
 */
@Repository
public class CaseBatchWriter {

    private static final String INSERT = """
            INSERT INTO cases (id, client_request_id, test_run_id, subject, description, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (client_request_id) DO NOTHING
            """;

    private final JdbcTemplate jdbc;

    public CaseBatchWriter(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    /** Returns how many new cases were inserted. */
    @Transactional
    public int saveAll(List<CaseSubmittedMessage> messages) {
        if (messages.isEmpty()) return 0;
        int[][] counts = jdbc.batchUpdate(INSERT, messages, messages.size(), (statement, message) -> {
            statement.setObject(1, UUID.randomUUID());
            statement.setObject(2, message.clientRequestId());
            statement.setObject(3, message.testRunId());
            statement.setString(4, message.subject());
            statement.setString(5, message.description());
            statement.setString(6, CaseStatus.OPEN.name());
            statement.setTimestamp(7, Timestamp.from(message.acceptedAt()));
        });
        int inserted = 0;
        for (int[] batch : counts) for (int count : batch) if (count > 0) inserted += count;
        return inserted;
    }
}
