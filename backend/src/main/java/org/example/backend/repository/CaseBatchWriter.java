package org.example.backend.repository;

import org.example.backend.messaging.CaseSubmittedMessage;
import org.example.backend.model.entity.OutboxEvent;
import org.example.backend.model.enums.CaseStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Saves a batch of accepted submissions and their outbox rows in ONE statement, so in one transaction:
 * a case is never saved without its "case.saved" outbox row, and never announced without being saved.
 *
 * <p>A clientRequestId that already exists is skipped ({@code ON CONFLICT DO NOTHING}), and only cases that
 * were actually inserted get an outbox row, so redelivered or resubmitted messages produce no duplicates.
 */
@Repository
public class CaseBatchWriter {

    // One round trip per batch: the arrays become rows, inserted cases are RETURNED and fed into the outbox.
    private static final String INSERT = """
            WITH inserted AS (
                INSERT INTO cases (id, client_request_id, test_run_id, subject, description, status, created_at)
                SELECT * FROM unnest(?::uuid[], ?::uuid[], ?::uuid[], ?::text[], ?::text[], ?::text[], ?::timestamptz[])
                ON CONFLICT (client_request_id) DO NOTHING
                RETURNING id, client_request_id, test_run_id
            )
            INSERT INTO outbox (event_type, case_id, client_request_id, test_run_id, created_at)
            SELECT ?, id, client_request_id, test_run_id, now() FROM inserted
            """;

    private final JdbcTemplate jdbc;

    public CaseBatchWriter(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    /** Returns how many new cases were inserted (each with one outbox row). */
    @Transactional
    public int saveAll(List<CaseSubmittedMessage> messages) {
        if (messages.isEmpty()) return 0;
        int n = messages.size();
        String[] ids = new String[n], requestIds = new String[n], runIds = new String[n], subjects = new String[n],
                descriptions = new String[n], statuses = new String[n], createdAt = new String[n];
        for (int i = 0; i < n; i++) {
            CaseSubmittedMessage m = messages.get(i);
            ids[i] = UUID.randomUUID().toString();
            requestIds[i] = m.clientRequestId().toString();
            runIds[i] = m.testRunId() == null ? null : m.testRunId().toString();
            subjects[i] = m.subject();
            descriptions[i] = m.description();
            statuses[i] = CaseStatus.OPEN.name();
            createdAt[i] = m.acceptedAt().toString();
        }
        return jdbc.update(connection -> {
            var statement = connection.prepareStatement(INSERT);
            String[][] columns = { ids, requestIds, runIds, subjects, descriptions, statuses, createdAt };
            for (int i = 0; i < columns.length; i++) statement.setArray(i + 1, connection.createArrayOf("text", columns[i]));
            statement.setString(columns.length + 1, OutboxEvent.CASE_SAVED);
            return statement;
        });
    }
}
