package org.example.backend.repository;

import org.example.backend.messaging.CaseProcessedMessage;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Records a batch of processing results in one statement. Only OPEN cases are updated, so a redelivered or
 * duplicate result changes nothing.
 */
@Repository
public class CaseResultWriter {

    private static final String UPDATE = """
            UPDATE cases c SET status = v.status, category = v.category, assigned_team = v.team,
                   processing_attempts = v.attempts, processed_at = v.processed_at
            FROM unnest(?::uuid[], ?::text[], ?::text[], ?::text[], ?::int[], ?::timestamptz[])
                 AS v(id, status, category, team, attempts, processed_at)
            WHERE c.id = v.id AND c.status = 'OPEN'
            """;

    private final JdbcTemplate jdbc;

    public CaseResultWriter(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    /** Returns how many cases changed status. */
    @Transactional
    public int saveAll(List<CaseProcessedMessage> results) {
        if (results.isEmpty()) return 0;
        int n = results.size();
        String[] ids = new String[n], statuses = new String[n], categories = new String[n], teams = new String[n],
                attempts = new String[n], processedAt = new String[n];
        for (int i = 0; i < n; i++) {
            CaseProcessedMessage r = results.get(i);
            ids[i] = r.caseId().toString();
            statuses[i] = r.outcome();
            categories[i] = r.category();
            teams[i] = r.team();
            attempts[i] = String.valueOf(r.attempts());
            processedAt[i] = r.processedAt().toString();
        }
        return jdbc.update(connection -> {
            var statement = connection.prepareStatement(UPDATE);
            String[][] columns = { ids, statuses, categories, teams, attempts, processedAt };
            for (int i = 0; i < columns.length; i++) statement.setArray(i + 1, connection.createArrayOf("text", columns[i]));
            return statement;
        });
    }
}
