package org.example.backend.config;

import lombok.extern.slf4j.Slf4j;
import org.example.backend.model.enums.CaseStatus;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Keeps the {@code cases.status} check constraint in line with {@link CaseStatus}.
 *
 * <p>Hibernate creates this constraint when it first creates the table, but {@code ddl-auto=update} never
 * changes it afterwards. Adding ROUTED and FAILED therefore left existing databases rejecting them, so the
 * processor could not save results. This runs after Hibernate's schema update on every startup.
 */
@Slf4j
@Component
public class CaseStatusConstraint implements ApplicationRunner {

    private final JdbcTemplate jdbc;

    public CaseStatusConstraint(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        String allowed = Arrays.stream(CaseStatus.values()).map(s -> "'" + s.name() + "'").collect(Collectors.joining(","));
        jdbc.execute("ALTER TABLE cases DROP CONSTRAINT IF EXISTS cases_status_check");
        jdbc.execute("ALTER TABLE cases ADD CONSTRAINT cases_status_check CHECK (status IN (" + allowed + "))");
        log.info("cases.status constraint allows {}", allowed);
    }
}
