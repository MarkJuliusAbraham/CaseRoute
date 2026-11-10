package org.example.backend.messaging;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.example.backend.service.PipelineControl;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Publishes outbox rows to the {@code cases.saved} topic, then deletes them, in one database transaction:
 * <ol>
 *   <li>Lock a batch of the oldest rows ({@code FOR UPDATE SKIP LOCKED}, so several relays could share the table).</li>
 *   <li>Send each to Kafka and wait for every confirmation.</li>
 *   <li>Delete the batch and commit.</li>
 * </ol>
 * If Kafka is down or the backend crashes before the commit, the rows stay and are sent again later. That can
 * deliver a message twice, never zero times; the processor ignores cases it has already processed.
 */
@Slf4j
@Component
public class OutboxRelay {

    private static final String SELECT = """
            SELECT id, case_id, client_request_id, test_run_id, created_at FROM outbox
            ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED
            """;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final String topic;
    private final int batchSize;
    private final PipelineControl pipeline;

    public OutboxRelay(JdbcTemplate jdbc, TransactionTemplate transaction, KafkaTemplate<String, String> kafka,
            ObjectMapper json, @Value("${caseroute.kafka.saved-topic}") String topic,
            @Value("${caseroute.outbox.batch-size}") int batchSize, PipelineControl pipeline) {
        this.pipeline = pipeline;
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.kafka = kafka;
        this.json = json;
        this.topic = topic;
        this.batchSize = batchSize;
    }

    /** Drains full batches back to back for up to 2 seconds, then yields until the next tick. */
    @Scheduled(fixedDelayString = "${caseroute.outbox.poll-interval-ms}")
    public void relay() {
        if (pipeline.isPaused()) return; // rows wait in the outbox until the pipeline is resumed
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        try {
            int sent;
            do { sent = publishBatch(); } while (sent == batchSize && System.nanoTime() < deadline);
        } catch (RuntimeException ex) {
            // Rows were not deleted; they are retried on the next tick.
            log.warn("Outbox relay could not publish; will retry: {}", ex.toString());
        }
    }

    int publishBatch() {
        Integer sent = transaction.execute(status -> {
            List<CaseSavedMessage> rows = jdbc.query(SELECT, (rs, i) -> new CaseSavedMessage(
                    rs.getLong("id"), rs.getObject("case_id", UUID.class), rs.getObject("client_request_id", UUID.class),
                    rs.getObject("test_run_id", UUID.class), rs.getTimestamp("created_at").toInstant()), batchSize);
            if (rows.isEmpty()) return 0;
            List<CompletableFuture<SendResult<String, String>>> confirmations = new ArrayList<>(rows.size());
            for (CaseSavedMessage row : rows) {
                // Keyed by case ID: all events for one case go to the same partition, in order.
                confirmations.add(kafka.send(topic, row.caseId().toString(), json.writeValueAsString(row)));
            }
            CompletableFuture.allOf(confirmations.toArray(CompletableFuture[]::new)).orTimeout(10, TimeUnit.SECONDS).join();
            Long[] ids = rows.stream().map(CaseSavedMessage::outboxId).toArray(Long[]::new);
            jdbc.update(connection -> {
                var statement = connection.prepareStatement("DELETE FROM outbox WHERE id = ANY(?)");
                statement.setArray(1, connection.createArrayOf("bigint", ids));
                return statement;
            });
            return rows.size();
        });
        return sent == null ? 0 : sent;
    }

    /** The {@code cases.saved} message. {@code outboxId} lets consumers spot a redelivered message if they need to. */
    public record CaseSavedMessage(long outboxId, UUID caseId, UUID clientRequestId, UUID testRunId,
            java.time.Instant savedAt) {
    }
}
