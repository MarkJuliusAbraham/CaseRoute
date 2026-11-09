package org.example.backend;

import org.example.backend.model.dto.cases.CreateCaseRequest;
import org.example.backend.repository.CaseRepository;
import org.example.backend.service.CaseService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.example.backend.service.CaseStatsService;
import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End to end: API acceptance -> Kafka -> writer -> Postgres + outbox -> relay -> cases.saved, against real containers. */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class CaseSubmissionPipelineTests {

    @Autowired CaseService cases;
    @Autowired CaseRepository repository;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ConsumerFactory<String, String> consumers;
    @Autowired CaseStatsService stats;
    @Autowired org.springframework.jdbc.core.JdbcTemplate jdbc;
    @Autowired org.example.backend.config.CaseStatusConstraint statusConstraint;
    @Autowired org.example.backend.controller.CaseController controller;

    @Test
    void processingResultsFromKafkaAreRecordedOnceAndUnreadableOnesDoNotBlock() throws Exception {
        UUID runId = UUID.randomUUID();
        var created = cases.createCase(new CreateCaseRequest(UUID.randomUUID(), runId, "Refund", "Charged twice."));
        String routed = "{\"caseId\":\"%s\",\"outcome\":\"ROUTED\",\"category\":\"BILLING\",\"team\":\"Billing Support\",\"attempts\":1,\"processedAt\":\"2026-11-08T12:00:00Z\"}"
                .formatted(created.id());
        String failedLater = routed.replace("ROUTED", "FAILED").replace("\"Billing Support\"", "null");
        kafka.send("cases.processed", "bad", "{not json").get();
        kafka.send("cases.processed", created.id().toString(), routed).get();
        kafka.send("cases.processed", created.id().toString(), failedLater).get(); // duplicate result: ignored

        await(() -> stats.stats(runId).routed() == 1);
        Thread.sleep(1000);
        var row = jdbc.queryForMap("SELECT status, category, assigned_team, processing_attempts FROM cases WHERE id = ?", created.id());
        assertEquals("ROUTED", row.get("status"));
        assertEquals("BILLING", row.get("category"));
        assertEquals("Billing Support", row.get("assigned_team"));
        assertEquals(1, ((Number) row.get("processing_attempts")).intValue());
    }

    @Test
    void startupWidensAnOutdatedStatusConstraintSoProcessedStatusesCanBeSaved() throws Exception {
        // Simulate a database created before ROUTED/FAILED existed. NOT VALID: other tests may already have
        // ROUTED rows in this shared container; only new writes are checked.
        jdbc.execute("ALTER TABLE cases DROP CONSTRAINT IF EXISTS cases_status_check");
        jdbc.execute("ALTER TABLE cases ADD CONSTRAINT cases_status_check CHECK (status IN ('OPEN','IN_PROGRESS','RESOLVED')) NOT VALID");
        var created = cases.createCase(new CreateCaseRequest(UUID.randomUUID(), UUID.randomUUID(), "Old schema", "Constraint test."));

        statusConstraint.run(null);

        assertEquals(1, jdbc.update("UPDATE cases SET status = 'ROUTED' WHERE id = ?", created.id()));
    }

    @Test
    void savedCaseIsAnnouncedOnceThroughTheOutboxEvenWhenResubmitted() throws Exception {
        UUID runId = UUID.randomUUID();
        UUID clientRequestId = UUID.randomUUID();
        var request = new CreateCaseRequest(clientRequestId, runId, "Login problem", "Cannot sign in.");
        try (Consumer<String, String> saved = consumers.createConsumer("test-" + UUID.randomUUID(), "saved")) {
            saved.subscribe(List.of("cases.saved"));
            cases.submitCase(request);
            cases.submitCase(request);

            List<String> announcements = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            // Keep reading a few seconds after the first match to catch any duplicate announcement.
            long settle = Long.MAX_VALUE;
            while (System.nanoTime() < Math.min(deadline, settle)) {
                for (var record : saved.poll(Duration.ofMillis(200))) {
                    if (record.value().contains(clientRequestId.toString())) {
                        announcements.add(record.value());
                        settle = Math.min(settle, System.nanoTime() + Duration.ofSeconds(3).toNanos());
                    }
                }
            }
            assertEquals(1, announcements.size(), "Each saved case is announced exactly once");
            UUID caseId = repository.findByTestRunId(runId, Pageable.unpaged()).getContent().getFirst().getId();
            assertTrue(announcements.getFirst().contains(caseId.toString()));
        }
        await(() -> stats.stats(runId).awaitingRelay() == 0);
        assertEquals(1, stats.stats(runId).saved());
        assertEquals(1, stats.stats(runId).open());
    }

    @Test
    void directSubmissionAlsoGetsAnOutboxRowThatTheRelayPublishes() throws Exception {
        UUID runId = UUID.randomUUID();
        cases.createCase(new CreateCaseRequest(UUID.randomUUID(), runId, "Direct", "Outbox too."));
        await(() -> stats.stats(runId).awaitingRelay() == 0);
        assertEquals(1, stats.stats(runId).saved());
    }

    @Test
    void acceptedSubmissionIsSavedOnceEvenWhenResubmitted() throws Exception {
        UUID runId = UUID.randomUUID();
        UUID clientRequestId = UUID.randomUUID();
        var request = new CreateCaseRequest(clientRequestId, runId, "Duplicate charge", "Charged twice.");

        assertEquals("ACCEPTED", cases.submitCase(request).status());
        assertEquals("ACCEPTED", cases.submitCase(request).status());

        await(() -> repository.findByTestRunId(runId, Pageable.unpaged()).getTotalElements() == 1);
        Thread.sleep(500); // give the duplicate time to be processed
        var saved = repository.findByTestRunId(runId, Pageable.unpaged()).getContent();
        assertEquals(1, saved.size());
        assertEquals(clientRequestId, saved.getFirst().getClientRequestId());
    }

    @Test
    void directSubmissionIsSavedBeforeTheReply() {
        UUID runId = UUID.randomUUID();
        var created = cases.createCase(new CreateCaseRequest(UUID.randomUUID(), runId, "Direct", "No Kafka."));

        assertEquals(1, repository.findByTestRunId(runId, Pageable.unpaged()).getTotalElements());
        assertEquals(created.id(), repository.findByTestRunId(runId, Pageable.unpaged()).getContent().getFirst().getId());
    }

    @Test
    void unreadableMessageDoesNotBlockTheCasesBehindIt() throws Exception {
        UUID runId = UUID.randomUUID();
        kafka.send("cases.submitted", "bad", "{not json").get();
        cases.submitCase(new CreateCaseRequest(UUID.randomUUID(), runId, "After poison", "Still saved."));

        await(() -> repository.findByTestRunId(runId, Pageable.unpaged()).getTotalElements() == 1);
    }

    @Test
    void directRetriesReturnTheSavedCaseAndTheStatusAndLedgerReportIt() {
        UUID runId = UUID.randomUUID();
        var request = new CreateCaseRequest(UUID.randomUUID(), runId, "Refund", "Charged twice.");
        var first = controller.createCaseDirectly(request);
        var retry = controller.createCaseDirectly(request); // the first reply was "lost"; the client retries
        assertEquals(201, first.getStatusCode().value());
        assertEquals(200, retry.getStatusCode().value());
        assertEquals(first.getBody().id(), retry.getBody().id());
        assertEquals(1, stats.stats(runId).saved());

        // Same clientRequestId with different content is not a retry: still a conflict.
        var different = new CreateCaseRequest(request.clientRequestId(), runId, "Other", "Different text.");
        org.junit.jupiter.api.Assertions.assertThrows(
                org.springframework.dao.DataIntegrityViolationException.class, () -> controller.createCaseDirectly(different));

        var status = controller.status(request.clientRequestId());
        assertEquals(first.getBody().id(), status.caseId());
        assertEquals("OPEN", status.status().name());
        org.junit.jupiter.api.Assertions.assertThrows(
                org.example.backend.exception.CaseNotFoundException.class, () -> controller.status(UUID.randomUUID()));

        var ledger = new java.io.StringWriter();
        stats.writeLedger(runId, ledger);
        assertEquals(request.clientRequestId() + ",OPEN\n", ledger.toString());
    }

    @Test
    void warmUpReachesItsOwnTopicAndNeverCreatesACase() throws Exception {
        UUID runId = UUID.randomUUID();
        UUID clientRequestId = UUID.randomUUID();
        try (Consumer<String, String> warmups = consumers.createConsumer("test-" + UUID.randomUUID(), "warmup")) {
            warmups.subscribe(List.of("cases.warmup"));
            warmups.poll(Duration.ofSeconds(2)); // join the group before the message is written
            cases.warmUp(new CreateCaseRequest(clientRequestId, runId, "Warm-up", "Stores nothing."));
            boolean seen = false;
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (!seen && System.nanoTime() < deadline) {
                for (var record : warmups.poll(Duration.ofMillis(200))) {
                    seen |= clientRequestId.toString().equals(record.key());
                }
            }
            assertTrue(seen, "the warm-up was written to cases.warmup");
        }
        Thread.sleep(2000); // long enough for the submission consumer to have saved it, had it been a real one
        assertTrue(repository.findByClientRequestId(clientRequestId).isEmpty());
        assertEquals(0, stats.stats(runId).saved());
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(100);
        assertTrue(condition.getAsBoolean(), "Timed out waiting for the case to be saved");
    }
}
