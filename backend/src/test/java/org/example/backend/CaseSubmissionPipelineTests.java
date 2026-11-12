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
    @Autowired org.example.backend.service.PipelineControl pipeline;
    @Autowired @org.springframework.beans.factory.annotation.Qualifier("intakeKafkaTemplate")
    KafkaTemplate<String, String> intakeKafka;
    @Autowired org.example.backend.messaging.IntakeProducer intake;
    @Autowired org.example.backend.messaging.KafkaTopics topics;

    @Test
    void intakeProducerSettingsChangeBetweenRunsAndSubmissionsUseTheNewProducer() throws Exception {
        var original = intake.settings();
        assertEquals(5, original.lingerMs(), "starts from spring.kafka.producer.properties.linger.ms");
        try {
            var tuned = new org.example.backend.messaging.IntakeProducer.Settings(20, 65_536, "lz4", 1, 3);
            assertEquals(tuned, intake.apply(tuned));
            assertEquals(3, intake.templates().size());
            var runId = UUID.randomUUID();
            for (int i = 0; i < 20; i++)
                cases.submitCase(new CreateCaseRequest(UUID.randomUUID(), runId, "Tuned " + i, "lz4, linger 20 ms."));
            // Used in turn: every producer sent some of the 20.
            for (var template : intake.templates()) {
                double sent = template.metrics().entrySet().stream()
                        .filter(e -> e.getKey().group().equals("producer-metrics") && e.getKey().name().equals("record-send-total"))
                        .mapToDouble(e -> ((Number) e.getValue().metricValue()).doubleValue()).sum();
                assertTrue(sent >= 6, "each producer carried its share: " + sent);
            }
            var metrics = intake.metrics();
            assertTrue(metrics.get("record-send-rate") != null, "metrics come from the new producer: " + metrics);
            assertTrue(metrics.get("compression-rate-avg") != null && metrics.get("compression-rate-avg") < 1.0,
                    "lz4 actually compresses: " + metrics);
            assertEquals(0, intake.sending());
            await(() -> stats.stats(runId).saved() == 20);
        } finally {
            intake.apply(original);
            assertEquals(1, intake.templates().size(), "back to one producer");
        }
    }

    @Test
    void invalidProducerSettingsAreRefusedAndChangeNothing() {
        var before = intake.settings();
        for (var bad : List.of(
                new org.example.backend.messaging.IntakeProducer.Settings(-1, 16_384, "none", 5, 1),
                new org.example.backend.messaging.IntakeProducer.Settings(5, 100, "none", 5, 1),
                new org.example.backend.messaging.IntakeProducer.Settings(5, 16_384, "brotli", 5, 1),
                new org.example.backend.messaging.IntakeProducer.Settings(5, 16_384, "none", 6, 1),
                new org.example.backend.messaging.IntakeProducer.Settings(5, 16_384, "none", 5, 0),
                new org.example.backend.messaging.IntakeProducer.Settings(5, 16_384, "none", 5, 9))) {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> intake.apply(bad));
        }
        assertEquals(before, intake.settings());
    }

    @Test
    void partitionsCanBeAddedButNeverRemoved() {
        var counts = topics.partitions();
        int saved = counts.get("cases.saved");
        assertEquals(counts.get("cases.submitted"), counts.get("cases.warmup"));
        assertEquals(saved + 1, topics.increase("cases.saved", saved + 1).get("cases.saved"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> topics.increase("cases.saved", saved), "Kafka cannot remove partitions");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> topics.increase("cases.warmup", 50), "the warm-up topic follows cases.submitted");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> topics.increase("cases.submitted.DLT", 5));
    }

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
        var row = jdbc.queryForMap("SELECT status, category, assigned_team, processing_attempts, results_received FROM cases WHERE id = ?", created.id());
        assertEquals("ROUTED", row.get("status"));
        assertEquals("BILLING", row.get("category"));
        assertEquals("Billing Support", row.get("assigned_team"));
        assertEquals(1, ((Number) row.get("processing_attempts")).intValue());
        assertEquals(2, ((Number) row.get("results_received")).intValue(),
                "the ignored second result is still counted, so a check can see the case was processed twice");
        var ledger = new java.io.StringWriter();
        stats.writeLedger(runId, ledger);
        assertEquals(created.clientRequestId() + ",ROUTED,2\n#end,1\n", ledger.toString());
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
        assertEquals(request.clientRequestId() + ",OPEN,0\n#end,1\n", ledger.toString());
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

    @Test
    void customerSubmissionsUseTheirOwnProducerSeparateFromTheRelay() {
        cases.submitCase(new CreateCaseRequest(UUID.randomUUID(), UUID.randomUUID(), "Own producer", "Intake."));
        org.junit.jupiter.api.Assertions.assertNotSame(kafka.getProducerFactory(), intakeKafka.getProducerFactory(),
                "intake and the outbox relay must not share a producer");
        java.util.function.Function<KafkaTemplate<String, String>, java.util.Set<String>> clientIds = template ->
                template.metrics().keySet().stream().map(name -> name.tags().get("client-id"))
                        .filter(java.util.Objects::nonNull).collect(java.util.stream.Collectors.toSet());
        assertTrue(clientIds.apply(intakeKafka).stream().allMatch(id -> id.startsWith("backend-intake")),
                "submissions went through the intake producer: " + clientIds.apply(intakeKafka));
        assertTrue(clientIds.apply(kafka).stream().noneMatch(id -> id.startsWith("backend-intake")));
    }

    @Test
    void theSameSubmissionSentTwiceThroughKafkaBecomesOneCase() throws Exception {
        UUID runId = UUID.randomUUID();
        var request = new CreateCaseRequest(UUID.randomUUID(), runId, "Twice", "Double-clicked submit.");
        cases.submitCase(request);
        cases.submitCase(request); // both accepted (202): Kafka holds two messages for one clientRequestId
        await(() -> stats.stats(runId).saved() >= 1);
        Thread.sleep(1500); // time for the second message to be written too, had it created a case
        assertEquals(1, stats.stats(runId).saved());
        var ledger = new java.io.StringWriter();
        stats.writeLedger(runId, ledger);
        assertEquals(1, ledger.toString().lines().filter(line -> !line.startsWith("#")).count(), "one case: " + ledger);
        assertTrue(ledger.toString().endsWith("#end,1\n"));
    }

    @Test
    void pausedPipelineHoldsSubmissionsInKafkaAndCatchesUpOnResume() throws Exception {
        UUID runId = UUID.randomUUID();
        pipeline.setPaused(true);
        try {
            cases.submitCase(new CreateCaseRequest(UUID.randomUUID(), runId, "Paused", "Waits in Kafka."));
            Thread.sleep(3000); // ample time for the case writer to have saved it, were it running
            assertEquals(0, stats.stats(runId).saved(), "nothing is saved while paused");
        } finally {
            pipeline.setPaused(false);
        }
        await(() -> stats.stats(runId).saved() == 1);
        await(() -> stats.stats(runId).awaitingRelay() == 0);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(100);
        assertTrue(condition.getAsBoolean(), "Timed out waiting for the case to be saved");
    }
}
