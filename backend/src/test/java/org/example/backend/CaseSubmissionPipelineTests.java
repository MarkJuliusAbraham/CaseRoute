package org.example.backend;

import org.example.backend.model.dto.cases.CreateCaseRequest;
import org.example.backend.repository.CaseRepository;
import org.example.backend.service.CaseService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End to end: API acceptance -> Kafka -> consumer -> Postgres, against real containers. */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class CaseSubmissionPipelineTests {

    @Autowired CaseService cases;
    @Autowired CaseRepository repository;
    @Autowired KafkaTemplate<String, String> kafka;

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

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(100);
        assertTrue(condition.getAsBoolean(), "Timed out waiting for the case to be saved");
    }
}
