package org.example.processor;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Against a real Kafka: every case gets exactly one result, and slow cases do not hold back fast ones. */
@SpringBootTest(properties = {
        "caseroute.processing.mean-ms=20",
        "caseroute.processing.slow-fraction=0.05",
        "caseroute.processing.slow-ms=4000",
        "caseroute.processing.transient-failure-rate=0",
        "caseroute.processing.permanent-failure-rate=0",
        "caseroute.processing.consumers=2",
        "spring.kafka.consumer.max-poll-records=100",
})
@Import(CaseConsumerLoopTests.Containers.class)
class CaseConsumerLoopTests {

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean @ServiceConnection
        KafkaContainer kafka() { return new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.2")); }

        // In the real system the backend creates these topics.
        @Bean NewTopic saved() { return TopicBuilder.name("cases.saved").partitions(2).replicas(1).build(); }
        @Bean NewTopic processed() { return TopicBuilder.name("cases.processed").partitions(2).replicas(1).build(); }
        @Bean NewTopic savedDlt() { return TopicBuilder.name("cases.saved.DLT").partitions(1).replicas(1).build(); }
    }

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ConsumerFactory<String, String> consumers;
    @Autowired ObjectMapper json;
    @Autowired ProcessingProfile profile;

    @Test
    void everyCaseGetsOneResultAndSlowCasesDoNotBlockFastOnes() throws Exception {
        int n = 1000;
        Map<UUID, ProcessingPlan> plans = new HashMap<>();
        for (int i = 0; i < n; i++) {
            UUID caseId = UUID.randomUUID(), requestId = UUID.randomUUID();
            plans.put(caseId, ProcessingPlan.forCase(requestId, profile));
            String message = json.writeValueAsString(new CaseSavedMessage(i, caseId, requestId, null, Instant.now()));
            kafka.send("cases.saved", caseId.toString(), message);
        }
        kafka.flush();
        long slow = plans.values().stream().filter(p -> p.durationMs() == 4000).count();
        assertTrue(slow > 0, "the sample should include slow cases");

        Map<UUID, Integer> results = new HashMap<>();
        Duration fastDone = null;
        Instant firstResult = null;
        try (Consumer<String, String> reader = consumers.createConsumer("test-" + UUID.randomUUID(), "results")) {
            reader.subscribe(List.of("cases.processed"));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (results.size() < n && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : reader.poll(Duration.ofMillis(200))) {
                    UUID id = UUID.fromString(json.readTree(record.value()).path("caseId").asText());
                    results.merge(id, 1, Integer::sum);
                    if (firstResult == null) firstResult = Instant.now();
                }
                if (fastDone == null && results.size() >= n - slow) fastDone = Duration.between(firstResult, Instant.now());
            }
        }
        assertEquals(n, results.size(), "every case produced a result");
        assertTrue(results.values().stream().allMatch(count -> count == 1), "no case produced two results");
        // Timed from the first result, so Kafka's consumer-group start-up delay is excluded. With 1,000 cases in
        // batches of 100 over 2 consumers, the old fetch-pause behaviour needed ~5 rounds, each held ~4 s by its slow
        // cases: about 16 s or more. Without the pause, the fast cases finish while the first slow ones still run.
        assertNotNull(fastDone);
        assertTrue(fastDone.toMillis() < 4000, "fast cases finished in " + fastDone.toMillis() + " ms");
    }
}
