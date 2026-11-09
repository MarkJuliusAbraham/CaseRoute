package org.example.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Processes one case from cases.saved: simulate the work (retrying transient errors), then publish the result
 * (ROUTED or FAILED) to cases.processed. Failed cases are also copied to cases.saved.DLT with the reason.
 * The processor never touches the database; the backend consumes cases.processed and records the result.
 *
 * <p>Called on its own virtual thread by {@link CaseConsumerLoop}. Returns only once the result is confirmed by
 * Kafka, so the case may then be counted as finished.
 */
@Component
public class CaseProcessor {
    private static final Logger log = LoggerFactory.getLogger(CaseProcessor.class);

    private final ProcessingProfile profile;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final String deadLetterTopic;
    private final String resultsTopic;

    public CaseProcessor(ProcessingProfile profile, KafkaTemplate<String, String> kafka, ObjectMapper json,
            @Value("${caseroute.kafka.saved-topic}") String savedTopic,
            @Value("${caseroute.kafka.processed-topic}") String resultsTopic) {
        this.profile = profile;
        this.kafka = kafka;
        this.json = json;
        this.deadLetterTopic = savedTopic + ".DLT";
        this.resultsTopic = resultsTopic;
    }

    public void process(ConsumerRecord<String, String> record) throws InterruptedException {
        CaseSavedMessage message;
        try {
            message = json.readValue(record.value(), CaseSavedMessage.class);
        } catch (RuntimeException ex) {
            publish(new ProducerRecord<>(deadLetterTopic, record.key(), record.value()), "Unreadable cases.saved message");
            return;
        }
        ProcessingPlan plan = ProcessingPlan.forCase(message.clientRequestId(), profile);
        for (int attempt = 1; attempt <= plan.attempts(); attempt++) {
            Thread.sleep(plan.durationMs()); // the simulated work
            if (attempt < plan.attempts()) Thread.sleep(profile.retryBackoffMs()); // transient error: back off, retry
        }
        if (!plan.routed()) {
            publish(new ProducerRecord<>(deadLetterTopic, record.key(), record.value()), plan.failureReason());
        }
        var result = new CaseProcessedMessage(message.caseId(), message.clientRequestId(), message.testRunId(),
                plan.outcome(), plan.category(), plan.team(), plan.attempts(), plan.failureReason(), Instant.now());
        // Keyed by case ID, like cases.saved, so all events for one case stay on one partition, in order.
        publish(new ProducerRecord<>(resultsTopic, message.caseId().toString(), json.writeValueAsString(result)), null);
    }

    /**
     * Sends and waits for Kafka's confirmation. Kafka being briefly unavailable must not lose a result, so this
     * retries every second until confirmed (warning once).
     */
    private void publish(ProducerRecord<String, String> record, String failureReason) throws InterruptedException {
        if (failureReason != null) {
            record.headers().add("failure-reason", failureReason.getBytes(StandardCharsets.UTF_8));
        }
        boolean warned = false;
        while (true) {
            try {
                kafka.send(record).get(10, TimeUnit.SECONDS);
                return;
            } catch (InterruptedException ex) {
                throw ex;
            } catch (Exception ex) {
                if (!warned) log.warn("Could not publish to {}; retrying every second: {}", record.topic(), ex.toString());
                warned = true;
                Thread.sleep(1000);
            }
        }
    }
}
