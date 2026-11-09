package org.example.backend.messaging;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.backend.repository.CaseResultWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Reads processing results from cases.processed in batches and records them on the cases table, which only the
 * backend writes. Same guarantees as {@link CaseSubmissionConsumer}: offsets are committed after the database
 * commit, a failed batch is retried, and unusable messages go to the dead-letter topic.
 */
@Slf4j
@Component
public class CaseProcessedConsumer {

    private static final Set<String> OUTCOMES = Set.of("ROUTED", "FAILED");

    private final CaseResultWriter writer;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final String deadLetterTopic;

    public CaseProcessedConsumer(CaseResultWriter writer, KafkaTemplate<String, String> kafka, ObjectMapper json,
            @Value("${caseroute.kafka.processed-topic}") String topic) {
        this.writer = writer;
        this.kafka = kafka;
        this.json = json;
        this.deadLetterTopic = topic + ".DLT";
    }

    @KafkaListener(topics = "${caseroute.kafka.processed-topic}", groupId = "case-result-writer")
    public void consume(List<ConsumerRecord<String, String>> records) throws ExecutionException, InterruptedException {
        List<CaseProcessedMessage> valid = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> record : records) {
            CaseProcessedMessage result = parse(record);
            if (result != null) valid.add(result);
            else {
                log.error("Unusable result at {}-{}@{}; sending to {}", record.topic(), record.partition(),
                        record.offset(), deadLetterTopic);
                kafka.send(deadLetterTopic, record.key(), record.value()).get();
            }
        }
        writer.saveAll(valid);
    }

    private CaseProcessedMessage parse(ConsumerRecord<String, String> record) {
        try {
            CaseProcessedMessage result = json.readValue(record.value(), CaseProcessedMessage.class);
            boolean complete = result.caseId() != null && result.processedAt() != null
                    && OUTCOMES.contains(result.outcome());
            return complete ? result : null;
        } catch (RuntimeException ex) {
            return null;
        }
    }
}
