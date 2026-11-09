package org.example.backend.messaging;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.backend.repository.CaseBatchWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Reads accepted submissions from Kafka in batches and saves them to Postgres.
 *
 * <p>Offsets are committed only after this method returns, i.e. after the database commit. If saving fails,
 * the error handler retries the whole batch (see {@code KafkaConfig}), so an accepted case is never skipped.
 * Messages that can never be saved (unreadable or invalid) go to the dead-letter topic instead of blocking.
 */
@Slf4j
@Component
public class CaseSubmissionConsumer {

    private final CaseBatchWriter writer;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final String deadLetterTopic;

    public CaseSubmissionConsumer(CaseBatchWriter writer, KafkaTemplate<String, String> kafka, ObjectMapper json,
            @Value("${caseroute.kafka.submissions-topic}") String topic) {
        this.writer = writer;
        this.kafka = kafka;
        this.json = json;
        this.deadLetterTopic = topic + ".DLT";
    }

    @KafkaListener(topics = "${caseroute.kafka.submissions-topic}")
    public void consume(List<ConsumerRecord<String, String>> records) throws ExecutionException, InterruptedException {
        List<CaseSubmittedMessage> valid = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> record : records) {
            CaseSubmittedMessage message = parse(record);
            if (message != null) valid.add(message);
            else deadLetter(record);
        }
        writer.saveAll(valid);
    }

    private CaseSubmittedMessage parse(ConsumerRecord<String, String> record) {
        try {
            CaseSubmittedMessage message = json.readValue(record.value(), CaseSubmittedMessage.class);
            boolean complete = message.clientRequestId() != null && message.acceptedAt() != null
                    && message.subject() != null && !message.subject().isBlank() && message.subject().length() <= 200
                    && message.description() != null && !message.description().isBlank();
            return complete ? message : null;
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private void deadLetter(ConsumerRecord<String, String> record) throws ExecutionException, InterruptedException {
        log.error("Unsaveable submission at {}-{}@{}; sending to {}",
                record.topic(), record.partition(), record.offset(), deadLetterTopic);
        // Wait for the confirmation so the message is safe before its offset is committed.
        kafka.send(deadLetterTopic, record.key(), record.value()).get();
    }
}
