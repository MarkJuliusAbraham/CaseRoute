package org.example.backend.controller;

import org.example.backend.messaging.IntakeProducer;
import org.example.backend.messaging.KafkaTopics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

/**
 * Local experiment controls for Kafka, used between load tests: the intake producer's batching and compression,
 * and the topics' partition counts. Loopback only, like the other simulation controls.
 */
@RestController
@RequestMapping("/api/simulation/kafka")
@ConditionalOnProperty(name = "simulation.reset-enabled", havingValue = "true")
public class KafkaSettingsController {
    private final IntakeProducer producer;
    private final KafkaTopics topics;
    private final int consumersPerTopic;

    public KafkaSettingsController(IntakeProducer producer, KafkaTopics topics,
            @Value("${spring.kafka.listener.concurrency:1}") int consumersPerTopic) {
        this.producer = producer;
        this.topics = topics;
        this.consumersPerTopic = consumersPerTopic;
    }

    /**
     * @param sending           submissions waiting for Kafka's confirmation right now; changes are refused unless 0
     * @param backendConsumers  consumers per topic the backend reads with (cases.submitted, cases.processed): more
     *                          partitions than this add no reading parallelism there
     */
    public record State(IntakeProducer.Settings producer, int sending, Map<String, Integer> partitions,
            int backendConsumers) { }

    public record PartitionChange(String topic, int partitions) { }

    /** Refusals carry their reason as {@code detail}, so the dashboard can say why a change was refused. */
    @ExceptionHandler(ResponseStatusException.class)
    public org.springframework.http.ResponseEntity<org.springframework.http.ProblemDetail> refused(ResponseStatusException ex) {
        return org.springframework.http.ResponseEntity.status(ex.getStatusCode())
                .body(org.springframework.http.ProblemDetail.forStatusAndDetail(ex.getStatusCode(), ex.getReason()));
    }

    @GetMapping
    public State state() {
        Map<String, Integer> partitions;
        try { partitions = topics.partitions(); }
        catch (RuntimeException ex) { partitions = Map.of(); } // Kafka unreachable: the producer settings still show
        return new State(producer.settings(), producer.sending(), partitions, consumersPerTopic);
    }

    @GetMapping("/metrics")
    public Map<String, Double> metrics() { return producer.metrics(); }

    @PutMapping("/producer")
    public State setProducer(@RequestBody IntakeProducer.Settings settings) {
        try {
            producer.apply(settings);
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
        } catch (IllegalStateException ex) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, ex.getMessage());
        }
        return state();
    }

    @PutMapping("/partitions")
    public State addPartitions(@RequestBody PartitionChange change) {
        if (producer.sending() > 0)
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Submissions are being sent; add partitions between runs");
        try {
            topics.increase(change.topic(), change.partitions());
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
        } catch (IllegalStateException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, ex.getMessage());
        }
        return state();
    }
}
