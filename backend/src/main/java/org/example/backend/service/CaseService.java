package org.example.backend.service;

import org.example.backend.exception.SubmissionUnavailableException;
import org.example.backend.messaging.CaseSubmittedMessage;
import org.example.backend.model.dto.cases.CaseResponse;
import org.example.backend.model.dto.cases.CaseStatusResponse;
import org.example.backend.model.dto.cases.CreateCaseRequest;
import org.example.backend.model.dto.cases.SubmissionAcceptedResponse;
import org.example.backend.model.entity.OutboxEvent;
import org.example.backend.model.entity.ServiceCase;
import org.example.backend.repository.CaseRepository;
import org.example.backend.repository.OutboxRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Accepts submissions on one of two write paths. The Kafka path writes to Kafka through the dedicated intake
 * producer (see {@code KafkaConfig#intakeKafkaTemplate}) and waits for the broker's confirmation; {@link org.example.backend.messaging.CaseSubmissionConsumer} saves the case later.
 * The direct path saves to Postgres before replying.
 */
@Service
public class CaseService {

    private final CaseRepository repository;
    private final OutboxRepository outbox;
    private final org.example.backend.messaging.IntakeProducer kafka;
    private final ObjectMapper json;
    private final String topic;
    private final String warmupTopic;
    private final Duration publishTimeout;

    public CaseService(CaseRepository repository, OutboxRepository outbox,
            org.example.backend.messaging.IntakeProducer kafka, ObjectMapper json,
            @Value("${caseroute.kafka.submissions-topic}") String topic,
            @Value("${caseroute.kafka.warmup-topic}") String warmupTopic,
            @Value("${caseroute.kafka.publish-timeout}") Duration publishTimeout) {
        this.repository = repository;
        this.outbox = outbox;
        this.kafka = kafka;
        this.json = json;
        this.topic = topic;
        this.warmupTopic = warmupTopic;
        this.publishTimeout = publishTimeout;
    }

    /**
     * Direct path: saves the case inside the request. The reply waits for the database commit.
     * The outbox row is part of the same transaction, so a saved case is always handed to processing.
     */
    @Transactional
    public CaseResponse createCase(CreateCaseRequest request) {
        ServiceCase saved = repository.saveAndFlush(new ServiceCase(request.clientRequestId(), request.testRunId(),
                request.subject(), request.description()));
        outbox.save(OutboxEvent.caseSaved(saved));
        return CaseResponse.from(saved);
    }

    /**
     * For a retried direct submission: the case already saved under this clientRequestId, if it has the same
     * content. A different subject or description under the same ID is a client error, not a retry.
     */
    @Transactional(readOnly = true)
    public Optional<CaseResponse> findRetriedCase(CreateCaseRequest request) {
        return repository.findByClientRequestId(request.clientRequestId())
                .filter(existing -> existing.getSubject().equals(request.subject())
                        && existing.getDescription().equals(request.description()))
                .map(CaseResponse::from);
    }

    /** The status lookup behind {@code GET /api/cases/requests/{clientRequestId}}. */
    @Transactional(readOnly = true)
    public Optional<CaseStatusResponse> findStatus(UUID clientRequestId) {
        return repository.findByClientRequestId(clientRequestId).map(CaseStatusResponse::from);
    }

    /** Kafka path: the reply waits only for Kafka's confirmation; the case is saved later. */
    public SubmissionAcceptedResponse submitCase(CreateCaseRequest request) {
        CaseSubmittedMessage message = publish(topic, request);
        return new SubmissionAcceptedResponse(message.clientRequestId(), message.testRunId(),
                SubmissionAcceptedResponse.ACCEPTED, message.acceptedAt());
    }

    /**
     * Load-test warm-up: the same Kafka write as {@link #submitCase}, waiting for the same confirmation, but to
     * a topic nobody consumes. Warms the producer (connections, batching, metadata) and the broker path without
     * creating a case.
     */
    public void warmUp(CreateCaseRequest request) {
        publish(warmupTopic, request);
    }

    private CaseSubmittedMessage publish(String target, CreateCaseRequest request) {
        var message = new CaseSubmittedMessage(request.clientRequestId(), request.testRunId(),
                request.subject(), request.description(), Instant.now());
        try {
            // Keyed by clientRequestId so a retried submission lands on the same partition as the original.
            kafka.sendAndWait(target, request.clientRequestId().toString(), json.writeValueAsString(message),
                    publishTimeout.toMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new SubmissionUnavailableException(ex);
        } catch (ExecutionException | TimeoutException | RuntimeException ex) {
            throw new SubmissionUnavailableException(ex);
        }
        return message;
    }
}
