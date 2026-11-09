package org.example.backend.service;

import org.example.backend.exception.SubmissionUnavailableException;
import org.example.backend.messaging.CaseSubmittedMessage;
import org.example.backend.model.dto.cases.CaseResponse;
import org.example.backend.model.dto.cases.CreateCaseRequest;
import org.example.backend.model.dto.cases.SubmissionAcceptedResponse;
import org.example.backend.model.entity.ServiceCase;
import org.example.backend.repository.CaseRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Accepts submissions on one of two write paths. The Kafka path writes to Kafka and waits for the broker's
 * confirmation; {@link org.example.backend.messaging.CaseSubmissionConsumer} saves the case later.
 * The direct path saves to Postgres before replying.
 */
@Service
public class CaseService {

    private final CaseRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final String topic;
    private final Duration publishTimeout;

    public CaseService(CaseRepository repository, KafkaTemplate<String, String> kafka, ObjectMapper json,
            @Value("${caseroute.kafka.submissions-topic}") String topic,
            @Value("${caseroute.kafka.publish-timeout}") Duration publishTimeout) {
        this.repository = repository;
        this.kafka = kafka;
        this.json = json;
        this.topic = topic;
        this.publishTimeout = publishTimeout;
    }

    /** Direct path: saves the case inside the request. The reply waits for the database commit. */
    @Transactional
    public CaseResponse createCase(CreateCaseRequest request) {
        ServiceCase saved = repository.save(new ServiceCase(request.clientRequestId(), request.testRunId(),
                request.subject(), request.description()));
        return CaseResponse.from(saved);
    }

    /** Kafka path: the reply waits only for Kafka's confirmation; the case is saved later. */
    public SubmissionAcceptedResponse submitCase(CreateCaseRequest request) {
        var message = new CaseSubmittedMessage(request.clientRequestId(), request.testRunId(),
                request.subject(), request.description(), Instant.now());
        try {
            // Keyed by clientRequestId so a retried submission lands on the same partition as the original.
            kafka.send(topic, request.clientRequestId().toString(), json.writeValueAsString(message))
                    .get(publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new SubmissionUnavailableException(ex);
        } catch (ExecutionException | TimeoutException | RuntimeException ex) {
            throw new SubmissionUnavailableException(ex);
        }
        return new SubmissionAcceptedResponse(message.clientRequestId(), message.testRunId(),
                SubmissionAcceptedResponse.ACCEPTED, message.acceptedAt());
    }
}
