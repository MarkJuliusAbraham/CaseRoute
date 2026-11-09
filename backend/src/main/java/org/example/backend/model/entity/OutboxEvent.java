package org.example.backend.model.entity;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * A "needs announcing" note, written in the same transaction as the case it describes.
 * The relay publishes each row to Kafka and then deletes it, so the table only holds what is not yet sent.
 */
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Entity
@Table(name = "outbox")
public class OutboxEvent {

    public static final String CASE_SAVED = "case.saved";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_type", nullable = false, length = 50)
    private String eventType;

    @Column(name = "case_id", nullable = false)
    private UUID caseId;

    @Column(name = "client_request_id", nullable = false)
    private UUID clientRequestId;

    @Column(name = "test_run_id")
    private UUID testRunId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    public static OutboxEvent caseSaved(ServiceCase serviceCase) {
        var event = new OutboxEvent();
        event.eventType = CASE_SAVED;
        event.caseId = serviceCase.getId();
        event.clientRequestId = serviceCase.getClientRequestId();
        event.testRunId = serviceCase.getTestRunId();
        event.createdAt = Instant.now();
        return event;
    }
}
