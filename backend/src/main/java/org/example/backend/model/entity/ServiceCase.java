package org.example.backend.model.entity;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.example.backend.model.enums.CaseStatus;

import java.time.Instant;
import java.util.UUID;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Entity
@Table(name = "cases", indexes = @Index(name = "idx_cases_test_run_id", columnList = "test_run_id"))
public class ServiceCase {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    // Nullable for cases created before client request IDs were introduced.
    @Column(name = "client_request_id", unique = true, updatable = false)
    private UUID clientRequestId;

    @Column(name = "test_run_id", updatable = false)
    private UUID testRunId;

    @Column(nullable = false, length = 200)
    private String subject;

    @Column(nullable = false, columnDefinition = "text")
    private String description;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private CaseStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    // Results from the processor application, recorded by CaseProcessedConsumer (cases.processed).
    @Column(length = 30)
    private String category;

    @Column(name = "assigned_team", length = 50)
    private String assignedTeam;

    @Column(name = "processing_attempts")
    private Integer processingAttempts;

    @Column(name = "processed_at")
    private Instant processedAt;

    /**
     * Processing results received for this case, including any the status update ignored. 1 for a case processed
     * once; more means it was processed again (a duplicate the pipeline should have absorbed, or a Kafka redelivery).
     * Null for cases saved before this column existed, and for cases not processed yet.
     */
    @Column(name = "results_received")
    private Integer resultsReceived;

    public ServiceCase(UUID clientRequestId, UUID testRunId, String subject, String description) {
        this.clientRequestId = java.util.Objects.requireNonNull(clientRequestId);
        this.testRunId = testRunId;
        this.subject = subject;
        this.description = description;
        this.status = CaseStatus.OPEN;
        this.createdAt = Instant.now();
    }
}
