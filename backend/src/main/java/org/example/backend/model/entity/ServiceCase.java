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
@Table(name = "cases")
public class ServiceCase {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false, length = 200)
    private String subject;

    @Column(nullable = false, columnDefinition = "text")
    private String description;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private CaseStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    public ServiceCase(String subject, String description) {
        this.subject = subject;
        this.description = description;
        this.status = CaseStatus.OPEN;
        this.createdAt = Instant.now();
    }
}
