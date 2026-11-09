package org.example.backend.repository;

import org.example.backend.model.entity.ServiceCase;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface CaseRepository extends JpaRepository<ServiceCase, UUID> {
    Page<ServiceCase> findByTestRunId(UUID testRunId, Pageable pageable);

    /** Uses the unique index on client_request_id. */
    Optional<ServiceCase> findByClientRequestId(UUID clientRequestId);
}
