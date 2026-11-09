package org.example.backend.repository;

import org.example.backend.model.entity.ServiceCase;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface CaseRepository extends JpaRepository<ServiceCase, UUID> {
    org.springframework.data.domain.Page<ServiceCase> findByTestRunId(
            UUID testRunId, org.springframework.data.domain.Pageable pageable);
}
