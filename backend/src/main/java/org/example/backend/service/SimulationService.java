package org.example.backend.service;

import org.example.backend.repository.CaseRepository;
import org.example.backend.repository.OutboxRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SimulationService {
    private final CaseRepository cases;
    private final OutboxRepository outbox;

    public SimulationService(CaseRepository cases, OutboxRepository outbox) { this.cases = cases; this.outbox = outbox; }

    /** Removes saved cases and any not-yet-announced outbox rows for them. Kafka topics are not touched. */
    @Transactional
    public void clearCases() {
        outbox.deleteAllInBatch();
        cases.deleteAllInBatch();
    }
}
