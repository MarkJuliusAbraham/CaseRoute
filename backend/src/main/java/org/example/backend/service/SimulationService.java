package org.example.backend.service;

import org.example.backend.repository.CaseRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SimulationService {
    private final CaseRepository cases;

    public SimulationService(CaseRepository cases) { this.cases = cases; }

    @Transactional
    public void clearCases() {
        cases.deleteAllInBatch();
    }
}
