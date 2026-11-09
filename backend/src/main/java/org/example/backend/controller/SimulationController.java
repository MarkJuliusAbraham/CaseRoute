package org.example.backend.controller;

import org.example.backend.service.SimulationService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/simulation")
@ConditionalOnProperty(name = "simulation.reset-enabled", havingValue = "true")
public class SimulationController {
    private final SimulationService simulation;

    public SimulationController(SimulationService simulation) { this.simulation = simulation; }

    @DeleteMapping("/cases")
    public ResponseEntity<Void> clearCases(@RequestHeader("X-Confirm-Reset") String confirmation) {
        if (!"DELETE_ALL_CASES".equals(confirmation))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Explicit reset confirmation is required");
        simulation.clearCases();
        return ResponseEntity.noContent().build();
    }
}
