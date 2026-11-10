package org.example.backend.controller;

import org.example.backend.config.LoadShedding;
import org.example.backend.service.PipelineControl;
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
    private final PipelineControl pipeline;
    private final LoadShedding shedding;

    public SimulationController(SimulationService simulation, PipelineControl pipeline, LoadShedding shedding) {
        this.simulation = simulation;
        this.pipeline = pipeline;
        this.shedding = shedding;
    }

    public record SheddingSettings(boolean enabled, Integer maxConcurrent) { }

    /** Load shedding state: on/off, the concurrency limit, submissions in progress and how many were rejected. */
    @GetMapping("/shedding")
    public LoadShedding.State shedding() { return shedding.state(); }

    /** Switches load shedding and optionally changes the limit. Applies immediately. */
    @PutMapping("/shedding")
    public LoadShedding.State setShedding(@RequestBody SheddingSettings settings) {
        try {
            return shedding.configure(settings.enabled(), settings.maxConcurrent());
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
        }
    }

    public record PipelineState(boolean paused) { }

    /** Whether the backend's downstream work (case writer, outbox relay, result writer) is paused. */
    @GetMapping("/pipeline")
    public PipelineState pipeline() { return new PipelineState(pipeline.isPaused()); }

    /** Pauses or resumes the downstream work. Intake keeps running; nothing is lost while paused. */
    @PutMapping("/pipeline")
    public PipelineState setPipeline(@RequestBody PipelineState state) {
        pipeline.setPaused(state.paused());
        return new PipelineState(pipeline.isPaused());
    }

    @DeleteMapping("/cases")
    public ResponseEntity<Void> clearCases(@RequestHeader("X-Confirm-Reset") String confirmation) {
        if (!"DELETE_ALL_CASES".equals(confirmation))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Explicit reset confirmation is required");
        simulation.clearCases();
        return ResponseEntity.noContent().build();
    }
}
