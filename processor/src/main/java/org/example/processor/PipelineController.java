package org.example.processor;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Pause and resume processing, for experiments that isolate intake. The processor listens on 127.0.0.1 only
 * ({@code server.address}), so only local callers such as the dashboard's proxy can reach this.
 */
@RestController
@RequestMapping("/api/pipeline")
public class PipelineController {
    private final CaseConsumerLoop loop;

    public PipelineController(CaseConsumerLoop loop) { this.loop = loop; }

    public record PipelineState(boolean paused) { }

    @GetMapping
    public PipelineState state() { return new PipelineState(loop.isPaused()); }

    @PutMapping
    public PipelineState set(@RequestBody PipelineState state) {
        loop.setPaused(state.paused());
        return new PipelineState(loop.isPaused());
    }
}
