package org.example.generator.controller;

import jakarta.validation.Valid;
import org.example.generator.model.RunSettings;
import org.example.generator.model.RunSnapshot;
import org.example.generator.service.LoadRun;
import org.example.generator.service.RunService;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ContentDisposition;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@RequestMapping("/api/runs")
public class RunController {
    private final RunService runs;

    public RunController(RunService runs) { this.runs = runs; }

    @PostMapping
    public ResponseEntity<RunSnapshot> start(@Valid @RequestBody RunSettings settings) throws IOException {
        LoadRun run = runs.start(settings);
        return ResponseEntity.created(URI.create("/api/runs/" + run.id())).body(run.snapshot());
    }

    @GetMapping("/health")
    public java.util.Map<String, String> health() {
        return java.util.Map.of("status", "UP");
    }

    @GetMapping("/current")
    public ResponseEntity<RunSnapshot> current() {
        LoadRun run = runs.current();
        return run == null ? ResponseEntity.noContent().build() : ResponseEntity.ok(run.snapshot());
    }

    @GetMapping("/{id}")
    public RunSnapshot status(@PathVariable UUID id) { return runs.find(id).snapshot(); }

    @PostMapping("/{id}/stop")
    public RunSnapshot stop(@PathVariable UUID id) {
        LoadRun run = runs.find(id);
        run.stop();
        return run.snapshot();
    }

    @GetMapping(value = "/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter events(@PathVariable UUID id) {
        LoadRun run = runs.find(id);
        SseEmitter emitter = new SseEmitter(0L);
        AtomicBoolean closed = new AtomicBoolean();
        emitter.onCompletion(() -> closed.set(true));
        emitter.onError(ex -> closed.set(true));
        emitter.onTimeout(() -> closed.set(true));
        Thread.ofVirtual().start(() -> {
            try {
                while (!closed.get()) {
                    RunSnapshot snapshot = run.snapshot();
                    emitter.send(SseEmitter.event().name("progress").data(snapshot));
                    if (snapshot.finishedAt() != null) break;
                    Thread.sleep(500);
                }
                emitter.complete();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                emitter.complete();
            } catch (Exception ex) { emitter.completeWithError(ex); }
        });
        return emitter;
    }

    @GetMapping(value = "/{id}/report", produces = "application/x-ndjson")
    public ResponseEntity<Resource> report(@PathVariable UUID id) {
        LoadRun run = runs.find(id);
        return ResponseEntity.ok().header("Content-Disposition", ContentDisposition.attachment().filename(id + ".jsonl").build().toString())
                .body(new FileSystemResource(run.report().path()));
    }
}
