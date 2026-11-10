package org.example.generator.service;

import jakarta.annotation.PreDestroy;
import org.example.generator.client.CaseClient;
import org.example.generator.client.RetryPolicy;
import org.example.generator.model.RunSettings;
import org.example.generator.report.RunReport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

@Service
public class RunService {
    private final CaseClient client;
    private final RetryPolicy retry;
    private final ObjectMapper json;
    private final Path directory;
    private LoadRun current;
    private Thread runner;

    public RunService(CaseClient client, RetryPolicy retry, ObjectMapper json,
            @Value("${generator.report-directory}") Path directory) {
        this.client = client;
        this.retry = retry;
        this.json = json;
        this.directory = directory;
    }

    public synchronized LoadRun start(RunSettings settings) throws IOException {
        if (current != null && (current.active() || current.resending()))
            throw new ResponseStatusException(HttpStatus.CONFLICT, "A run is already active");
        UUID id = UUID.randomUUID();
        // Nothing is planned up front: submissions are built from their sequence number as they are sent.
        var report = new RunReport(directory, id, settings, json);
        current = new LoadRun(id, settings, client, retry, report);
        runner = Thread.ofVirtual().name("load-run-" + id).start(current);
        return current;
    }

    public synchronized LoadRun find(UUID id) {
        if (current == null || !current.id().equals(id))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Run is not loaded; reports remain on disk");
        return current;
    }

    public synchronized LoadRun current() { return current; }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        Thread running;
        synchronized (this) {
            if (current != null) current.stop();
            running = runner;
        }
        if (running != null) running.join(20_000);
    }
}
