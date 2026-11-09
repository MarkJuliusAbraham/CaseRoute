package org.example.generator.service;

import jakarta.annotation.PreDestroy;
import org.example.generator.client.CaseClient;
import org.example.generator.model.RunSettings;
import org.example.generator.model.Submission;
import org.example.generator.report.RunReport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.UUID;

@Service
public class RunService {
    private final CaseClient client;
    private final ObjectMapper json;
    private final Path directory;
    private LoadRun current;
    private Thread runner;

    public RunService(CaseClient client, ObjectMapper json,
            @Value("${generator.report-directory}") Path directory) {
        this.client = client;
        this.json = json;
        this.directory = directory;
    }

    public synchronized LoadRun start(RunSettings settings) throws IOException {
        if (current != null && current.active())
            throw new ResponseStatusException(HttpStatus.CONFLICT, "A run is already active");
        if (settings.planned() > 1_000_000)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Limit each run to 1,000,000 submissions");
        UUID id = UUID.randomUUID();
        var submissions = new ArrayList<Submission>();
        for (long i = 1; i <= settings.planned(); i++) submissions.add(Submission.forRun(id, i));
        var report = new RunReport(directory, id, settings, submissions, json);
        current = new LoadRun(id, settings, submissions, client, report);
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
