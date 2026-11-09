package org.example.generator;

import com.sun.net.httpserver.HttpServer;
import org.example.generator.model.RunSettings;
import org.example.generator.service.LoadRun;
import org.example.generator.service.RunService;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GeneratorIntegrationTests {
    @TempDir static Path reports;
    private static final ObjectMapper JSON = new JsonMapper();
    private static final AtomicInteger delay = new AtomicInteger();
    private static final AtomicInteger responseStatus = new AtomicInteger(-1);
    private static final HttpServer TARGET = startTarget();
    @LocalServerPort int port;
    @Autowired RunService runs;
    private final HttpClient http = HttpClient.newHttpClient();

    private static HttpServer startTarget() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/api/cases", exchange -> {
                try {
                    Thread.sleep(delay.get());
                    var input = JSON.readTree(exchange.getRequestBody().readAllBytes());
                    var output = new LinkedHashMap<String, Object>();
                    boolean direct = exchange.getRequestURI().getPath().endsWith("/direct");
                    output.put("clientRequestId", input.path("clientRequestId").asText());
                    output.put("testRunId", input.path("testRunId").asText());
                    if (direct) {
                        output.put("id", UUID.randomUUID().toString());
                        output.put("subject", input.path("subject").asText());
                        output.put("description", input.path("description").asText());
                        output.put("status", "OPEN");
                    } else {
                        output.put("status", "ACCEPTED");
                        output.put("acceptedAt", java.time.Instant.now().toString());
                    }
                    int code = responseStatus.get() > 0 ? responseStatus.get() : direct ? 201 : 202;
                    byte[] body = JSON.writeValueAsBytes(output);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(code, body.length);
                    exchange.getResponseBody().write(body);
                } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
                finally { exchange.close(); }
            });
            server.start();
            return server;
        } catch (Exception ex) { throw new RuntimeException(ex); }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("generator.target-url", () -> "http://127.0.0.1:" + TARGET.getAddress().getPort() + "/api/cases");
        registry.add("generator.report-directory", () -> reports.toString());
    }

    @AfterAll static void closeTarget() { TARGET.stop(0); }

    @AfterEach void drain() throws Exception {
        LoadRun run = runs.current();
        if (run != null) { run.stop(); await(() -> !run.active()); }
        delay.set(0); responseStatus.set(-1);
    }

    @Test void publicControlsStartStreamStopAndDownloadManifest() throws Exception {
        var response = post("/api/runs", "{\"requestsPerMinute\":600,\"durationMinutes\":1,\"maxInFlight\":2}");
        assertEquals(201, response.statusCode());
        String id = JSON.readTree(response.body()).path("runId").asText();
        assertEquals(409, post("/api/runs", "{\"requestsPerMinute\":50,\"durationMinutes\":1,\"maxInFlight\":2}").statusCode());
        LoadRun run = runs.find(UUID.fromString(id));
        await(() -> run.snapshot().accepted() >= 2);
        var events = http.send(HttpRequest.newBuilder(url("/api/runs/" + id + "/events")).GET().build(),
                HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, events.statusCode());
        try (var reader = new java.io.BufferedReader(new java.io.InputStreamReader(events.body()))) {
            assertTrue(reader.readLine().startsWith("event:progress"));
        }
        assertEquals(200, post("/api/runs/" + id + "/stop", "").statusCode());
        await(() -> !run.active());
        var snapshot = run.snapshot();
        assertEquals("STOPPED", snapshot.state());
        assertTrue(snapshot.accepted() >= 2);
        assertEquals(0, snapshot.failed()); assertEquals(0, snapshot.unknown());
        assertEquals(snapshot.planned(), snapshot.accepted() + snapshot.failed() + snapshot.unknown()
                + snapshot.skipped() + snapshot.notSent());
        var report = http.send(HttpRequest.newBuilder(url("/api/runs/" + id + "/report")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, report.statusCode());
        var records = report.body().lines().map(JSON::readTree).toList();
        assertEquals(600, records.stream().filter(n -> n.path("type").asText().equals("planned")).count());
        // Only non-accepted outcomes get a result line; accepted ones are counted in per-second lines.
        assertEquals(snapshot.failed() + snapshot.unknown() + snapshot.skipped(),
                records.stream().filter(n -> n.path("type").asText().equals("result")).count());
        assertEquals(snapshot.accepted(), records.stream().filter(n -> n.path("type").asText().equals("second"))
                .mapToLong(n -> n.path("accepted").asLong()).sum());
        assertEquals(snapshot.notSent(), records.stream().filter(n -> n.path("type").asText().equals("notSent")).count());
        assertTrue(records.stream().anyMatch(n -> n.path("type").asText().equals("summary")));
    }

    @Test void slowTargetSkipsAtCapacityAndDrainsWithoutLosingAccounting() throws Exception {
        delay.set(200);
        LoadRun run = runs.start(new RunSettings(6000, 1, 1));
        await(() -> run.snapshot().skipped() >= 5);
        assertTrue(run.snapshot().inFlight() <= 1);
        run.stop(); await(() -> !run.active());
        var s = run.snapshot();
        assertEquals(s.dispatched(), s.accepted());
        assertTrue(s.skipped() >= 5);
        assertEquals(s.planned(), s.accepted() + s.skipped() + s.notSent());
    }

    @Test void serverErrorsAreUnknownRatherThanAssumedNotPersisted() throws Exception {
        responseStatus.set(503);
        LoadRun run = runs.start(new RunSettings(60, 1, 1));
        await(() -> run.snapshot().unknown() == 1);
        run.stop(); await(() -> !run.active());
        assertEquals(0, run.snapshot().accepted());
    }

    @Test void directModeExpectsCreatedCasesAndRecordsTheirIds() throws Exception {
        LoadRun run = runs.start(new RunSettings(60, 1, 1, org.example.generator.model.SubmissionMode.DIRECT));
        await(() -> run.snapshot().accepted() == 1);
        run.stop(); await(() -> !run.active());
        var result = run.snapshot().recentResults().getFirst();
        assertEquals(201, result.httpStatus());
        assertNotNull(result.caseId());
        assertEquals(org.example.generator.model.SubmissionMode.DIRECT, run.snapshot().settings().submissionMode());
    }

    @Test void runsDefaultToKafkaModeWhenNoneIsGiven() throws Exception {
        var response = post("/api/runs", "{\"requestsPerMinute\":60,\"durationMinutes\":1,\"maxInFlight\":1}");
        assertEquals(201, response.statusCode());
        assertEquals("KAFKA", JSON.readTree(response.body()).path("settings").path("submissionMode").asText());
        LoadRun run = runs.find(UUID.fromString(JSON.readTree(response.body()).path("runId").asText()));
        await(() -> run.snapshot().accepted() == 1);
        assertEquals(202, run.snapshot().recentResults().getFirst().httpStatus());
    }

    @Test void reportListsOnlyProblemsIndividuallyAndAggregatesEverySecond() throws Exception {
        responseStatus.set(503);
        LoadRun failing = runs.start(new RunSettings(120, 1, 1));
        await(() -> failing.snapshot().unknown() >= 2);
        failing.stop(); await(() -> !failing.active());
        responseStatus.set(-1);
        LoadRun clean = runs.start(new RunSettings(120, 1, 1));
        await(() -> clean.snapshot().accepted() >= 2);
        clean.stop(); await(() -> !clean.active());

        var failingRecords = Files.readAllLines(failing.report().path()).stream().map(JSON::readTree).toList();
        var unknownLines = failingRecords.stream().filter(n -> n.path("type").asText().equals("result")).toList();
        assertEquals(failing.snapshot().unknown(), unknownLines.size());
        assertTrue(unknownLines.stream().allMatch(n -> n.path("result").path("outcome").asText().equals("UNKNOWN")));

        var cleanRecords = Files.readAllLines(clean.report().path()).stream().map(JSON::readTree).toList();
        assertEquals(0, cleanRecords.stream().filter(n -> n.path("type").asText().equals("result")).count());
        var secondsLines = cleanRecords.stream().filter(n -> n.path("type").asText().equals("second")).toList();
        assertEquals(clean.snapshot().accepted(), secondsLines.stream().mapToLong(n -> n.path("accepted").asLong()).sum());
        assertTrue(secondsLines.getFirst().path("latencyMs").has("p99"));
        assertEquals(clean.snapshot().notSent(), cleanRecords.stream().filter(n -> n.path("type").asText().equals("notSent")).count());
        assertEquals("summary", cleanRecords.getLast().path("type").asText());
    }

    @Test void invalidRunSettingsAreRejectedAndUiIsNotServed() throws Exception {
        assertEquals(400, post("/api/runs", "{\"requestsPerMinute\":0,\"durationMinutes\":1,\"maxInFlight\":2}").statusCode());
        assertEquals(400, post("/api/runs", "{\"requestsPerMinute\":60000,\"durationMinutes\":60,\"maxInFlight\":2}").statusCode());
        var page = http.send(HttpRequest.newBuilder(url("/")).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(404, page.statusCode());
    }

    @Test void completedRunAccountsForEveryPlannedSubmission() throws Exception {
        LoadRun run = runs.start(new RunSettings(1, 1, 1));
        await(() -> !run.active());
        var s = run.snapshot();
        assertEquals("COMPLETED", s.state());
        assertEquals(1, s.planned()); assertEquals(1, s.accepted());
        assertEquals(0, s.notSent()); assertEquals(0, s.inFlight());
        assertTrue(Files.readString(run.report().path()).contains("summary"));
    }

    @Test void rateValidationAcceptsOneMillionAndRejectsAnythingHigher() {
        try (var factory = jakarta.validation.Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            var maximum = new RunSettings(1_000_000, 1, 500);
            assertTrue(validator.validate(maximum).isEmpty());
            assertEquals(1_000_000, maximum.planned());
            assertFalse(validator.validate(new RunSettings(1_000_001, 1, 500)).isEmpty());
        }
    }

    @Test void healthEndpointRespondsWithoutStartingARun() throws Exception {
        var response = http.send(HttpRequest.newBuilder(url("/api/runs/health")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("UP", JSON.readTree(response.body()).path("status").asText());
    }

    @Test void unlimitedConcurrencyTracksResponsesWithoutCapacitySkips() throws Exception {
        delay.set(200);
        LoadRun run = runs.start(new RunSettings(6000, 1, null));
        await(() -> run.snapshot().inFlight() >= 5);
        run.stop(); await(() -> !run.active());
        var s = run.snapshot();
        assertNull(s.settings().maxInFlight());
        assertEquals(0, s.skipped());
        assertTrue(s.accepted() >= 5);
        assertEquals(s.dispatched(), s.accepted());
        assertEquals(s.planned(), s.accepted() + s.notSent());
    }

    private URI url(String path) { return URI.create("http://localhost:" + port + path); }
    private HttpResponse<String> post(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(url(path)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
        assertTrue(condition.getAsBoolean(), "Timed out waiting for run progress");
    }
}
