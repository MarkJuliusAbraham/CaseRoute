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
    /** Per clientRequestId: how many attempts reached the target. Every attempt counts as "saved". */
    private static final java.util.Map<String, Integer> received = new java.util.concurrent.ConcurrentHashMap<>();
    /** Attempts per request that get a 503 before the target starts replying normally. */
    private static final AtomicInteger failFirstAttempts = new AtomicInteger();
    /** Request IDs the fake ledger leaves out, as if the case was lost after acceptance. */
    private static final java.util.Set<String> lost = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private static final AtomicInteger warmups = new AtomicInteger();
    /** When set, the target sheds load: 503 + Retry-After, nothing stored. */
    private static final java.util.concurrent.atomic.AtomicBoolean shedding = new java.util.concurrent.atomic.AtomicBoolean();
    private static final java.util.Map<String, Integer> paths = new java.util.concurrent.ConcurrentHashMap<>();
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
                    paths.merge(exchange.getRequestURI().getPath(), 1, Integer::sum);
                    if (exchange.getRequestURI().getPath().endsWith("/warmup")) {
                        exchange.getRequestBody().readAllBytes();
                        warmups.incrementAndGet(); // stores nothing
                        exchange.sendResponseHeaders(204, -1);
                        return;
                    }
                    if (shedding.get()) {
                        exchange.getRequestBody().readAllBytes();
                        exchange.getResponseHeaders().set("Retry-After", "1");
                        exchange.sendResponseHeaders(503, -1);
                        return;
                    }
                    var input = JSON.readTree(exchange.getRequestBody().readAllBytes());
                    int attempt = received.merge(input.path("clientRequestId").asText(), 1, Integer::sum);
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
                    int code = attempt <= failFirstAttempts.get() ? 503
                            : responseStatus.get() > 0 ? responseStatus.get() : direct ? 201 : 202;
                    byte[] body = JSON.writeValueAsBytes(output);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(code, body.length);
                    exchange.getResponseBody().write(body);
                } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
                finally { exchange.close(); }
            });
            server.createContext("/api/simulation/cases/ledger", exchange -> {
                var out = new StringBuilder();
                received.keySet().stream().filter(id -> !lost.contains(id))
                        .forEach(id -> out.append(id).append(",ROUTED").append((char) 10));
                byte[] body = out.toString().getBytes();
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            return server;
        } catch (Exception ex) { throw new RuntimeException(ex); }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("generator.target-url", () -> "http://127.0.0.1:" + TARGET.getAddress().getPort() + "/api/cases");
        registry.add("generator.report-directory", () -> reports.toString());
        registry.add("generator.retry.base-delay", () -> "20ms");
        registry.add("generator.retry.max-delay", () -> "40ms");
    }

    @AfterAll static void closeTarget() { TARGET.stop(0); }

    @AfterEach void drain() throws Exception {
        LoadRun run = runs.current();
        if (run != null) { run.stop(); await(() -> !run.active()); }
        delay.set(0); responseStatus.set(-1); failFirstAttempts.set(0); lost.clear(); received.clear(); warmups.set(0); paths.clear(); shedding.set(false);
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
        var plan = records.stream().filter(n -> n.path("type").asText().equals("plan")).findFirst().orElseThrow();
        assertEquals(600, plan.path("planned").asLong());
        assertEquals(0, records.stream().filter(n -> n.path("type").asText().equals("planned")).count(),
                "no line per planned submission");
        // Only non-accepted outcomes get a result line; accepted ones are counted in per-second lines.
        assertEquals(snapshot.failed() + snapshot.unknown() + snapshot.skipped(),
                records.stream().filter(n -> n.path("type").asText().equals("result")).count());
        assertEquals(snapshot.accepted(), records.stream().filter(n -> n.path("type").asText().equals("second"))
                .mapToLong(n -> n.path("accepted").asLong()).sum());
        assertEquals(snapshot.notSent(), records.stream().filter(n -> n.path("type").asText().equals("notSent"))
                .mapToLong(n -> n.path("count").asLong()).sum());
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
        assertEquals("JETTY", JSON.readTree(response.body()).path("settings").path("httpClient").asText(),
                "Jetty is the default HTTP client");
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
        assertEquals(clean.snapshot().notSent(), cleanRecords.stream().filter(n -> n.path("type").asText().equals("notSent"))
                .mapToLong(n -> n.path("count").asLong()).sum());
        assertEquals("summary", cleanRecords.getLast().path("type").asText());
    }

    @Test void invalidRunSettingsAreRejectedAndUiIsNotServed() throws Exception {
        assertEquals(400, post("/api/runs", "{\"requestsPerMinute\":0,\"durationMinutes\":1,\"maxInFlight\":2}").statusCode());
        assertEquals(400, post("/api/runs", "{\"requestsPerMinute\":60,\"durationMinutes\":61,\"maxInFlight\":2}").statusCode());
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

    @Test void retriesResendUnknownOutcomesWithTheSameIdUntilAccepted() throws Exception {
        failFirstAttempts.set(2); // each submission gets two 503s, then a 202
        LoadRun run = runs.start(new RunSettings(60, 1, 1, org.example.generator.model.SubmissionMode.KAFKA, true));
        await(() -> run.snapshot().accepted() == 1);
        run.stop(); await(() -> !run.active());
        var s = run.snapshot();
        assertEquals(0, s.unknown());
        assertEquals(1, s.retried()); assertEquals(1, s.recovered()); assertEquals(2, s.retryRequests());
        assertEquals(3, s.recentResults().getFirst().attempts());
        assertEquals(1, received.size(), "all three attempts reused one clientRequestId");
    }

    @Test void retriesGiveUpAfterMaxAttemptsAndNeverResendDefiniteRejections() throws Exception {
        failFirstAttempts.set(100);
        LoadRun unknown = runs.start(new RunSettings(60, 1, 1, org.example.generator.model.SubmissionMode.KAFKA, true));
        await(() -> unknown.snapshot().unknown() == 1);
        unknown.stop(); await(() -> !unknown.active());
        assertEquals(5, unknown.snapshot().recentResults().getFirst().attempts(), "generator.retry.max-attempts");
        assertEquals(0, unknown.snapshot().recovered());

        failFirstAttempts.set(0); responseStatus.set(400);
        LoadRun rejected = runs.start(new RunSettings(60, 1, 1, org.example.generator.model.SubmissionMode.KAFKA, true));
        await(() -> rejected.snapshot().failed() == 1);
        rejected.stop(); await(() -> !rejected.active());
        assertEquals(1, rejected.snapshot().recentResults().getFirst().attempts());
        assertEquals(0, rejected.snapshot().retryRequests());
    }

    @Test void checkerReconcilesTheClientsOutcomesWithTheBackendLedger() throws Exception {
        LoadRun run = runs.start(new RunSettings(600, 1, null));
        await(() -> run.snapshot().accepted() >= 3);
        responseStatus.set(503); // the target saves these but the client only sees a 503: unknown outcome
        await(() -> run.snapshot().unknown() >= 1);
        run.stop(); await(() -> !run.active());
        var s = run.snapshot();

        String lostId = s.recentResults().stream().filter(r -> r.outcome().equals("ACCEPTED")).findFirst()
                .orElseThrow().clientRequestId().toString();
        lost.add(lostId);
        var response = post("/api/runs/" + run.id() + "/check", "");
        assertEquals(200, response.statusCode());
        var check = JSON.readTree(response.body());
        assertEquals("DISCREPANCIES", check.path("verdict").asText());
        assertEquals(s.accepted(), check.path("accepted").path("total").asLong());
        assertEquals(1, check.path("accepted").path("missing").asLong());
        assertEquals(s.accepted() - 1, check.path("accepted").path("processed").asLong());
        assertEquals(lostId, check.path("examples").path("acceptedButMissing").get(0).asText());
        assertEquals(s.unknown(), check.path("unknown").path("saved").asLong(), "saved even though the reply was a 503");
        assertEquals(0, check.path("unexpected").asLong());
        assertTrue(Files.exists(reports.resolve(run.id() + ".check.json")));

        lost.clear();
        var again = JSON.readTree(post("/api/runs/" + run.id() + "/check", "").body());
        assertEquals("CONSISTENT", again.path("verdict").asText());
        var latest = http.send(HttpRequest.newBuilder(url("/api/runs/" + run.id() + "/check")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals("CONSISTENT", JSON.readTree(latest.body()).path("verdict").asText());
    }

    @Test void warmUpRampsAgainstTheNoOpEndpointBeforeTheMeasuredRun() throws Exception {
        // 600/min with a 2 s ramp: 600 * 2 / 120 = 10 warm-up requests, then the real submissions.
        var settings = new RunSettings(600, 1, null, org.example.generator.model.SubmissionMode.KAFKA, false, 2);
        LoadRun run = runs.start(settings);
        assertEquals("WARMING_UP", run.snapshot().state());
        await(() -> run.snapshot().accepted() >= 2);
        run.stop(); await(() -> !run.active());
        var s = run.snapshot();
        assertEquals(10, s.warmupPlanned());
        assertEquals(10, s.warmupSent());
        assertEquals(0, s.warmupFailed());
        assertEquals(10, warmups.get());
        assertEquals(s.accepted(), received.size(), "warm-up requests were never stored or counted");
        assertEquals(s.planned(), s.accepted() + s.notSent());
    }

    @Test void unknownOutcomesRecordTheExceptionChainWithMessages() {
        var refused = new java.net.ConnectException("Connection refused");
        var wrapped = new java.net.ConnectException();
        wrapped.initCause(new java.nio.channels.ClosedChannelException());
        assertEquals("ConnectException: Connection refused", org.example.generator.client.CaseClient.describe(refused));
        assertEquals("ConnectException <- ClosedChannelException", org.example.generator.client.CaseClient.describe(wrapped));
    }

    @Test void connectionTestSendsEverythingIncludingWarmUpToTheNoOpEndpointAndCannotBeChecked() throws Exception {
        var settings = new RunSettings(600, 1, null, org.example.generator.model.SubmissionMode.NOOP, false, 1);
        LoadRun run = runs.start(settings);
        await(() -> run.snapshot().accepted() >= 2);
        run.stop(); await(() -> !run.active());
        var s = run.snapshot();
        assertEquals(0, s.unknown()); assertEquals(0, s.failed());
        assertEquals(s.warmupPlanned(), s.warmupSent());
        assertEquals(0, s.warmupFailed(), "noop warm-ups expect 202");
        assertEquals(java.util.Set.of("/api/cases/noop"), paths.keySet(), "nothing else was called: " + paths);
        assertEquals(409, post("/api/runs/" + run.id() + "/check", "").statusCode());
    }

    @Test void shedSubmissionsAreBlockedNotRetriedAndCanBeResentAfterTheRun() throws Exception {
        shedding.set(true);
        // Retries are on, yet blocked submissions must not be retried automatically.
        LoadRun run = runs.start(new RunSettings(600, 1, null, org.example.generator.model.SubmissionMode.KAFKA, true, 0));
        await(() -> run.snapshot().blocked() >= 3);
        run.stop(); await(() -> !run.active());
        var s = run.snapshot();
        long blocked = s.blocked();
        assertEquals(0, s.unknown()); assertEquals(0, s.retryRequests(), "blocked is never retried automatically");
        assertEquals("BLOCKED", s.recentResults().getFirst().outcome());
        assertTrue(received.isEmpty(), "nothing was stored");

        var check = JSON.readTree(post("/api/runs/" + run.id() + "/check", "").body());
        assertEquals(blocked, check.path("blocked").path("total").asLong());
        assertEquals(0, check.path("blocked").path("saved").asLong());

        shedding.set(false);
        assertEquals(200, post("/api/runs/" + run.id() + "/resend-blocked", "").statusCode());
        await(() -> run.snapshot().resend() != null && run.snapshot().resend().finishedAt() != null);
        var after = run.snapshot();
        assertEquals(0, after.blocked());
        assertEquals(s.accepted() + blocked, after.accepted(), "every blocked submission was accepted on resend");
        assertEquals(blocked, after.resend().accepted());
        assertEquals("STOPPED", after.state(), "the run's own state is restored after the pass");
        assertEquals(blocked, received.size(), "each resent once, with its original clientRequestId");
        assertTrue(Files.exists(reports.resolve(run.id() + ".resend-1.json")));
        assertEquals(409, post("/api/runs/" + run.id() + "/resend-blocked", "").statusCode(), "nothing left to resend");
    }

    @Test void jettyClientRunsWarmUpAndSubmissionsWithTheSameOutcomes() throws Exception {
        var settings = new RunSettings(600, 1, null, org.example.generator.model.SubmissionMode.KAFKA, false, 1,
                org.example.generator.model.HttpClientKind.JETTY);
        LoadRun run = runs.start(settings);
        await(() -> run.snapshot().accepted() >= 3);
        run.stop(); await(() -> !run.active());
        var s = run.snapshot();
        assertEquals(org.example.generator.model.HttpClientKind.JETTY, s.settings().httpClient());
        assertEquals(0, s.unknown()); assertEquals(0, s.failed());
        assertEquals(s.warmupPlanned(), s.warmupSent()); assertEquals(0, s.warmupFailed());
        assertEquals(s.accepted(), received.size(), "every accepted submission reached the target once");
        assertEquals(202, s.recentResults().getFirst().httpStatus());
    }

    @Test void jettyClientClassifiesSheddingAsBlockedAndOther503AsUnknown() throws Exception {
        shedding.set(true);
        LoadRun blocked = runs.start(new RunSettings(600, 1, null, org.example.generator.model.SubmissionMode.KAFKA,
                true, 0, org.example.generator.model.HttpClientKind.JETTY));
        await(() -> blocked.snapshot().blocked() >= 2);
        blocked.stop(); await(() -> !blocked.active());
        assertEquals(0, blocked.snapshot().unknown());
        assertEquals(0, blocked.snapshot().retryRequests(), "blocked is never retried");

        shedding.set(false); responseStatus.set(503); // a 503 without Retry-After: outcome unknown
        LoadRun unknown = runs.start(new RunSettings(600, 1, null, org.example.generator.model.SubmissionMode.KAFKA,
                false, 0, org.example.generator.model.HttpClientKind.JETTY));
        await(() -> unknown.snapshot().unknown() >= 2);
        unknown.stop(); await(() -> !unknown.active());
        assertEquals(0, unknown.snapshot().blocked());
    }

    @Test void builtInJavaClientStillWorksWhenChosen() throws Exception {
        LoadRun run = runs.start(new RunSettings(600, 1, null, org.example.generator.model.SubmissionMode.KAFKA, false, 1,
                org.example.generator.model.HttpClientKind.JDK));
        await(() -> run.snapshot().accepted() >= 3);
        run.stop(); await(() -> !run.active());
        var s = run.snapshot();
        assertEquals(org.example.generator.model.HttpClientKind.JDK, s.settings().httpClient());
        assertEquals(0, s.unknown()); assertEquals(0, s.warmupFailed());
    }

    @Test void checkingAnActiveRunIsRefused() throws Exception {
        delay.set(200);
        LoadRun run = runs.start(new RunSettings(600, 1, null));
        assertEquals(409, post("/api/runs/" + run.id() + "/check", "").statusCode());
    }

    @Test void endlessRunKeepsSendingPastItsDurationUntilStopped() throws Exception {
        var response = post("/api/runs", "{\"requestsPerMinute\":6000,\"durationMinutes\":1,\"endless\":true}");
        assertEquals(201, response.statusCode());
        LoadRun run = runs.find(UUID.fromString(JSON.readTree(response.body()).path("runId").asText()));
        assertTrue(run.snapshot().settings().endless());
        await(() -> run.snapshot().accepted() >= 20);
        run.stop(); await(() -> !run.active());
        var s = run.snapshot();
        assertEquals("STOPPED", s.state());
        assertEquals(0, s.notSent(), "an endless run has no end to fall short of");
        assertEquals(s.planned(), s.accepted() + s.failed() + s.unknown() + s.skipped() + s.blocked());
        var records = Files.readAllLines(run.report().path()).stream().map(JSON::readTree).toList();
        assertTrue(records.stream().filter(n -> n.path("type").asText().equals("plan")).findFirst().orElseThrow()
                .path("planned").isNull());
        // Every id the target received is the run id plus a sequence number in 1..planned.
        for (String id : received.keySet()) {
            long sequence = org.example.generator.model.Submission.sequenceOf(run.id(), UUID.fromString(id));
            assertTrue(sequence >= 1 && sequence <= s.planned(), id);
        }
        var check = JSON.readTree(post("/api/runs/" + run.id() + "/check", "").body());
        assertEquals("CONSISTENT", check.path("verdict").asText());
        assertEquals(s.accepted(), check.path("accepted").path("processed").asLong());
    }

    @Test void submissionIdsAreDerivedFromTheRunAndSequenceNumber() {
        UUID run = UUID.randomUUID();
        var first = org.example.generator.model.Submission.forRun(run, 1);
        assertEquals(first, org.example.generator.model.Submission.forRun(run, 1), "rebuilt identically");
        assertNotEquals(first.clientRequestId(), org.example.generator.model.Submission.forRun(run, 2).clientRequestId());
        assertEquals(42, org.example.generator.model.Submission.sequenceOf(run,
                org.example.generator.model.Submission.forRun(run, 42).clientRequestId()));
        assertEquals(-1, org.example.generator.model.Submission.sequenceOf(run, UUID.randomUUID()));
        assertEquals(-1, org.example.generator.model.Submission.sequenceOf(run,
                org.example.generator.model.Submission.forRun(run, -3).clientRequestId()), "warm-up ids are not run ids");
    }

    @Test void jettyRunCountsConnectionsAndReusesThemInsteadOfOpeningNewOnes() throws Exception {
        var settings = new RunSettings(6000, 1, null, org.example.generator.model.SubmissionMode.KAFKA, false, 0,
                org.example.generator.model.HttpClientKind.JETTY);
        LoadRun run = runs.start(settings);
        await(() -> run.snapshot().accepted() >= 50);
        run.stop(); await(() -> !run.active());
        var s = run.snapshot();
        assertTrue(s.connectionsOpen() >= 1, "connections are counted");
        assertTrue(s.connectionsOpened() < s.accepted() / 5,
                "connections are reused: " + s.connectionsOpened() + " opened for " + s.accepted() + " accepted");
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
