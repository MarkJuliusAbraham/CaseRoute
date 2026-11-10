package org.example.generator.client;

import jakarta.annotation.PreDestroy;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.StringRequestContent;
import org.eclipse.jetty.client.transport.HttpClientTransportOverHTTP;
import org.eclipse.jetty.io.ClientConnector;
import org.example.generator.model.HttpClientKind;
import org.example.generator.model.Submission;
import org.example.generator.model.SubmissionMode;
import org.example.generator.model.SubmissionResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Sends submissions to the backend with the HTTP client a run chooses ({@link HttpClientKind}). Both produce the
 * same outcomes; only the transport differs.
 *
 * <p><b>JDK</b>: Java's built-in HttpClient, spread over {@code generator.http-clients} instances. Each does all
 * network work for its connections on a single thread behind shared locks, and opens a new connection for every
 * request waiting at once. Measured: it collapses at about 10,000 requests/s on one Windows machine, while the
 * backend itself served 25,000/s to a native load tool.
 *
 * <p><b>JETTY</b>: Jetty's HttpClient, spread over {@code generator.jetty.clients} instances. Together they open at
 * most {@code generator.jetty.max-connections} connections to the backend; when replies slow down, extra requests
 * wait in Jetty's queues ({@code generator.jetty.max-queued} in total) instead of opening new connections. Each
 * instance has one waiting line guarded by one lock: with a single instance, a large backlog left 192 of its 200
 * worker threads queued on that lock and the client nearly stalled. Several instances spread that contention.
 *
 * <p>Jetty is set up to run indefinitely without anything building up over time:
 * <ul>
 *   <li>A fixed pool: connections are opened once, gradually during the warm-up, up to
 *       {@code generator.jetty.max-connections}, and then never closed for being idle, by either side
 *       ({@code generator.jetty.idle-timeout=0}, backend keep-alive and connection timeouts off). With an idle
 *       timeout, the pool shrank during calm periods, and the next brief slowdown had to open hundreds of
 *       connections at once, which the backend's accept queue partly refused. With no timer there is also no race
 *       where one side closes a connection the other is sending on.</li>
 *   <li>Its pool reuses the connections already open first; open connections that are not needed just wait,
 *       which costs the backend almost nothing.</li>
 *   <li>The waiting line is short ({@code generator.jetty.max-queued}): a request that finds it full is counted as
 *       SKIPPED, never sent. A long line let waits grow until requests timed out, and on HTTP/1.1 every timed-out
 *       request closes its connection: a new one has to be opened and the old port stays reserved for 2 minutes
 *       (TIME_WAIT), which builds up until no ports are left.</li>
 * </ul>
 * Connections opened and closed are counted ({@link #connectionsOpened()}, {@link #connectionsClosed()}); in a
 * healthy run neither changes after the warm-up.
 */
@Component
public class CaseClient {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);

    private final HttpClient[] clients;
    private final ObjectMapper json;
    private final URI target;
    private final int jettyClients;
    private final int jettyMaxConnections;
    private final int jettyMaxQueued;
    private final int jettySelectors;
    private final java.time.Duration jettyIdleTimeout;
    private final java.util.concurrent.atomic.AtomicLong opened = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong closed = new java.util.concurrent.atomic.AtomicLong();
    private volatile java.util.function.Consumer<Boolean> connectionEvents;
    private volatile org.eclipse.jetty.client.HttpClient[] jetty;

    /** What the generator needs from a reply, whichever client fetched it. */
    record Reply(int status, String body, String retryAfter) { }

    @org.springframework.beans.factory.annotation.Autowired
    public CaseClient(ObjectMapper json, @Value("${generator.target-url}") URI target,
            @Value("${generator.http-clients:8}") int httpClients,
            @Value("${generator.jetty.clients:8}") int jettyClients,
            @Value("${generator.jetty.max-connections:2000}") int jettyMaxConnections,
            @Value("${generator.jetty.max-queued:10000}") int jettyMaxQueued,
            @Value("${generator.jetty.selectors:1}") int jettySelectors,
            @Value("${generator.jetty.idle-timeout:0}") java.time.Duration jettyIdleTimeout) {
        this.json = json;
        this.target = target;
        this.jettyClients = Math.max(1, jettyClients);
        this.jettyMaxConnections = jettyMaxConnections;
        this.jettyMaxQueued = jettyMaxQueued;
        this.jettySelectors = jettySelectors;
        this.jettyIdleTimeout = jettyIdleTimeout;
        this.clients = new HttpClient[Math.max(1, httpClients)];
        for (int i = 0; i < clients.length; i++) {
            // HTTP/1.1 explicitly: the default (HTTP/2) adds an upgrade attempt to every new connection,
            // which the HTTP/1.1 backend ignores. Pinning it skips that wasted work.
            clients[i] = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds(5)).build();
        }
    }

    /** Convenience for tests and callers that only use the built-in client. */
    public CaseClient(ObjectMapper json, URI target, int httpClients) {
        this(json, target, httpClients, 8, 2000, 10_000, 1, java.time.Duration.ZERO);
    }

    /** Which JDK client carries a submission: fixed per clientRequestId, so a retry uses the same client. */
    int clientIndex(java.util.UUID clientRequestId) {
        return clientIndex(clientRequestId, clients.length);
    }

    private static int clientIndex(java.util.UUID clientRequestId, int count) {
        return Math.floorMod(clientRequestId.hashCode(), count);
    }
    int clientCount() { return clients.length; }

    public SubmissionResult send(Submission submission, double schedulingDelayMs) {
        return send(submission, schedulingDelayMs, SubmissionMode.KAFKA, HttpClientKind.JDK);
    }

    public SubmissionResult send(Submission submission, double schedulingDelayMs, SubmissionMode mode) {
        return send(submission, schedulingDelayMs, mode, HttpClientKind.JDK);
    }

    public SubmissionResult send(Submission submission, double schedulingDelayMs, SubmissionMode mode,
            HttpClientKind kind) {
        long start = System.nanoTime();
        try {
            Reply reply = post(submission, mode.pathSuffix(), kind);
            int status = reply.status();
            // Direct: 201 for a new case, or 200 when a retry finds the case its earlier attempt saved.
            boolean expected = mode == SubmissionMode.DIRECT ? status == 201 || status == 200 : status == 202;
            if (status == 503 && reply.retryAfter() != null) {
                // Load shedding: the backend turned it away before doing anything. Definitely not stored, so it is
                // safe to resend later; never retried automatically, to keep the run's timing intact.
                return result(submission, "BLOCKED", status, null, start, schedulingDelayMs,
                        "Backend busy (503, Retry-After " + reply.retryAfter() + "); nothing stored, safe to resend");
            }
            if (!expected) {
                return result(submission, status >= 500 ? "UNKNOWN" : "REJECTED", status, null,
                        start, schedulingDelayMs, "HTTP " + status);
            }
            try {
                var body = json.readTree(reply.body());
                boolean sameRequest = submission.clientRequestId().toString().equals(body.path("clientRequestId").asText())
                        && submission.testRunId().toString().equals(body.path("testRunId").asText());
                if (mode == SubmissionMode.DIRECT) {
                    // Direct: the reply is the saved case, so its id and payload can be checked.
                    String id = body.path("id").asText("");
                    java.util.UUID.fromString(id);
                    boolean matches = sameRequest
                            && submission.subject().equals(body.path("subject").asText())
                            && submission.description().equals(body.path("description").asText())
                            && "OPEN".equals(body.path("status").asText());
                    return result(submission, matches ? "ACCEPTED" : "MISMATCH", status, id,
                            start, schedulingDelayMs, matches ? "Creation response matched" : "Unexpected response fields");
                }
                boolean matches = sameRequest && "ACCEPTED".equals(body.path("status").asText());
                return result(submission, matches ? "ACCEPTED" : "MISMATCH", status, null,
                        start, schedulingDelayMs, matches ? "Acceptance response matched" : "Unexpected response fields");
            } catch (Exception ex) {
                return result(submission, "MISMATCH", status, null, start, schedulingDelayMs,
                        "Invalid " + (mode == SubmissionMode.DIRECT ? "creation" : "acceptance") + " response");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return result(submission, "UNKNOWN", null, null, start, schedulingDelayMs,
                    "Interrupted; database outcome unknown");
        } catch (Exception ex) {
            if (queueFull(ex)) {
                // Jetty's waiting line was full, so the request was never sent: nothing can have been stored.
                return result(submission, "SKIPPED", null, null, start, schedulingDelayMs,
                        "Generator's queue full; not sent");
            }
            return result(submission, "UNKNOWN", null, null, start, schedulingDelayMs,
                    describe(ex) + "; database outcome unknown");
        }
    }

    /** Jetty rejects a request it has no room to queue with a RejectedExecutionException, before sending anything. */
    static boolean queueFull(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof java.util.concurrent.RejectedExecutionException) return true;
        }
        return false;
    }

    /** Connections Jetty has opened to the backend since the generator started. */
    public long connectionsOpened() { return opened.get(); }

    /** Connections Jetty has closed since the generator started (by either side). */
    public long connectionsClosed() { return closed.get(); }

    /** Receives every Jetty connection opened (true) or closed (false); null to stop. Used by the running run. */
    public void onConnectionEvents(java.util.function.Consumer<Boolean> listener) { this.connectionEvents = listener; }

    private void connectionEvent(boolean open) {
        (open ? opened : closed).incrementAndGet();
        var listener = connectionEvents;
        if (listener != null) listener.accept(open);
    }

    /**
     * Sends a submission to the backend's warm-up endpoint ({@code POST /api/cases/warmup}), which goes through
     * the real path but creates no case. Same client as the run, so its connections are reused. Returns whether
     * the backend answered 204.
     */
    public boolean warmUp(Submission submission) {
        return warmUp(submission, SubmissionMode.KAFKA, HttpClientKind.JDK);
    }

    public boolean warmUp(Submission submission, SubmissionMode mode) {
        return warmUp(submission, mode, HttpClientKind.JDK);
    }

    /** In a connection test, the warm-up also goes to the no-op endpoint (202), so it touches nothing either. */
    public boolean warmUp(Submission submission, SubmissionMode mode, HttpClientKind kind) {
        boolean noop = mode == SubmissionMode.NOOP;
        try {
            return post(submission, noop ? "/noop" : "/warmup", kind).status() == (noop ? 202 : 204);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception ex) {
            return false;
        }
    }

    /** POSTs the submission as JSON to {@code target + path} and waits for the reply, with the chosen client. */
    private Reply post(Submission submission, String path, HttpClientKind kind) throws Exception {
        URI uri = URI.create(target + path);
        String body = json.writeValueAsString(submission);
        if (kind == HttpClientKind.JETTY) {
            ContentResponse response = jetty()[clientIndex(submission.clientRequestId(), jettyClients)].POST(uri)
                    .headers(headers -> headers.put("Accept", "application/json"))
                    .body(new StringRequestContent("application/json", body))
                    .timeout(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .send();
            return new Reply(response.getStatus(), response.getContentAsString(), response.getHeaders().get("Retry-After"));
        }
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = clients[clientIndex(submission.clientRequestId())]
                .send(request, HttpResponse.BodyHandlers.ofString());
        return new Reply(response.statusCode(), response.body(), response.headers().firstValue("Retry-After").orElse(null));
    }

    /**
     * The Jetty instances, started on first use so runs on the built-in client never start Jetty's threads. The
     * connection and queue limits are totals, divided evenly among the instances.
     */
    private org.eclipse.jetty.client.HttpClient[] jetty() throws Exception {
        org.eclipse.jetty.client.HttpClient[] started = jetty;
        if (started != null) return started;
        synchronized (this) {
            if (jetty == null) {
                var created = new org.eclipse.jetty.client.HttpClient[jettyClients];
                for (int i = 0; i < created.length; i++) {
                    ClientConnector connector = new ClientConnector();
                    connector.setSelectors(jettySelectors);
                    // 0: idle connections are never closed (Jetty's own default is 30 s, on the connector too).
                    connector.setIdleTimeout(jettyIdleTimeout);
                    int perInstance = Math.max(1, jettyMaxConnections / created.length);
                    // Jetty's default pool: reuses open connections first. (A round-robin pool spent 24.8% of the
                    // generator's CPU choosing a connection in a 1M/min profile.)
                    var transport = new HttpClientTransportOverHTTP(connector);
                    var client = new org.eclipse.jetty.client.HttpClient(transport);
                    client.setConnectTimeout(5_000);
                    client.setIdleTimeout(jettyIdleTimeout.toMillis());
                    client.addBean(new org.eclipse.jetty.io.Connection.Listener() {
                        @Override public void onOpened(org.eclipse.jetty.io.Connection connection) { connectionEvent(true); }
                        @Override public void onClosed(org.eclipse.jetty.io.Connection connection) { connectionEvent(false); }
                    });
                    client.setMaxConnectionsPerDestination(perInstance);
                    client.setMaxRequestsQueuedPerDestination(Math.max(1, jettyMaxQueued / created.length));
                    client.setFollowRedirects(false);
                    client.start();
                    created[i] = client;
                }
                jetty = created;
            }
            return jetty;
        }
    }

    /**
     * Opens Jetty's connections to the backend gradually over {@code over}, up to each instance's share of
     * {@code generator.jetty.max-connections}, in 20 even steps. Run at the end of the warm-up, so the measured run
     * starts with spare connections open: opening hundreds at once at the switch overflowed the backend's accept
     * queue ("Connection refused"). They then stay open (no idle timeout), so later slowdowns never need a burst of
     * new connections either. Failures are ignored; requests open any missing connections themselves.
     */
    public void preOpenJettyConnections(Duration over) {
        final int steps = 20;
        try {
            var instances = jetty();
            int perInstance = Math.max(1, jettyMaxConnections / instances.length);
            var pools = new org.eclipse.jetty.client.ConnectionPool[instances.length];
            for (int i = 0; i < instances.length; i++) {
                pools[i] = instances[i].resolveDestination(instances[i].newRequest(target)).getConnectionPool();
            }
            long pause = Math.max(0, over.toMillis() / steps);
            for (int step = 1; step <= steps && !Thread.currentThread().isInterrupted(); step++) {
                int wanted = perInstance * step / steps;
                var created = new java.util.concurrent.CompletableFuture<?>[pools.length];
                for (int i = 0; i < pools.length; i++) created[i] = pools[i].preCreateConnections(wanted);
                try {
                    java.util.concurrent.CompletableFuture.allOf(created).get(5, TimeUnit.SECONDS);
                } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException ignored) {
                    // Some connections could not be opened in time; the run opens them on demand instead.
                }
                if (step < steps) Thread.sleep(pause);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
            // Best effort: without pre-opened connections the run still works, it just opens them as it goes.
        }
    }

    @PreDestroy
    public void close() throws Exception {
        if (jetty != null) for (var client : jetty) client.stop();
    }

    /**
     * The exception and its causes with their messages, e.g. "ConnectException <- ClosedChannelException" or
     * "ConnectException: Connection refused". The class alone cannot tell a refused connection from a timeout
     * or from running out of local ports.
     */
    public static String describe(Throwable ex) {
        StringBuilder text = new StringBuilder();
        java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (Throwable t = ex; t != null && seen.add(t) && seen.size() <= 4; t = t.getCause()) {
            if (!text.isEmpty()) text.append(" <- ");
            text.append(t.getClass().getSimpleName());
            if (t.getMessage() != null && !t.getMessage().isBlank()) {
                String message = t.getMessage().strip();
                text.append(": ").append(message.length() > 120 ? message.substring(0, 120) + "..." : message);
            }
        }
        return text.toString();
    }

    private SubmissionResult result(Submission submission, String outcome, Integer status, String caseId,
            long start, double delay, String detail) {
        return new SubmissionResult(submission.clientRequestId(), outcome, status, caseId,
                (System.nanoTime() - start) / 1_000_000.0, delay, detail, Instant.now(), 1);
    }
}
