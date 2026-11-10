package org.example.generator.client;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CaseClientTests {

    @Test
    void submissionsSpreadEvenlyOverAllClientsAndEachIdAlwaysUsesTheSameOne() {
        var client = new CaseClient(new JsonMapper(), URI.create("http://127.0.0.1:1/api/cases"), 8);
        assertEquals(8, client.clientCount());

        int[] perClient = new int[8];
        for (int i = 0; i < 80_000; i++) perClient[client.clientIndex(UUID.randomUUID())]++;
        for (int count : perClient) {
            assertTrue(count > 9_000 && count < 11_000, "each client carries about 1/8: " + count);
        }

        UUID id = UUID.randomUUID();
        assertEquals(client.clientIndex(id), client.clientIndex(id), "a retry goes through the same client");
    }

    @Test
    void jettyConnectionsArePreOpenedGraduallyUpToTheConfiguredTotal() throws Exception {
        try (var server = new java.net.ServerSocket(0, 100, java.net.InetAddress.getLoopbackAddress())) {
            var accepted = new java.util.concurrent.atomic.AtomicInteger();
            var sockets = new java.util.concurrent.CopyOnWriteArrayList<java.net.Socket>();
            Thread.ofVirtual().start(() -> {
                try { while (true) { sockets.add(server.accept()); accepted.incrementAndGet(); } }
                catch (java.io.IOException closed) { /* test finished */ }
            });
            // 2 Jetty instances sharing 20 connections: 10 each.
            var client = new CaseClient(new JsonMapper(), URI.create("http://127.0.0.1:" + server.getLocalPort() + "/api/cases"),
                    8, 2, 20, 100, 1, java.time.Duration.ofSeconds(30));
            try {
                long start = System.nanoTime();
                client.preOpenJettyConnections(java.time.Duration.ofMillis(400));
                assertTrue(System.nanoTime() - start >= 300_000_000L, "spread over the given time, not all at once");
                assertEquals(20, accepted.get(), "the whole pool is open before the run");
            } finally {
                client.close();
                for (var socket : sockets) socket.close();
            }
        }
    }

    /** A target that answers 202 after {@code delayMs}, recording the client port of every request. */
    private static com.sun.net.httpserver.HttpServer target(java.util.Map<Integer, java.util.concurrent.atomic.AtomicInteger> portsUsed,
            int delayMs) throws Exception {
        var server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 100);
        server.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
        var json = new JsonMapper();
        server.createContext("/api/cases", exchange -> {
            try { Thread.sleep(delayMs); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
            portsUsed.computeIfAbsent(exchange.getRemoteAddress().getPort(), p -> new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet();
            var input = json.readTree(exchange.getRequestBody().readAllBytes());
            byte[] body = ("{\"clientRequestId\":\"" + input.path("clientRequestId").asText() + "\",\"testRunId\":\""
                    + input.path("testRunId").asText() + "\",\"status\":\"ACCEPTED\"}").getBytes();
            exchange.sendResponseHeaders(202, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        return server;
    }

    @Test
    void jettyReusesOpenConnectionsAndNeverClosesIdleOnes() throws Exception {
        var portsUsed = new java.util.concurrent.ConcurrentHashMap<Integer, java.util.concurrent.atomic.AtomicInteger>();
        var server = target(portsUsed, 0);
        // One Jetty instance with up to 4 connections and the default: no idle timeout.
        var client = new CaseClient(new JsonMapper(), URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/api/cases"),
                8, 1, 4, 100, 1, java.time.Duration.ZERO);
        try {
            UUID run = UUID.randomUUID();
            for (int i = 1; i <= 40; i++) {
                var result = client.send(org.example.generator.model.Submission.forRun(run, i), 0,
                        org.example.generator.model.SubmissionMode.KAFKA, org.example.generator.model.HttpClientKind.JETTY);
                assertEquals("ACCEPTED", result.outcome());
            }
            // One at a time needs one connection: it is reused rather than new ones opened.
            assertEquals(1, portsUsed.size(), "one connection carried every request: " + portsUsed);
            assertEquals(1, client.connectionsOpened());
            // Left idle well past Jetty's own 30 s default would matter only in a slow test; 1.5 s checks the
            // timeout is off rather than short, and the connection is still the one reused afterwards.
            Thread.sleep(1_500);
            assertEquals(0, client.connectionsClosed(), "idle connection kept open");
            client.send(org.example.generator.model.Submission.forRun(run, 41), 0,
                    org.example.generator.model.SubmissionMode.KAFKA, org.example.generator.model.HttpClientKind.JETTY);
            assertEquals(1, portsUsed.size(), "the same connection carried the next request");
            assertEquals(1, client.connectionsOpened());
        } finally {
            client.close();
            server.stop(0);
        }
    }

    @Test
    void requestsTheJettyQueueHasNoRoomForAreSkippedNotUnknown() throws Exception {
        var portsUsed = new java.util.concurrent.ConcurrentHashMap<Integer, java.util.concurrent.atomic.AtomicInteger>();
        var server = target(portsUsed, 500);
        // One connection and room for one waiting request: of 6 sent at once, at most 2 can be taken.
        var client = new CaseClient(new JsonMapper(), URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/api/cases"),
                8, 1, 1, 1, 1, java.time.Duration.ofSeconds(30));
        try {
            UUID run = UUID.randomUUID();
            var outcomes = new java.util.concurrent.ConcurrentLinkedQueue<String>();
            try (var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 1; i <= 6; i++) {
                    long sequence = i;
                    workers.submit(() -> outcomes.add(client.send(org.example.generator.model.Submission.forRun(run, sequence), 0,
                            org.example.generator.model.SubmissionMode.KAFKA, org.example.generator.model.HttpClientKind.JETTY).outcome()));
                }
            }
            assertFalse(outcomes.contains("UNKNOWN"), "a full queue sends nothing, so nothing is unknown: " + outcomes);
            assertTrue(outcomes.stream().filter("SKIPPED"::equals).count() >= 4, outcomes.toString());
            assertTrue(outcomes.contains("ACCEPTED"), outcomes.toString());
        } finally {
            client.close();
            server.stop(0);
        }
    }

    @Test
    void atLeastOneClientIsAlwaysCreated() {
        assertEquals(1, new CaseClient(new JsonMapper(), URI.create("http://127.0.0.1:1/api/cases"), 0).clientCount());
    }
}
