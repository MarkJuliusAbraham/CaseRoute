package org.example.backend.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Takes the backend down on purpose, for downtime tests. Either way the process ends, and it is restarted by hand.
 *
 * <ul>
 *   <li><b>Graceful</b>: a normal shutdown ({@code server.shutdown=graceful}). The web server stops accepting new
 *       connections (clients see "connection refused": not sent), lets requests already in progress finish and reply,
 *       then the Kafka consumers and producers stop cleanly. A planned restart this way leaves no request in doubt.</li>
 *   <li><b>Crash</b>: the process stops instantly, as on a power cut or a killed process: no replies to requests in
 *       progress, nothing flushed. Their clients cannot know whether Kafka had the submission; the checker shows how
 *       many were saved anyway.</li>
 * </ul>
 * Both start after a short delay, so the request that asked for it still gets its reply.
 */
@Service
public class ShutdownControl {
    private static final Logger log = LoggerFactory.getLogger(ShutdownControl.class);
    private static final long DELAY_MS = 300;

    public void graceful() {
        later("graceful-shutdown", () -> {
            log.warn("Downtime test: graceful shutdown requested; finishing requests in progress, then exiting");
            System.exit(0); // runs Spring's shutdown hook: graceful web server stop, then Kafka and the database
        });
    }

    public void crash() {
        later("crash", () -> {
            log.warn("Downtime test: crashing now (no shutdown, nothing flushed)");
            Runtime.getRuntime().halt(137);
        });
    }

    private static void later(String name, Runnable action) {
        Thread.ofPlatform().name(name).start(() -> {
            try { Thread.sleep(DELAY_MS); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
            action.run();
        });
    }
}
