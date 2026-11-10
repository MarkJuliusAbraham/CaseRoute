package org.example.backend.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

/**
 * Pauses and resumes everything the backend does after intake, for experiments that isolate intake:
 * the case writer (cases.submitted to PostgreSQL), the outbox relay (to cases.saved) and the result writer
 * (cases.processed to PostgreSQL). Intake itself keeps running.
 *
 * <p>Nothing is lost while paused: submissions wait in Kafka and outbox rows wait in PostgreSQL, and both are
 * caught up on resume. Paused consumers keep polling, so they stay in their consumer groups. The state lives in
 * memory only; a restart always starts unpaused.
 */
@Service
public class PipelineControl {
    private static final Logger log = LoggerFactory.getLogger(PipelineControl.class);

    private final KafkaListenerEndpointRegistry listeners;
    private volatile boolean paused;

    public PipelineControl(KafkaListenerEndpointRegistry listeners) {
        this.listeners = listeners;
    }

    public synchronized void setPaused(boolean pause) {
        if (pause == paused) return;
        paused = pause;
        for (MessageListenerContainer container : listeners.getListenerContainers()) {
            if (pause) container.pause();
            else container.resume();
        }
        log.info("Downstream pipeline {} (case writer, outbox relay, result writer)", pause ? "PAUSED" : "resumed");
    }

    public boolean isPaused() { return paused; }
}
